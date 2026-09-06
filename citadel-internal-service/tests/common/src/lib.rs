#![allow(dead_code)]
use citadel_internal_service::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::connector::{InternalServiceConnector, WrappedSink};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    FileTransferTickNotification, InternalServiceRequest, InternalServiceResponse,
    PeerConnectNotification, PeerConnectSuccess, PeerRegisterNotification, PeerRegisterSuccess,
};
use citadel_sdk::logging::info;
use citadel_sdk::prefabs::server::client_connect_listener::ClientConnectListenerKernel;
use citadel_sdk::prefabs::server::empty::EmptyKernel;
use citadel_sdk::prelude::*;
use core::panic;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub fn setup_log() {
    citadel_sdk::logging::setup_log();
    std::panic::set_hook(Box::new(|info| {
        // The MESSAGE, not just the location.
        //
        // `{:?}` on PanicHookInfo renders the payload as `Any { .. }`, so every
        // failing test in this suite reported a file and a line number and
        // nothing about what went wrong -- a CI leg could go red for weeks
        // saying only that it had. The payload is a &str for `panic!("...")`
        // and a String for a formatted one; neither is reachable through Debug.
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());

        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown location>".to_string());

        citadel_sdk::logging::error!(target: "citadel", "Panic at {location}: {message}");
        // Also to stderr: the tracing subscriber is filtered by RUST_LOG, and a
        // panic is not something a log level should be able to hide.
        eprintln!("Panic at {location}: {message}");
        std::process::exit(1);
    }));
}

/// Ports below the OS ephemeral range, partitioned so concurrent test processes
/// cannot be handed the same one.
///
/// The obvious implementation — bind `:0`, read the port, drop the listener — is
/// what this used to be, and it is a race. Between the drop and the caller's real
/// bind the port is free, so the kernel may hand that exact port to another test
/// process binding `:0`. That is the `AddrInUse` that took out
/// `group_chat::test_internal_service_group_create` in CI at 0.012s, before a
/// single line of group logic ran. nextest gives each test its own process, so no
/// process-local registry can see the conflict; it has to be avoided by
/// construction.
///
/// Two properties do that:
///
/// 1. Ports come from a fixed range *below* Linux's ephemeral range
///    (`net.ipv4.ip_local_port_range`, 32768-60999 by default). A `bind(:0)`
///    anywhere on the machine can never be handed one of ours, which removes the
///    race entirely for the case above.
/// 2. Concurrent test processes are separated by partitioning that range by pid
///    into disjoint blocks. nextest spawns processes with near-consecutive pids,
///    so multiplying by the block size is what keeps neighbours from overlapping:
///    `pid` and `pid + 1` land [`PORTS_PER_PROCESS`] apart, not one apart.
///
/// The bindability probe below is a backstop, not the mechanism — it catches a
/// port a previous run still holds or one sitting in TIME_WAIT. It still closes
/// the socket before returning, so it carries the same race it always did; what
/// makes that harmless now is (1) and (2).
const PORT_BASE: u16 = 20_000;
const PORTS_PER_PROCESS: u16 = 16;
const PORT_SLOTS: u16 = 750; // 750 * 16 = 12_000 ports, ending at 32_000.

/// Helper function to get a free port for testing
pub fn get_free_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    const SPAN: u16 = PORT_SLOTS * PORTS_PER_PROCESS;

    let slot = (std::process::id() % PORT_SLOTS as u32) as u16;
    let block_start = slot.wrapping_mul(PORTS_PER_PROCESS) % SPAN;

    // Walk the whole span rather than just this process's block: a test needing
    // more than PORTS_PER_PROCESS ports must still get them, it just borrows from
    // a neighbouring block and relies on the probe to skip anything in use.
    for _ in 0..SPAN {
        let step = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let port = PORT_BASE + (block_start.wrapping_add(step) % SPAN);
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free port in {PORT_BASE}..{}", PORT_BASE + SPAN);
}

pub struct RegisterAndConnectItems<
    T: Into<String>,
    R: Into<String>,
    S: Into<SecBuffer>,
    Q: Into<PreSharedKey>,
> {
    pub internal_service_addr: SocketAddr,
    pub server_addr: SocketAddr,
    pub full_name: T,
    pub username: R,
    pub password: S,
    pub pre_shared_key: Option<Q>,
}

pub type InternalServicesFutures =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + 'static>>;

pub type PeerReturnHandle = (
    UnboundedSender<InternalServiceRequest>,
    UnboundedReceiver<InternalServiceResponse>,
    u64,
);

pub trait PeerServiceHandles {
    fn take_next_service_handle(&mut self) -> PeerReturnHandle;
}

impl PeerServiceHandles for Vec<PeerReturnHandle> {
    fn take_next_service_handle(&mut self) -> PeerReturnHandle {
        self.remove(0)
    }
}

pub fn generic_error<T: ToString>(msg: T) -> Box<dyn Error> {
    Box::new(std::io::Error::other(msg.to_string()))
}

pub async fn register_and_connect_to_server<
    T: Into<String>,
    R: Into<String>,
    S: Into<SecBuffer>,
    Q: Into<PreSharedKey>,
>(
    services_to_create: Vec<RegisterAndConnectItems<T, R, S, Q>>,
) -> Result<
    Vec<(
        UnboundedSender<InternalServiceRequest>,
        UnboundedReceiver<InternalServiceResponse>,
        u64,
    )>,
    Box<dyn Error>,
> {
    info!(target = "citadel", "Registering and Connecting To Server");
    let mut return_results: Vec<(
        UnboundedSender<InternalServiceRequest>,
        UnboundedReceiver<InternalServiceResponse>,
        u64,
    )> = Vec::new();

    for item in services_to_create {
        let (mut sink, mut stream) = InternalServiceConnector::connect(item.internal_service_addr)
            .await?
            .split();

        let username = item.username.into();
        let full_name = item.full_name.into();
        let password = item.password.into();
        let server_password: Option<PreSharedKey> = item.pre_shared_key.map(|x| x.into());
        let session_security_settings = SessionSecuritySettingsBuilder::default().build()?;

        info!(target = "citadel", "Sending Register Request");
        let register_command = InternalServiceRequest::Register {
            request_id: Uuid::new_v4(),
            server_addr: item.server_addr.to_string(),
            full_name,
            username: username.clone(),
            proposed_password: password.clone(),
            session_security_settings,
            connect_after_register: false,
            server_password: server_password.clone(),
        };
        send(&mut sink, register_command).await?;

        let response_packet = stream.next().await.unwrap();

        if let InternalServiceResponse::RegisterSuccess(
            citadel_internal_service_types::RegisterSuccess { .. },
        ) = response_packet
        {
            info!(
                target = "citadel",
                "RegisterSuccess Received, Now Connecting"
            );
            // now, connect to the server
            let command = InternalServiceRequest::Connect {
                username,
                password,
                connect_mode: Default::default(),
                udp_mode: Default::default(),
                keep_alive_timeout: None,
                session_security_settings,
                request_id: Uuid::new_v4(),
                server_password: server_password.clone(),
            };

            send(&mut sink, command).await?;

            let response_packet = stream.next().await.unwrap();
            if let InternalServiceResponse::ConnectSuccess(
                citadel_internal_service_types::ConnectSuccess { cid, request_id: _ },
            ) = response_packet
            {
                info!(
                    target = "citadel",
                    "ConnectSuccess Received, Creating Service Channels"
                );
                let (to_service, from_service) = tokio::sync::mpsc::unbounded_channel();
                let service_to_test = async move {
                    // take messages from the service and send them to from_service
                    while let Some(msg) = stream.next().await {
                        info!(target = "citadel", "Service to test {msg:?}");
                        to_service.send(msg).unwrap();
                    }
                };

                let (to_service_sender, mut from_test) = tokio::sync::mpsc::unbounded_channel();
                let test_to_service = async move {
                    while let Some(msg) = from_test.recv().await {
                        info!(target = "citadel", "Test to service {:?}", msg);
                        send(&mut sink, msg).await.unwrap();
                    }
                };

                let mut internal_services: Vec<InternalServicesFutures> = Vec::new();
                internal_services.push(Box::pin(async move {
                    test_to_service.await;
                    Ok(())
                }));
                internal_services.push(Box::pin(async move {
                    service_to_test.await;
                    Ok(())
                }));
                spawn_services(internal_services);
                return_results.push((to_service_sender, from_service, cid));
            } else {
                panic!("Connection to server was not a success");
            }
        } else {
            panic!("Registration to server was not a success");
        }
    }
    Ok(return_results)
}

pub async fn register_and_connect_to_server_then_peers<R: Ratchet>(
    int_svc_addrs: Vec<SocketAddr>,
    server_session_password: Option<PreSharedKey>,
    peer_session_password: Option<PreSharedKey>,
) -> Result<Vec<PeerReturnHandle>, Box<dyn Error>> {
    register_and_connect_to_server_then_peers_with_udp::<R>(
        int_svc_addrs,
        server_session_password,
        peer_session_password,
        Default::default(),
    )
    .await
}

/// `register_and_connect_to_server_then_peers`, with the peer connection's
/// `UdpMode` stated explicitly.
///
/// The default is `Disabled`, so without this no Rust test can reach the media
/// path at all — see `connect_p2p_with_udp`.
pub async fn register_and_connect_to_server_then_peers_with_udp<R: Ratchet>(
    int_svc_addrs: Vec<SocketAddr>,
    server_session_password: Option<PreSharedKey>,
    peer_session_password: Option<PreSharedKey>,
    udp_mode: citadel_sdk::prelude::UdpMode,
) -> Result<Vec<PeerReturnHandle>, Box<dyn Error>> {
    // TCP client (GUI, CLI) -> internal service -> empty kernel server(s)
    let (server, server_bind_address) = if server_session_password.is_some() {
        server_info_skip_cert_verification_with_password::<R>(
            server_session_password.clone().unwrap(),
        )
    } else {
        server_info_skip_cert_verification::<R>()
    };
    tokio::task::spawn(server);
    let mut internal_services: Vec<InternalServicesFutures> = Vec::new();

    // Spawn Internal Services with given addresses
    for int_svc_addr_iter in int_svc_addrs.clone() {
        let bind_address_internal_service = int_svc_addr_iter;
        info!(target: "citadel", "Internal Service Spawning");
        let internal_service_kernel =
            CitadelWorkspaceService::<_, R>::new_tcp(bind_address_internal_service).await?;
        let internal_service = NodeBuilder::default()
            .with_node_type(NodeType::Peer)
            .with_insecure_skip_cert_verification()
            .build(internal_service_kernel)?;

        // Add NodeFuture for Internal Service to Vector to be spawned
        internal_services.push(Box::pin(async move {
            match internal_service.await {
                Err(err) => Err(Box::from(err)),
                _ => Ok(()),
            }
        }));
    }
    spawn_services(internal_services);

    // Give time for both the Server and Internal Service to run
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Set Info for Vector of Peers
    let mut to_spawn: Vec<RegisterAndConnectItems<String, String, Vec<u8>, PreSharedKey>> =
        Vec::new();
    for (peer_number, int_svc_addr_iter) in int_svc_addrs.clone().iter().enumerate() {
        let bind_address_internal_service = *int_svc_addr_iter;
        to_spawn.push(RegisterAndConnectItems {
            internal_service_addr: bind_address_internal_service,
            server_addr: server_bind_address,
            full_name: format!("Peer {peer_number}"),
            username: format!("peer.{peer_number}"),
            password: format!("secret_{peer_number}").into_bytes().to_owned(),
            pre_shared_key: server_session_password.clone(),
        });
    }

    // Registers and Connects all peers to Server
    let mut returned_service_info = register_and_connect_to_server(to_spawn).await?;

    info!(
        target = "citadel",
        "Starting Registration and Connection between peers"
    );
    // Registers and Connects all peers to Each Other Peer
    for service_index in 0..returned_service_info.len() {
        let (item, neighbor_items) = {
            let (_, second) = returned_service_info.split_at_mut(service_index);
            let (element, remainder) = second.split_at_mut(1);
            (&mut element[0], remainder)
        };

        let (ref mut to_service_a, ref mut from_service_a, cid_a) = item;
        for neighbor in neighbor_items {
            let (ref mut to_service_b, ref mut from_service_b, cid_b) = neighbor;
            let session_security_settings = SessionSecuritySettingsBuilder::default().build()?;
            register_p2p(
                to_service_a,
                from_service_a,
                *cid_a,
                to_service_b,
                from_service_b,
                *cid_b,
                session_security_settings,
                peer_session_password.clone(),
            )
            .await?;

            connect_p2p_with_udp(
                to_service_a,
                from_service_a,
                *cid_a,
                to_service_b,
                from_service_b,
                *cid_b,
                session_security_settings,
                peer_session_password.clone(),
                udp_mode,
            )
            .await?;
        }
    }
    Ok(returned_service_info)
}

#[allow(clippy::too_many_arguments)]
pub async fn register_p2p(
    to_service_a: &mut UnboundedSender<InternalServiceRequest>,
    from_service_a: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_a: u64,
    to_service_b: &mut UnboundedSender<InternalServiceRequest>,
    from_service_b: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_b: u64,
    session_security_settings: SessionSecuritySettings,
    session_password: Option<PreSharedKey>,
) -> Result<(), Box<dyn Error>> {
    // Service A Requests to Register with Service B
    to_service_a
        .send(InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: cid_a,
            peer_cid: cid_b,
            session_security_settings,
            connect_after_register: false,
            peer_session_password: session_password.clone(),
        })
        .unwrap();

    // Service B receives Register Request from Service A
    let inbound_response = from_service_b.recv().await.unwrap();
    match inbound_response {
        InternalServiceResponse::PeerRegisterNotification(PeerRegisterNotification {
            cid,
            peer_cid,
            peer_username: _,
            request_id: _,
        }) => {
            assert_eq!(cid, cid_b);
            assert_eq!(peer_cid, cid_a);
        }
        _ => {
            panic!(
                "Peer B didn't get the PeerRegisterNotification, instead got {inbound_response:?}"
            );
        }
    }

    // Service B Sends Register Request to Accept
    to_service_b
        .send(InternalServiceRequest::PeerRegister {
            request_id: Uuid::new_v4(),
            cid: cid_b,
            peer_cid: cid_a,
            session_security_settings,
            connect_after_register: false,
            peer_session_password: session_password,
        })
        .unwrap();

    // Receive Register Success Responses
    let resp = from_service_a.recv().await.unwrap();
    let InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess { cid, peer_cid, .. }) =
        resp
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_a);
    assert_eq!(peer_cid, cid_b);

    let resp = from_service_b.recv().await.unwrap();
    let InternalServiceResponse::PeerRegisterSuccess(PeerRegisterSuccess { cid, peer_cid, .. }) =
        resp
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_b);
    assert_eq!(peer_cid, cid_a);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn connect_p2p(
    to_service_a: &mut UnboundedSender<InternalServiceRequest>,
    from_service_a: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_a: u64,
    to_service_b: &mut UnboundedSender<InternalServiceRequest>,
    from_service_b: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_b: u64,
    session_security_settings: SessionSecuritySettings,
    session_password: Option<PreSharedKey>,
) -> Result<(), Box<dyn Error>> {
    connect_p2p_with_udp(
        to_service_a,
        from_service_a,
        cid_a,
        to_service_b,
        from_service_b,
        cid_b,
        session_security_settings,
        session_password,
        Default::default(),
    )
    .await
}

/// `connect_p2p`, with the peer connection's `UdpMode` stated explicitly.
///
/// Every path in this harness passed `udp_mode: Default::default()`, and that
/// default is `Disabled` — so no Rust test had ever brought a peer connection up
/// with UDP, and the whole media path was exercised only by the browser suite.
/// That is why a bug as blunt as `UdpState::Pending` holding one receiver and
/// dropping the other (fixed in dfb50a2) could only be caught in CI.
#[allow(clippy::too_many_arguments)]
pub async fn connect_p2p_with_udp(
    to_service_a: &mut UnboundedSender<InternalServiceRequest>,
    from_service_a: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_a: u64,
    to_service_b: &mut UnboundedSender<InternalServiceRequest>,
    from_service_b: &mut UnboundedReceiver<InternalServiceResponse>,
    cid_b: u64,
    session_security_settings: SessionSecuritySettings,
    session_password: Option<PreSharedKey>,
    udp_mode: citadel_sdk::prelude::UdpMode,
) -> Result<(), Box<dyn Error>> {
    // Service A Requests To Connect
    to_service_a
        .send(InternalServiceRequest::PeerConnect {
            request_id: Uuid::new_v4(),
            cid: cid_a,
            peer_cid: cid_b,
            udp_mode,
            session_security_settings,
            peer_session_password: session_password.clone(),
        })
        .unwrap();

    // Service B Receives Connect Request from Service A
    let inbound_response = from_service_b.recv().await.unwrap();
    match inbound_response {
        InternalServiceResponse::PeerConnectNotification(PeerConnectNotification {
            cid,
            peer_cid,
            session_security_settings: _,
            udp_mode: _,
            request_id: _,
        }) => {
            assert_eq!(cid, cid_b);
            assert_eq!(peer_cid, cid_a);
        }
        _ => {
            panic!("Peer B didn't get the PeerConnectNotification");
        }
    }

    // Service B Sends Connect Request to Accept
    to_service_b
        .send(InternalServiceRequest::PeerConnect {
            request_id: Uuid::new_v4(),
            cid: cid_b,
            peer_cid: cid_a,
            udp_mode,
            session_security_settings,
            peer_session_password: session_password,
        })
        .unwrap();

    // Receive Connect Success Responses
    let signal = from_service_a.recv().await.unwrap();
    let InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid, peer_cid, .. }) =
        signal
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_a);
    assert_eq!(peer_cid, cid_b);
    let signal = from_service_b.recv().await.unwrap();
    let InternalServiceResponse::PeerConnectSuccess(PeerConnectSuccess { cid, peer_cid, .. }) =
        signal
    else {
        panic!("Invalid signal")
    };
    assert_eq!(cid, cid_b);
    assert_eq!(peer_cid, cid_a);

    Ok(())
}

pub fn spawn_services(futures_to_spawn: Vec<InternalServicesFutures>) {
    let services_to_spawn = async move {
        let (returned_future, _, _) = futures::future::select_all(futures_to_spawn).await;
        match returned_future {
            Ok(_) => {
                info!(target: "citadel","Vital Internal Service Ended");
            }
            Err(err) => {
                citadel_sdk::logging::error!(target: "citadel", "Internal service error: {err:?}");
            }
        }
    };
    tokio::task::spawn(services_to_spawn);
}

pub async fn send<T: IOInterface>(
    sink: &mut WrappedSink<T>,
    command: InternalServiceRequest,
) -> Result<(), Box<dyn Error>> {
    sink.send(command).await?;
    Ok(())
}

pub fn server_test_node_skip_cert_verification<'a, K: NetKernel<R> + 'a, R: Ratchet>(
    kernel: K,
    opts: impl FnOnce(&mut NodeBuilder<R>),
) -> (NodeFuture<'a, K>, SocketAddr) {
    let mut builder = NodeBuilder::<R>::default();
    let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind_addr = tcp_listener.local_addr().unwrap();
    let builder = builder
        .with_node_type(NodeType::Server(bind_addr))
        .with_insecure_skip_cert_verification()
        .with_underlying_protocol(ServerMode::OrderedReliable(
            NativeOrderedReliableConfig::from_std_listener(tcp_listener).unwrap(),
        ));

    (opts)(builder);

    (builder.build(kernel).unwrap(), bind_addr)
}

pub fn server_test_node_skip_cert_verification_with_password<
    'a,
    K: NetKernel<R> + 'a,
    R: Ratchet,
>(
    kernel: K,
    server_password: PreSharedKey,
    opts: impl FnOnce(&mut NodeBuilder<R>),
) -> (NodeFuture<'a, K>, SocketAddr) {
    let mut builder = NodeBuilder::<R>::default();
    let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind_addr = tcp_listener.local_addr().unwrap();
    let builder = builder
        .with_node_type(NodeType::Server(bind_addr))
        .with_server_password(server_password)
        .with_insecure_skip_cert_verification()
        .with_underlying_protocol(ServerMode::OrderedReliable(
            NativeOrderedReliableConfig::from_std_listener(tcp_listener).unwrap(),
        ));

    (opts)(builder);

    (builder.build(kernel).unwrap(), bind_addr)
}

pub fn server_info_skip_cert_verification<'a, R: Ratchet>(
) -> (NodeFuture<'a, EmptyKernel<R>>, SocketAddr) {
    server_test_node_skip_cert_verification(EmptyKernel::<R>::default(), |_| {})
}

pub fn server_info_skip_cert_verification_with_password<'a, R: Ratchet>(
    server_password: PreSharedKey,
) -> (NodeFuture<'a, EmptyKernel<R>>, SocketAddr) {
    server_test_node_skip_cert_verification_with_password(
        EmptyKernel::<R>::default(),
        server_password,
        |_| {},
    )
}

pub fn server_info_reactive_skip_cert_verification<'a, F, Fut, R: Ratchet>(
    f: F,
    opts: impl FnOnce(&mut NodeBuilder<R>),
) -> (NodeFuture<'a, Box<dyn NetKernel<R> + 'a>>, SocketAddr)
where
    F: Fn(CitadelClientServerConnection<R>) -> Fut + Send + Sync + 'a,
    Fut: Future<Output = Result<(), NetworkError>> + Send + Sync + 'a,
{
    server_test_node_skip_cert_verification(
        Box::new(ClientConnectListenerKernel::new(f)) as Box<dyn NetKernel<R>>,
        opts,
    )
}

pub struct ReceiverFileTransferKernel<R: Ratchet>(pub Option<NodeRemote<R>>, pub Arc<AtomicBool>);

#[async_trait]
impl<R: Ratchet> NetKernel<R> for ReceiverFileTransferKernel<R> {
    fn load_remote(&mut self, node_remote: NodeRemote<R>) -> Result<(), NetworkError> {
        self.0 = Some(node_remote);
        Ok(())
    }

    async fn on_start(&self) -> Result<(), NetworkError> {
        Ok(())
    }

    async fn on_node_event_received(&self, message: NodeResult<R>) -> Result<(), NetworkError> {
        citadel_sdk::logging::trace!(target: "citadel", "SERVER received {:?}", message);
        if let NodeResult::ObjectTransferHandle(object_transfer_handle) = message {
            let mut handle = object_transfer_handle.handle;
            let mut path = None;
            let mut is_revfs = false;
            // Automatically accept the transfer
            handle.accept().unwrap();

            use futures::StreamExt;
            while let Some(status) = handle.next().await {
                match status {
                    ObjectTransferStatus::ReceptionComplete => {
                        citadel_sdk::logging::trace!(target: "citadel", "Server has finished receiving the file!");
                        let mut cmp_path = PathBuf::from("..");
                        cmp_path.push("resources");
                        cmp_path.push("test");
                        cmp_path.set_extension("txt");
                        let cmp_data = tokio::fs::read(cmp_path).await.unwrap();
                        let streamed_data = tokio::fs::read(path.clone().unwrap()).await.unwrap();
                        if is_revfs {
                            assert_ne!(
                                cmp_data.as_slice(),
                                streamed_data.as_slice(),
                                "Original data and streamed data match - Should not match"
                            );
                        } else {
                            assert_eq!(
                                cmp_data.as_slice(),
                                streamed_data.as_slice(),
                                "Original data and streamed data does not match"
                            );
                        }
                    }
                    ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                        is_revfs = matches!(
                            vfm.transfer_type,
                            TransferType::RemoteEncryptedVirtualFilesystem { .. }
                        );
                        path = Some(file_path);
                        assert_eq!(vfm.name, "test.txt")
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    async fn on_stop(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }
}

pub fn server_info_file_transfer<'a, R: Ratchet>(
    switch: Arc<AtomicBool>,
) -> (NodeFuture<'a, ReceiverFileTransferKernel<R>>, SocketAddr) {
    let (server, bind_addr) =
        server_test_node_skip_cert_verification(ReceiverFileTransferKernel(None, switch), |_| {});
    (server, bind_addr)
}

pub async fn exhaust_stream_to_file_completion(
    cmp_path: PathBuf,
    svc: &mut UnboundedReceiver<InternalServiceResponse>,
) {
    exhaust_stream_to_file_completion_from(cmp_path, svc, None).await
}

/// As above, for a caller that has ALREADY taken the first tick off the stream.
///
/// A REVFS push is auto-accepted, so the receiver's first notification is
/// `ReceptionBeginning` rather than an offer — and a test that wants to assert
/// that has to consume the tick to look at it. `UnboundedReceiver` has no peek,
/// so the tick is handed back in rather than lost: without it the loop below
/// never sees the beginning, and its `expect("Never received the
/// ReceptionBeginning tick!")` fires on a transfer that began perfectly well.
pub async fn exhaust_stream_to_file_completion_from(
    cmp_path: PathBuf,
    svc: &mut UnboundedReceiver<InternalServiceResponse>,
    already_taken: Option<InternalServiceResponse>,
) {
    // Exhaust the stream for the receiver
    let mut path = None;
    let mut is_revfs = false;
    let cmp_file_name = cmp_path
        .file_name()
        .unwrap()
        .to_os_string()
        .into_string()
        .unwrap();
    let mut already_taken = already_taken;
    loop {
        let tick_response = match already_taken.take() {
            Some(first) => first,
            None => svc.recv().await.unwrap(),
        };
        match tick_response {
            InternalServiceResponse::FileTransferTickNotification(
                FileTransferTickNotification {
                    cid: _,
                    peer_cid: _,
                    status,
                    ..
                },
            ) => match status {
                ObjectTransferStatus::ReceptionBeginning(file_path, vfm) => {
                    path = Some(file_path);
                    is_revfs = matches!(
                        vfm.transfer_type,
                        TransferType::RemoteEncryptedVirtualFilesystem { .. }
                    );
                    info!(target: "citadel", "File Transfer (Receiving) Beginning");
                    assert_eq!(vfm.name, cmp_file_name)
                }
                ObjectTransferStatus::ReceptionTick(..) => {
                    info!(target: "citadel", "File Transfer (Receiving) Tick");
                }
                ObjectTransferStatus::ReceptionComplete => {
                    info!(target: "citadel", "File Transfer (Receiving) Completed");
                    let cmp_data = tokio::fs::read(cmp_path.clone()).await.unwrap();
                    let streamed_data = tokio::fs::read(
                        path.clone()
                            .expect("Never received the ReceptionBeginning tick!"),
                    )
                    .await
                    .unwrap();
                    if is_revfs {
                        // The locally stored contents should NEVER be the same as the plaintext for REVFS
                        assert_ne!(
                            cmp_data.as_slice(),
                            streamed_data.as_slice(),
                            "Original data and streamed data does not match"
                        );
                    } else {
                        assert_eq!(
                            cmp_data.as_slice(),
                            streamed_data.as_slice(),
                            "Original data and streamed data does not match"
                        );
                    }

                    return;
                }
                ObjectTransferStatus::TransferComplete => {
                    info!(target: "citadel", "File Transfer (Sending) Completed");
                    return;
                }
                ObjectTransferStatus::TransferBeginning => {
                    info!(target: "citadel", "File Transfer (Sending) Beginning");
                }
                ObjectTransferStatus::TransferTick(..) => {}
                _ => {
                    panic!("File Send Reception Status Yielded Unexpected Response")
                }
            },
            unexpected_response => {
                citadel_sdk::logging::warn!(target: "citadel", "Unexpected signal {unexpected_response:?}")
            }
        }
    }
}

pub async fn test_kv_for_service(
    to_service: &UnboundedSender<InternalServiceRequest>,
    from_service: &mut UnboundedReceiver<InternalServiceResponse>,
    cid: u64,
    peer_cid: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    // test get_all_kv
    to_service.send(InternalServiceRequest::LocalDBGetAllKV {
        cid,
        peer_cid,
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBGetAllKVSuccess(resp) =
        from_service.recv().await.unwrap()
    {
        assert_eq!(resp.cid, cid);
        assert_eq!(resp.map.len(), 0);
        assert_eq!(peer_cid, resp.peer_cid);
    } else {
        panic!("Didn't get the LocalDBGetAllKVSuccess");
    }

    // test set_kv
    let value = Vec::from("Hello, World!");
    to_service.send(InternalServiceRequest::LocalDBSetKV {
        cid,
        peer_cid,
        key: "tmp".to_string(),
        value: value.clone(),
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBSetKVSuccess(resp) = from_service.recv().await.unwrap() {
        assert_eq!(resp.cid, cid);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(resp.key, "tmp");
    } else {
        panic!("Didn't get the LocalDBSetKVSuccess");
    }

    // test get_kv
    to_service.send(InternalServiceRequest::LocalDBGetKV {
        cid,
        peer_cid,
        key: "tmp".to_string(),
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBGetKVSuccess(resp) = from_service.recv().await.unwrap() {
        assert_eq!(resp.cid, cid);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(resp.key, "tmp");
        assert_eq!(&resp.value, &value);
    } else {
        panic!("Didn't get the LocalDBGetKVSuccess");
    }

    // test get_all_kv
    to_service.send(InternalServiceRequest::LocalDBGetAllKV {
        cid,
        peer_cid,
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBGetAllKVSuccess(resp) =
        from_service.recv().await.unwrap()
    {
        assert_eq!(resp.cid, cid);
        assert_eq!(resp.map.len(), 1);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(
            resp.map,
            HashMap::from([("tmp".to_string(), value.clone())])
        );
    } else {
        panic!("Didn't get the LocalDBGetAllKVSuccess");
    }

    // test delete_kv
    to_service.send(InternalServiceRequest::LocalDBDeleteKV {
        cid,
        peer_cid,
        key: "tmp".to_string(),
        request_id: Uuid::new_v4(),
    })?;

    if let InternalServiceResponse::LocalDBDeleteKVSuccess(resp) =
        from_service.recv().await.unwrap()
    {
        assert_eq!(resp.cid, cid);
        assert_eq!(peer_cid, resp.peer_cid);
        assert_eq!(resp.key, "tmp");
    } else {
        panic!("Didn't get the LocalDBDeleteKVSuccess");
    }

    Ok(())
}

/// What `register_and_connect_to_server` hands back per session.
pub type PeerHandle = (
    UnboundedSender<InternalServiceRequest>,
    UnboundedReceiver<InternalServiceResponse>,
    u64,
);

/// Opens a media session and reports how long the UDP channel took.
///
/// Shared, so the three tests differ only in how the peer connection was
/// established -- the variable under study.
pub async fn open_media_and_measure(
    tx: &UnboundedSender<InternalServiceRequest>,
    rx: &mut UnboundedReceiver<InternalServiceResponse>,
    cid: u64,
    peer_cid: u64,
    label: &str,
) {
    let started = std::time::Instant::now();
    tx.send(InternalServiceRequest::MediaOpen {
        request_id: Uuid::new_v4(),
        cid,
        peer_cid,
    })
    .unwrap();

    // Bounded: this failure presents as silence, and an unbounded recv
    // would hang the suite instead of failing it.
    let response = tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{label}: no answer to MediaOpen within 30s"))
        .expect("channel open");

    match response {
        InternalServiceResponse::MediaSessionOpened(opened) => {
            assert_eq!(
                opened.peer_cid, peer_cid,
                "{label}: opened against the wrong peer"
            );
            println!(
                "MEASURED {label}: {:?} (unreliable={})",
                started.elapsed(),
                opened.unreliable
            );
        }
        // The message names WHICH failure: no channel within the budget,
        // or a connection brought up with UDP disabled.
        InternalServiceResponse::MediaSessionFailed(failed) => panic!(
            "{label}: media open failed after {:?}: {}",
            started.elapsed(),
            failed.message
        ),
        other => panic!("{label}: expected a media session result, got {other:?}"),
    }
}

/// One internal service hosting two registered, server-connected sessions.
///
/// This is the browser's shape: one browser is one WebSocket is one service.
pub async fn two_sessions_on_one_service(
    tag: &str,
) -> Result<(PeerHandle, PeerHandle), Box<dyn Error>> {
    let (server, server_bind_address) = server_info_skip_cert_verification::<StackedRatchet>();
    tokio::task::spawn(server);

    let service_addr: SocketAddr = format!("127.0.0.1:{}", get_free_port()).parse().unwrap();
    let service = CitadelWorkspaceService::<_, StackedRatchet>::new_tcp(service_addr).await?;
    let internal_service = NodeBuilder::default()
        .with_backend(BackendType::InMemory)
        .with_node_type(NodeType::Peer)
        .with_insecure_skip_cert_verification()
        .build(service)?;
    tokio::task::spawn(internal_service);
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let to_spawn = (0..2)
        .map(|i| RegisterAndConnectItems {
            internal_service_addr: service_addr,
            server_addr: server_bind_address,
            full_name: format!("{tag} {i}"),
            username: format!("{tag}.{i}"),
            password: format!("secret_{i}").into_bytes(),
            pre_shared_key: None::<PreSharedKey>,
        })
        .collect();

    let mut info = register_and_connect_to_server(to_spawn).await.unwrap();
    let second = info.remove(1);
    let first = info.remove(0);
    Ok((first, second))
}

#[cfg(test)]
mod port_allocation_tests {
    use super::*;

    /// The property the CI failure was about: a port we hand out must be one the
    /// kernel will never hand to somebody else's `bind(:0)`.
    ///
    /// Linux's default ephemeral range starts at 32768; macOS's at 49152. Staying
    /// strictly below both is what makes the unavoidable close-then-bind window in
    /// `get_free_port` harmless.
    #[test]
    fn a_handed_out_port_is_never_one_the_os_can_auto_assign() {
        const LOWEST_EPHEMERAL_ANY_PLATFORM: u16 = 32_768;
        for _ in 0..32 {
            let port = get_free_port();
            assert!(
                (PORT_BASE..LOWEST_EPHEMERAL_ANY_PLATFORM).contains(&port),
                "port {port} is inside the OS ephemeral range, so a concurrent \
                 bind(:0) in another test process can be handed it too"
            );
        }
    }

    /// nextest spawns test processes with near-consecutive pids. Blocks are indexed
    /// by pid *multiplied* by the block size for exactly that reason: without the
    /// multiply, pid and pid+1 would start one port apart and overlap on their very
    /// first two allocations.
    #[test]
    fn neighbouring_pids_do_not_share_a_block() {
        let block = |pid: u32| (pid % PORT_SLOTS as u32) as u16 * PORTS_PER_PROCESS;
        for pid in 1000..1064u32 {
            let (mine, next) = (block(pid), block(pid + 1));
            assert!(
                mine.abs_diff(next) >= PORTS_PER_PROCESS,
                "pids {pid} and {} start {} ports apart; a test taking two ports \
                 would collide with its neighbour",
                pid + 1,
                mine.abs_diff(next)
            );
        }
    }

    /// A port is only returned if it actually binds, so the same port is never
    /// returned twice within a process while the caller still holds it.
    #[test]
    fn successive_calls_do_not_repeat_a_port() {
        let held: Vec<_> = (0..8)
            .map(|_| {
                let port = get_free_port();
                (
                    port,
                    TcpListener::bind(("127.0.0.1", port)).expect("probe said free"),
                )
            })
            .collect();
        let mut ports: Vec<u16> = held.iter().map(|(p, _)| *p).collect();
        let before = ports.len();
        ports.sort_unstable();
        ports.dedup();
        assert_eq!(
            before,
            ports.len(),
            "get_free_port handed out a duplicate: {ports:?}"
        );
    }
}
