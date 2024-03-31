use crate::kernel::requests::{spawn_group_channel_receiver, HandledRequestResult};
use crate::kernel::{CitadelWorkspaceService, GroupConnection};
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    GroupCreateFailure, GroupCreateSuccess, InternalServiceRequest, InternalServiceResponse,
};
use citadel_sdk::prefabs::ClientServerRemote;
use citadel_sdk::prelude::{ProtocolRemoteTargetExt, VirtualTargetType};
use uuid::Uuid;

pub async fn handle<T: IOInterface>(
    this: &CitadelWorkspaceService<T>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::GroupCreate {
        cid,
        request_id,
        initial_users_to_invite,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote();

    let client_to_server_remote = ClientServerRemote::new(
        VirtualTargetType::LocalGroupServer {
            implicated_cid: cid,
        },
        remote.clone(),
        Default::default(),
    );
    let response = match client_to_server_remote
        .create_group(initial_users_to_invite)
        .await
    {
        Ok(group_channel) => {
            // Store the group connection in map
            let key = group_channel.key();
            let group_cid = group_channel.cid();
            let (tx, rx) = group_channel.split();
            match this.server_connection_map.lock().await.get_mut(&cid) {
                Some(conn) => {
                    conn.add_group_channel(
                        key,
                        GroupConnection {
                            key,
                            cid: group_cid,
                            tx,
                        },
                    );

                    let uuid = conn.associated_tcp_connection;
                    spawn_group_channel_receiver(
                        key,
                        cid,
                        uuid,
                        rx,
                        this.tcp_connection_map.clone(),
                    );

                    InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
                        cid,
                        group_key: key,
                        request_id: Some(request_id),
                    })
                }

                None => InternalServiceResponse::GroupCreateFailure(GroupCreateFailure {
                    cid,
                    message: format!("Server connection for {cid} not found"),
                    request_id: Some(request_id),
                }),
            }
        }

        Err(err) => InternalServiceResponse::GroupCreateFailure(GroupCreateFailure {
            cid,
            message: err.into_string(),
            request_id: Some(request_id),
        }),
    };

    Some(HandledRequestResult { response, uuid })
}
