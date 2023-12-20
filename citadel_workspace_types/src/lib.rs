use bytes::BytesMut;
pub use citadel_sdk::prelude::{
    ConnectMode, ObjectTransferStatus, SecBuffer, SecurityLevel, SessionSecuritySettings, UdpMode,
    UserIdentifier,
};
use citadel_sdk::prelude::{MemberState, MessageGroupKey, TransferType, VirtualObjectMetadata};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectSuccess {
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionFailure {
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterSuccess {
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterFailure {
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceConnectionAccepted;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageSent {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageSendError {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageReceived {
    pub message: BytesMut,
    pub cid: u64,
    pub peer_cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Disconnected {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DisconnectFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SendFileRequestSent {
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SendFileFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownloadFileSuccess {
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownloadFileFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteVirtualFileSuccess {
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteVirtualFileFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerConnectSuccess {
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerConnectFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerDisconnectSuccess {
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerDisconnectFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerRegisterSuccess {
    pub cid: u64,
    pub peer_cid: u64,
    pub peer_username: String,
    pub request_id: Option<Uuid>,
    // TODO: add access to MutualPeer
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerRegisterFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupChannelCreateSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupChannelCreateFailure {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupBroadcastHandleFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupCreateSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupCreateFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupLeaveSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupLeaveFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupEndSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupEndFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupEnded {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupLeft {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMessageReceived {
    pub cid: u64,
    pub peer_cid: u64,
    pub message: BytesMut,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMessageSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMessageResponse {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMessageFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupInvitation {
    pub cid: u64,
    pub peer_cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupInviteSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupInviteFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRespondRequestSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRespondRequestFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMembershipResponse {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRequestJoinPending {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub result: Result<(), String>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupDisconnected {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupKickSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupKickFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupListGroupsForSuccess {
    pub cid: u64,
    pub peer_cid: u64,
    pub group_list: Option<Vec<MessageGroupKey>>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupListGroupsForFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupListGroupsResponse {
    pub cid: u64,
    pub group_list: Option<Vec<MessageGroupKey>>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupJoinRequestReceived {
    pub cid: u64,
    pub peer_cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRequestJoinAccepted {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRequestDeclined {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRequestJoinSuccess {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupRequestJoinFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMemberStateChanged {
    pub cid: u64,
    pub group_key: MessageGroupKey,
    pub state: MemberState,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub key: String,
    pub value: Vec<u8>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBSetKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub key: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBSetKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBDeleteKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub key: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBDeleteKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetAllKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub map: HashMap<String, Vec<u8>>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBGetAllKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBClearAllKVSuccess {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListAllPeers {
    pub cid: u64,
    pub online_status: HashMap<u64, bool>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListAllPeersFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListRegisteredPeersFailure {
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListRegisteredPeers {
    pub cid: u64,
    pub peers: HashMap<u64, PeerSessionInformation>,
    pub online_status: HashMap<u64, bool>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalDBClearAllKVFailure {
    pub cid: u64,
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetSessions {
    pub sessions: Vec<SessionInformation>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferRequest {
    pub cid: u64,
    pub peer_cid: u64,
    pub metadata: VirtualObjectMetadata, // TODO: metadata: VirtualObjectMetadata
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferStatus {
    pub cid: u64,
    pub object_id: u64,
    pub success: bool,
    pub response: bool,
    pub message: Option<String>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileTransferTick {
    pub cid: u64,
    pub peer_cid: u64,
    pub status: ObjectTransferStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServiceResponse {
    ConnectSuccess(ConnectSuccess),
    ConnectionFailure(ConnectionFailure),
    RegisterSuccess(RegisterSuccess),
    RegisterFailure(RegisterFailure),
    ServiceConnectionAccepted(ServiceConnectionAccepted),
    MessageSent(MessageSent),
    MessageSendError(MessageSendError),
    MessageReceived(MessageReceived),
    Disconnected(Disconnected),
    DisconnectFailure(DisconnectFailure),
    SendFileRequestSent(SendFileRequestSent),
    SendFileFailure(SendFileFailure),
    FileTransferRequest(FileTransferRequest),
    FileTransferStatus(FileTransferStatus),
    FileTransferTick(FileTransferTick),
    DownloadFileSuccess(DownloadFileSuccess),
    DownloadFileFailure(DownloadFileFailure),
    DeleteVirtualFileSuccess(DeleteVirtualFileSuccess),
    DeleteVirtualFileFailure(DeleteVirtualFileFailure),
    PeerConnectSuccess(PeerConnectSuccess),
    PeerConnectFailure(PeerConnectFailure),
    PeerDisconnectSuccess(PeerDisconnectSuccess),
    PeerDisconnectFailure(PeerDisconnectFailure),
    PeerRegisterSuccess(PeerRegisterSuccess),
    PeerRegisterFailure(PeerRegisterFailure),
    GroupChannelCreateSuccess(GroupChannelCreateSuccess),
    GroupChannelCreateFailure(GroupChannelCreateFailure),
    GroupBroadcastHandleFailure(GroupBroadcastHandleFailure),
    GroupCreateSuccess(GroupCreateSuccess),
    GroupCreateFailure(GroupCreateFailure),
    GroupLeaveSuccess(GroupLeaveSuccess),
    GroupLeaveFailure(GroupLeaveFailure),
    GroupLeft(GroupLeft),
    GroupEndSuccess(GroupEndSuccess),
    GroupEndFailure(GroupEndFailure),
    GroupEnded(GroupEnded),
    GroupMessageReceived(GroupMessageReceived),
    GroupMessageResponse(GroupMessageResponse),
    GroupMessageSuccess(GroupMessageSuccess),
    GroupMessageFailure(GroupMessageFailure),
    GroupInvitation(GroupInvitation),
    GroupInviteSuccess(GroupInviteSuccess),
    GroupInviteFailure(GroupInviteFailure),
    GroupRespondRequestSuccess(GroupRespondRequestSuccess),
    GroupRespondRequestFailure(GroupRespondRequestFailure),
    GroupMembershipResponse(GroupMembershipResponse),
    GroupRequestJoinPending(GroupRequestJoinPending),
    GroupDisconnected(GroupDisconnected),
    GroupKickSuccess(GroupKickSuccess),
    GroupKickFailure(GroupKickFailure),
    GroupListGroupsForSuccess(GroupListGroupsForSuccess),
    GroupListGroupsForFailure(GroupListGroupsForFailure),
    GroupListGroupsResponse(GroupListGroupsResponse),
    GroupJoinRequestReceived(GroupJoinRequestReceived),
    GroupRequestJoinAccepted(GroupRequestJoinAccepted),
    GroupRequestDeclined(GroupRequestDeclined),
    GroupRequestJoinSuccess(GroupRequestJoinSuccess),
    GroupMemberStateChanged(GroupMemberStateChanged),
    GroupRequestJoinFailure(GroupRequestJoinFailure),
    LocalDBGetKVSuccess(LocalDBGetKVSuccess),
    LocalDBGetKVFailure(LocalDBGetKVFailure),
    LocalDBSetKVSuccess(LocalDBSetKVSuccess),
    LocalDBSetKVFailure(LocalDBSetKVFailure),
    LocalDBDeleteKVSuccess(LocalDBDeleteKVSuccess),
    LocalDBDeleteKVFailure(LocalDBDeleteKVFailure),
    LocalDBGetAllKVSuccess(LocalDBGetAllKVSuccess),
    LocalDBGetAllKVFailure(LocalDBGetAllKVFailure),
    LocalDBClearAllKVSuccess(LocalDBClearAllKVSuccess),
    LocalDBClearAllKVFailure(LocalDBClearAllKVFailure),
    GetSessions(GetSessions),
    GetAccountInformation(Accounts),
    ListAllPeers(ListAllPeers),
    ListAllPeersFailure(ListAllPeersFailure),
    ListRegisteredPeers(ListRegisteredPeers),
    ListRegisteredPeersFailure(ListRegisteredPeersFailure),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalServiceRequest {
    Connect {
        // A user-provided unique ID that will be returned in the response
        request_id: Uuid,
        username: String,
        password: SecBuffer,
        connect_mode: ConnectMode,
        udp_mode: UdpMode,
        keep_alive_timeout: Option<Duration>,
        session_security_settings: SessionSecuritySettings,
    },
    Register {
        request_id: Uuid,
        server_addr: SocketAddr,
        full_name: String,
        username: String,
        proposed_password: SecBuffer,
        connect_after_register: bool,
        session_security_settings: SessionSecuritySettings,
    },
    Message {
        request_id: Uuid,
        message: Vec<u8>,
        cid: u64,
        // if None, send to server, otherwise, send to p2p
        peer_cid: Option<u64>,
        security_level: SecurityLevel,
    },
    Disconnect {
        request_id: Uuid,
        cid: u64,
    },
    SendFile {
        request_id: Uuid,
        source: PathBuf,
        cid: u64,
        peer_cid: Option<u64>,
        chunk_size: Option<usize>,
        transfer_type: TransferType,
    },
    RespondFileTransfer {
        cid: u64,
        peer_cid: u64,
        object_id: u64,
        accept: bool,
        download_location: Option<PathBuf>,
        request_id: Uuid,
    },
    DownloadFile {
        virtual_directory: PathBuf,
        security_level: Option<SecurityLevel>,
        delete_on_pull: bool,
        cid: u64,
        peer_cid: Option<u64>,
        request_id: Uuid,
    },
    DeleteVirtualFile {
        virtual_directory: PathBuf,
        cid: u64,
        peer_cid: Option<u64>,
        request_id: Uuid,
    },
    ListAllPeers {
        request_id: Uuid,
        cid: u64,
    },
    ListRegisteredPeers {
        request_id: Uuid,
        cid: u64,
    },
    PeerConnect {
        request_id: Uuid,
        cid: u64,
        peer_cid: u64,
        udp_mode: UdpMode,
        session_security_settings: SessionSecuritySettings,
    },
    PeerDisconnect {
        request_id: Uuid,
        cid: u64,
        peer_cid: u64,
    },
    PeerRegister {
        request_id: Uuid,
        cid: u64,
        peer_cid: u64,
        session_security_settings: SessionSecuritySettings,
        connect_after_register: bool,
    },
    LocalDBGetKV {
        request_id: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
        key: String,
    },
    LocalDBSetKV {
        request_id: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
        key: String,
        value: Vec<u8>,
    },
    LocalDBDeleteKV {
        request_id: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
        key: String,
    },
    LocalDBGetAllKV {
        request_id: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
    },
    LocalDBClearAllKV {
        request_id: Uuid,
        cid: u64,
        peer_cid: Option<u64>,
    },
    GetSessions {
        request_id: Uuid,
    },
    GetAccountInformation {
        request_id: Uuid,
        // If specified, the command will reply with information for a specific account. Otherwise
        // the command will reply with information for all accounts
        cid: Option<u64>,
    },
    GroupCreate {
        cid: u64,
        request_id: Uuid,
        initial_users_to_invite: Option<Vec<UserIdentifier>>,
    },
    GroupLeave {
        cid: u64,
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupEnd {
        cid: u64,
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupMessage {
        cid: u64,
        message: BytesMut,
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupInvite {
        cid: u64,
        peer_cid: u64,
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupRespondRequest {
        cid: u64,
        peer_cid: u64,
        group_key: MessageGroupKey,
        response: bool,
        request_id: Uuid,
        invitation: bool,
    },
    GroupKick {
        cid: u64,
        peer_cid: u64,
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupListGroupsFor {
        cid: u64,
        peer_cid: u64,
        request_id: Uuid,
    },
    GroupRequestJoin {
        cid: u64,
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SessionInformation {
    pub cid: u64,
    pub peer_connections: HashMap<u64, PeerSessionInformation>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Accounts {
    pub accounts: HashMap<u64, AccountInformation>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AccountInformation {
    pub username: String,
    pub full_name: String,
    pub peers: HashMap<u64, PeerSessionInformation>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PeerSessionInformation {
    pub cid: u64,
    pub peer_cid: u64,
    pub peer_username: String,
}
