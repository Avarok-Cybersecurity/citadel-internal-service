use crate::kernel::{
    requests, send_response_to_tcp_client, CitadelWorkspaceService, GroupConnection,
};
use citadel_internal_service_types::{GroupChannelCreateSuccess, InternalServiceResponse};
use citadel_sdk::prelude::{GroupChannelCreated, NetworkError};

pub async fn handle(
    this: &CitadelWorkspaceService,
    group_channel_created: GroupChannelCreated,
) -> Result<(), NetworkError> {
    let channel = group_channel_created.channel;
    let cid = channel.cid();
    let key = channel.key();
    let (tx, rx) = channel.split();

    let mut server_connection_map = this.server_connection_map.lock().await;
    if let Some(connection) = server_connection_map.get_mut(&cid) {
        connection.add_group_channel(key, GroupConnection { key, tx, cid });

        let uuid = connection.associated_tcp_connection;
        requests::spawn_group_channel_receiver(key, cid, uuid, rx, this.tcp_connection_map.clone());

        let associated_tcp_connection = connection.associated_tcp_connection;
        drop(server_connection_map);
        send_response_to_tcp_client(
            &this.tcp_connection_map,
            InternalServiceResponse::GroupChannelCreateSuccess(GroupChannelCreateSuccess {
                cid,
                group_key: key,
                request_id: None,
            }),
            associated_tcp_connection,
        )
        .await?;

        Ok(())
    } else {
        Err(NetworkError::Generic(format!(
            "No connection found for cid in connection map: {cid}"
        )))
    }
}
