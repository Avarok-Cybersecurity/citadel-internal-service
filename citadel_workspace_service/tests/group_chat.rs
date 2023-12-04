mod common;

#[cfg(test)]
mod tests {
    use crate::common::*;
    use citadel_logging::info;
    use citadel_sdk::prelude::UserIdentifier;
    use citadel_workspace_types::{
        GroupCreateSuccess, GroupInvitation, GroupInviteSuccess, GroupJoinRequestReceived,
        GroupLeaveSuccess, GroupListGroupsForSuccess, GroupRequestJoinAccepted,
        GroupRequestJoinFailure, GroupRequestJoinSuccess, GroupRespondInviteRequestFailure,
        GroupRespondInviteRequestSuccess, InternalServiceRequest, InternalServiceResponse,
    };
    use std::error::Error;
    use std::net::SocketAddr;
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_citadel_workspace_service_group_create() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        // internal service for peer C
        let bind_address_internal_service_c: SocketAddr = "127.0.0.1:55538".parse().unwrap();

        let mut peer_return_handle_vec = register_and_connect_to_server_then_peers(vec![
            bind_address_internal_service_a,
            bind_address_internal_service_b,
            bind_address_internal_service_c,
        ])
        .await?;

        let (peer_one, peer_two) = peer_return_handle_vec
            .as_mut_slice()
            .split_at_mut(1 as usize);
        let (peer_two, peer_three) = peer_two.split_at_mut(1 as usize);
        let (to_service_a, from_service_a, cid_a) = peer_one.get_mut(0 as usize).unwrap();
        let (to_service_b, from_service_b, cid_b) = peer_two.get_mut(0 as usize).unwrap();
        let (to_service_c, from_service_c, cid_c) = peer_three.get_mut(0 as usize).unwrap();

        let mut initial_users_to_invite: Vec<UserIdentifier> = Vec::new();
        initial_users_to_invite.push(UserIdentifier::from(*cid_b));
        initial_users_to_invite.push(UserIdentifier::from(*cid_c));
        let send_group_create_payload = InternalServiceRequest::GroupCreate {
            cid: *cid_a,
            request_id: Uuid::new_v4(),
            initial_users_to_invite: Some(initial_users_to_invite),
        };
        to_service_a.send(send_group_create_payload).unwrap();
        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","Service A: {deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
            cid: _,
            group_key,
            request_id: _,
        }) = &deserialized_service_a_payload_response
        {
            let owner_group_key = *group_key;
            let service_b_group_create_invite = from_service_b.recv().await.unwrap();
            info!(target: "citadel","Service B: {service_b_group_create_invite:?}");
            if let InternalServiceResponse::GroupInvitation(GroupInvitation {
                cid: _,
                peer_cid,
                group_key,
                request_id: _,
            }) = &service_b_group_create_invite
            {
                assert_eq!(peer_cid, cid_a);
                assert_eq!(*group_key, owner_group_key.clone());
                let group_invite_response = InternalServiceRequest::GroupRespondInviteRequest {
                    cid: *cid_b,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: false,
                    request_id: Uuid::new_v4(),
                };
                info!(target: "citadel","Service B Sending Invite Response");
                to_service_b.send(group_invite_response).unwrap();
                let deserialized_service_b_payload_response = from_service_b.recv().await.unwrap();
                info!(target: "citadel","Service B Response Sent");
                if let InternalServiceResponse::GroupRespondInviteRequestSuccess(
                    GroupRespondInviteRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_b_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service B: Successfully Declined Group Invite");
                } else if let InternalServiceResponse::GroupRespondInviteRequestFailure(
                    GroupRespondInviteRequestFailure {
                        cid: _,
                        message,
                        request_id: _,
                    },
                ) = &deserialized_service_b_payload_response
                {
                    panic!("Service B Failed Upon Responding to Group Invite: {message:?}");
                }
                info!(target: "citadel","{deserialized_service_b_payload_response:?}");
            } else {
                panic!("Service B Invitation Not Received");
            }

            let service_c_group_create_invite = from_service_c.recv().await.unwrap();
            info!(target: "citadel","Service C: {service_c_group_create_invite:?}");
            if let InternalServiceResponse::GroupInvitation(GroupInvitation {
                cid: _,
                peer_cid,
                group_key,
                request_id: _,
            }) = &service_c_group_create_invite
            {
                assert_eq!(*group_key, owner_group_key.clone());
                let group_invite_response = InternalServiceRequest::GroupRespondInviteRequest {
                    cid: *cid_c,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                };
                info!(target: "citadel","Service C Sending Invite Response");
                to_service_c.send(group_invite_response).unwrap();
                let deserialized_service_c_payload_response = from_service_c.recv().await.unwrap();
                info!(target: "citadel","Service C Response Sent");
                if let InternalServiceResponse::GroupRespondInviteRequestSuccess(
                    GroupRespondInviteRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_c_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service C: Successfully Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondInviteRequestFailure(
                    GroupRespondInviteRequestFailure {
                        cid: _,
                        message,
                        request_id: _,
                    },
                ) = &deserialized_service_c_payload_response
                {
                    panic!("Service C Failed Upon Responding to Group Invite: {message:?}");
                }
                info!(target: "citadel","{deserialized_service_c_payload_response:?}");
            } else {
                panic!("Service C Invitation Not Received");
            }
        } else {
            panic! {"Group Creation Error: Service A did not receive success response"};
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_group_invite_and_join() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        // internal service for peer C
        let bind_address_internal_service_c: SocketAddr = "127.0.0.1:55538".parse().unwrap();

        let mut peer_return_handle_vec = register_and_connect_to_server_then_peers(vec![
            bind_address_internal_service_a,
            bind_address_internal_service_b,
            bind_address_internal_service_c,
        ])
        .await?;

        let (peer_one, peer_two) = peer_return_handle_vec
            .as_mut_slice()
            .split_at_mut(1 as usize);
        let (peer_two, peer_three) = peer_two.split_at_mut(1 as usize);
        let (to_service_a, from_service_a, cid_a) = peer_one.get_mut(0 as usize).unwrap();
        let (to_service_b, from_service_b, cid_b) = peer_two.get_mut(0 as usize).unwrap();
        let (to_service_c, from_service_c, cid_c) = peer_three.get_mut(0 as usize).unwrap();

        let send_group_payload = InternalServiceRequest::GroupCreate {
            cid: *cid_a,
            request_id: Uuid::new_v4(),
            initial_users_to_invite: None,
        };
        to_service_a.send(send_group_payload).unwrap();
        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","Service A: {deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
            cid: _,
            group_key,
            request_id: _,
        }) = &deserialized_service_a_payload_response
        {
            let send_group_payload = InternalServiceRequest::GroupInvite {
                cid: *cid_a,
                peer_cid: *cid_b,
                group_key: *group_key,
                request_id: Uuid::new_v4(),
            };
            to_service_a.send(send_group_payload).unwrap();
            let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
            if let InternalServiceResponse::GroupInviteSuccess(GroupInviteSuccess {
                cid: _,
                group_key: _,
                request_id: _,
            }) = &deserialized_service_a_payload_response
            {
                let service_b_group_inbound = from_service_b.recv().await.unwrap();
                let owner_group_key = *group_key;
                info!(target: "citadel","Service B: {service_b_group_inbound:?}");
                if let InternalServiceResponse::GroupInvitation(GroupInvitation {
                    cid: _,
                    peer_cid,
                    group_key,
                    request_id: _,
                }) = &service_b_group_inbound
                {
                    let service_b_group_outbound =
                        InternalServiceRequest::GroupRespondInviteRequest {
                            cid: *cid_b,
                            peer_cid: *peer_cid,
                            group_key: *group_key,
                            response: true,
                            request_id: Uuid::new_v4(),
                        };
                    info!(target: "citadel","Service B Sending Invite Response");
                    to_service_b.send(service_b_group_outbound).unwrap();
                    let deserialized_service_b_payload_response =
                        from_service_b.recv().await.unwrap();
                    info!(target: "citadel","Service B Response Sent");
                    if let InternalServiceResponse::GroupRespondInviteRequestSuccess(
                        GroupRespondInviteRequestSuccess {
                            cid: _,
                            group_key,
                            request_id: _,
                        },
                    ) = &deserialized_service_b_payload_response
                    {
                        assert_eq!(*group_key, owner_group_key.clone());
                        info!(target: "citadel","Service B: Successfully Accepted Group Invite");
                    } else if let InternalServiceResponse::GroupRespondInviteRequestFailure(
                        GroupRespondInviteRequestFailure {
                            cid: _,
                            message,
                            request_id: _,
                        },
                    ) = &deserialized_service_b_payload_response
                    {
                        panic!("Service B Failed Upon Responding to Group Invite: {message:?}");
                    }
                    info!(target: "citadel","{deserialized_service_b_payload_response:?}");

                    // let service_b_group_outbound =
                    //     InternalServiceRequest::GroupMessage {
                    //         cid: *cid_b,
                    //         message: Vec::from("THIS IS A TEST FROM SERVICE B"),
                    //         group_key: *group_key,
                    //         request_id: Uuid::new_v4(),
                    //     };
                    // info!(target: "citadel","Service B Sending Test Message");
                    // to_service_b.send(service_b_group_outbound).unwrap();
                    // let service_a_group_inbound = from_service_a.recv().await.unwrap();
                    // info!(target: "citadel"," RECEIVED MESSAGE FROM GROUP: {service_a_group_inbound:?}");
                }
            }

            tokio::time::sleep(Duration::from_millis(2000)).await;

            let service_c_group_outbound = InternalServiceRequest::GroupListGroupsFor {
                cid: *cid_c,
                peer_cid: *cid_a,
                request_id: Uuid::new_v4(),
            };
            to_service_c.send(service_c_group_outbound).unwrap();
            info!(target: "citadel","Service C Requesting Groups for Service A");
            let service_c_group_inbound = from_service_c.recv().await.unwrap();
            if let InternalServiceResponse::GroupListGroupsForSuccess(GroupListGroupsForSuccess {
                cid: _,
                peer_cid: _,
                group_list,
                request_id: _,
            }) = &service_c_group_inbound
            {
                info!(target: "citadel", "Service C Got Success Response with groups: {group_list:?}");
                if let Some(&group_to_join) = group_list.clone().unwrap().first() {
                    info!(target: "citadel","Service C Found Group {group_to_join:?} for Service A: {cid_a:?}");
                    let service_c_group_outbound = InternalServiceRequest::GroupRequestJoin {
                        cid: *cid_c,
                        group_key: group_to_join,
                        request_id: Uuid::new_v4(),
                    };
                    to_service_c.send(service_c_group_outbound).unwrap();
                    info!(target: "citadel","Service C Sending Group Join Request");
                    let service_c_group_inbound = from_service_c.recv().await.unwrap();
                    if let InternalServiceResponse::GroupRequestJoinSuccess(
                        GroupRequestJoinSuccess {
                            cid: _,
                            group_key,
                            request_id: _,
                        },
                    ) = &service_c_group_inbound
                    {
                        assert_eq!(group_to_join, *group_key);
                        info!(target: "citadel","Service C Requested To Join Group");
                    } else if let InternalServiceResponse::GroupRequestJoinFailure(
                        GroupRequestJoinFailure {
                            cid: _,
                            message,
                            request_id: _,
                        },
                    ) = &service_c_group_inbound
                    {
                        panic!("Service C Group Request Join Failure: {message:?}");
                    }

                    let service_a_group_inbound = from_service_a.recv().await.unwrap();
                    if let InternalServiceResponse::GroupJoinRequestReceived(
                        GroupJoinRequestReceived {
                            cid: _,
                            peer_cid: _,
                            group_key,
                            request_id: _,
                        },
                    ) = &service_a_group_inbound
                    {
                        let service_a_group_outbound =
                            InternalServiceRequest::GroupRespondInviteRequest {
                                cid: *cid_a,
                                peer_cid: *cid_c,
                                group_key: *group_key,
                                response: true,
                                request_id: Uuid::new_v4(),
                            };
                        to_service_a.send(service_a_group_outbound).unwrap();
                        info!(target: "citadel","Service A Accepted Join Request");
                        let service_c_group_inbound = from_service_c.recv().await.unwrap();
                        if let InternalServiceResponse::GroupRequestJoinAccepted(
                            GroupRequestJoinAccepted {
                                cid: _,
                                group_key: _,
                                request_id: _,
                            },
                        ) = &service_c_group_inbound
                        {
                            info!(target: "citadel","Service C Joined Group");
                        }
                    } else {
                        panic!("Service A Did Not Receive Join Request");
                    }
                }
            } else {
                panic!("Service C List Groups Failure");
            }
        } else {
            panic! {"Group Creation Error: Service A did not receive success response"};
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_group_leave_and_end() -> Result<(), Box<dyn Error>> {
        citadel_logging::setup_log();
        // internal service for peer A
        let bind_address_internal_service_a: SocketAddr = "127.0.0.1:55536".parse().unwrap();
        // internal service for peer B
        let bind_address_internal_service_b: SocketAddr = "127.0.0.1:55537".parse().unwrap();
        // internal service for peer C
        let bind_address_internal_service_c: SocketAddr = "127.0.0.1:55538".parse().unwrap();

        let mut peer_return_handle_vec = register_and_connect_to_server_then_peers(vec![
            bind_address_internal_service_a,
            bind_address_internal_service_b,
            bind_address_internal_service_c,
        ])
        .await?;

        let (peer_one, peer_two) = peer_return_handle_vec
            .as_mut_slice()
            .split_at_mut(1 as usize);
        let (peer_two, peer_three) = peer_two.split_at_mut(1 as usize);
        let (to_service_a, from_service_a, cid_a) = peer_one.get_mut(0 as usize).unwrap();
        let (to_service_b, from_service_b, cid_b) = peer_two.get_mut(0 as usize).unwrap();
        let (to_service_c, from_service_c, cid_c) = peer_three.get_mut(0 as usize).unwrap();

        let mut initial_users_to_invite: Vec<UserIdentifier> = Vec::new();
        initial_users_to_invite.push(UserIdentifier::from(*cid_b));
        initial_users_to_invite.push(UserIdentifier::from(*cid_c));
        let send_group_create_payload = InternalServiceRequest::GroupCreate {
            cid: *cid_a,
            request_id: Uuid::new_v4(),
            initial_users_to_invite: Some(initial_users_to_invite),
        };
        to_service_a.send(send_group_create_payload).unwrap();
        let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
        info!(target: "citadel","Service A: {deserialized_service_a_payload_response:?}");

        if let InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
            cid: _,
            group_key,
            request_id: _,
        }) = &deserialized_service_a_payload_response
        {
            let owner_group_key = *group_key;

            // Service B Accepts Invitation
            let service_b_group_create_invite = from_service_b.recv().await.unwrap();
            info!(target: "citadel","Service B: {service_b_group_create_invite:?}");
            if let InternalServiceResponse::GroupInvitation(GroupInvitation {
                cid: _,
                peer_cid,
                group_key,
                request_id: _,
            }) = &service_b_group_create_invite
            {
                assert_eq!(peer_cid, cid_a);
                assert_eq!(*group_key, owner_group_key.clone());
                let group_invite_response = InternalServiceRequest::GroupRespondInviteRequest {
                    cid: *cid_b,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                };
                info!(target: "citadel","Service B Sending Invite Response");
                to_service_b.send(group_invite_response).unwrap();
                let deserialized_service_b_payload_response = from_service_b.recv().await.unwrap();
                info!(target: "citadel","Service B Response Sent");
                if let InternalServiceResponse::GroupRespondInviteRequestSuccess(
                    GroupRespondInviteRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_b_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service B: Successfully Declined Group Invite");
                } else if let InternalServiceResponse::GroupRespondInviteRequestFailure(
                    GroupRespondInviteRequestFailure {
                        cid: _,
                        message,
                        request_id: _,
                    },
                ) = &deserialized_service_b_payload_response
                {
                    panic!("Service B Failed Upon Responding to Group Invite: {message:?}");
                }
                info!(target: "citadel","{deserialized_service_b_payload_response:?}");
            } else {
                panic!("Service B Invitation Not Received");
            }

            // Service C Accepts Group Invitation
            let service_c_group_create_invite = from_service_c.recv().await.unwrap();
            info!(target: "citadel","Service C: {service_c_group_create_invite:?}");
            if let InternalServiceResponse::GroupInvitation(GroupInvitation {
                cid: _,
                peer_cid,
                group_key,
                request_id: _,
            }) = &service_c_group_create_invite
            {
                assert_eq!(*group_key, owner_group_key.clone());
                let group_invite_response = InternalServiceRequest::GroupRespondInviteRequest {
                    cid: *cid_c,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                };
                info!(target: "citadel","Service C Sending Invite Response");
                to_service_c.send(group_invite_response).unwrap();
                let deserialized_service_c_payload_response = from_service_c.recv().await.unwrap();
                info!(target: "citadel","Service C Response Sent");
                if let InternalServiceResponse::GroupRespondInviteRequestSuccess(
                    GroupRespondInviteRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_c_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service C: Successfully Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondInviteRequestFailure(
                    GroupRespondInviteRequestFailure {
                        cid: _,
                        message,
                        request_id: _,
                    },
                ) = &deserialized_service_c_payload_response
                {
                    panic!("Service C Failed Upon Responding to Group Invite: {message:?}");
                }
                info!(target: "citadel","{deserialized_service_c_payload_response:?}");
            } else {
                panic!("Service C Invitation Not Received");
            }

            // Service C Leaves Group
            let group_invite_response = InternalServiceRequest::GroupLeave {
                cid: *cid_c,
                group_key: owner_group_key,
                request_id: Uuid::new_v4(),
            };
            info!(target: "citadel","Service C Leaving Group");
            to_service_c.send(group_invite_response).unwrap();
            let deserialized_service_c_payload_response = from_service_c.recv().await.unwrap();
            if let InternalServiceResponse::GroupLeaveSuccess(GroupLeaveSuccess {
                cid: _,
                group_key,
                request_id: _,
            }) = &deserialized_service_c_payload_response
            {
                info!(target: "citadel","Service C Successfully Left Group");
            } else {
                panic!("Service C panicked while attempting to leave group");
            }

            // Service A Ends Group
            let send_group_create_payload = InternalServiceRequest::GroupEnd {
                cid: *cid_a,
                group_key: owner_group_key,
                request_id: Uuid::new_v4(),
            };
            info!(target: "citadel","Service A Ending Group");
            to_service_a.send(send_group_create_payload).unwrap();
            let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();

            if let InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
                cid: _,
                group_key,
                request_id: _,
            }) = &deserialized_service_a_payload_response
            {
                info!(target: "citadel","Service A Successfully Ended Group");
            } else {
                panic!("Service A Panicked while ending group")
            }
        } else {
            panic! {"Group Creation Error: Service A did not receive success response"};
        }

        Ok(())
    }
}
