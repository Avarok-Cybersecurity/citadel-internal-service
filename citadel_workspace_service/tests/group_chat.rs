mod common;

#[cfg(test)]
mod tests {
    use crate::common::*;
    use citadel_logging::info;
    use citadel_sdk::prelude::{MemberState, SyncIO, UserIdentifier};
    use citadel_workspace_types::{
        GroupCreateSuccess, GroupDisconnected, GroupEndSuccess, GroupEnded, GroupInvitation,
        GroupInviteSuccess, GroupJoinRequestReceived, GroupKickFailure, GroupKickSuccess,
        GroupLeaveSuccess, GroupLeft, GroupListGroupsForSuccess, GroupMemberStateChanged,
        GroupMessageReceived, GroupMessageResponse, GroupMessageSuccess, GroupRequestDeclined,
        GroupRequestJoinFailure, GroupRequestJoinSuccess, GroupRespondRequestFailure,
        GroupRespondRequestSuccess, InternalServiceRequest, InternalServiceResponse,
    };
    use std::error::Error;
    use std::net::SocketAddr;
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

            // Service B Declines Group Invitation
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
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_b,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: false,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                info!(target: "citadel","Service B Sending Invite Response");
                to_service_b.send(group_invite_response).unwrap();
                let deserialized_service_b_payload_response = from_service_b.recv().await.unwrap();
                info!(target: "citadel","Service B Response Sent");
                if let InternalServiceResponse::GroupRespondRequestSuccess(
                    GroupRespondRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_b_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service B: Successfully Declined Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(
                    GroupRespondRequestFailure {
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
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_c,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                info!(target: "citadel","Service C Sending Invite Response");
                to_service_c.send(group_invite_response).unwrap();
                let deserialized_service_c_payload_response = from_service_c.recv().await.unwrap();
                info!(target: "citadel","Service C Response Sent");
                if let InternalServiceResponse::GroupRespondRequestSuccess(
                    GroupRespondRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_c_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service C: Successfully Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(
                    GroupRespondRequestFailure {
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
    async fn test_citadel_workspace_service_group_invite() -> Result<(), Box<dyn Error>> {
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
            // Invite Service B and Accept it
            let send_group_payload = InternalServiceRequest::GroupInvite {
                cid: *cid_a,
                peer_cid: *cid_b,
                group_key: *group_key,
                request_id: Uuid::new_v4(),
            };
            to_service_a.send(send_group_payload).unwrap();
            let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
            if let InternalServiceResponse::GroupInviteSuccess(GroupInviteSuccess { .. }) =
                &deserialized_service_a_payload_response
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
                    let service_b_group_outbound = InternalServiceRequest::GroupRespondRequest {
                        cid: *cid_b,
                        peer_cid: *peer_cid,
                        group_key: *group_key,
                        response: true,
                        request_id: Uuid::new_v4(),
                        invitation: true,
                    };
                    info!(target: "citadel","Service B Sending Invite Response");
                    to_service_b.send(service_b_group_outbound).unwrap();
                    let deserialized_service_b_payload_response =
                        from_service_b.recv().await.unwrap();
                    info!(target: "citadel","Service B Response Sent");
                    if let InternalServiceResponse::GroupRespondRequestSuccess(
                        GroupRespondRequestSuccess {
                            cid: _,
                            group_key,
                            request_id: _,
                        },
                    ) = &deserialized_service_b_payload_response
                    {
                        assert_eq!(*group_key, owner_group_key.clone());
                        info!(target: "citadel","Service B: Successfully Accepted Group Invite");
                    } else if let InternalServiceResponse::GroupRespondRequestFailure(
                        GroupRespondRequestFailure {
                            cid: _,
                            message,
                            request_id: _,
                        },
                    ) = &deserialized_service_b_payload_response
                    {
                        panic!("Service B Failed Upon Responding to Group Invite: {message:?}");
                    }
                    info!(target: "citadel","{deserialized_service_b_payload_response:?}");
                }
            } else {
                panic!("Service A Panicked When looking for Group Invite Response for Service B");
            }

            // Invite Service C and Decline it
            let send_group_payload = InternalServiceRequest::GroupInvite {
                cid: *cid_a,
                peer_cid: *cid_c,
                group_key: *group_key,
                request_id: Uuid::new_v4(),
            };
            to_service_a.send(send_group_payload).unwrap();
            let _ = from_service_a.recv().await.unwrap(); // Receive unnecessary MemberStateChanged
            let deserialized_service_a_payload_response = from_service_a.recv().await.unwrap();
            if let InternalServiceResponse::GroupInviteSuccess(GroupInviteSuccess { .. }) =
                &deserialized_service_a_payload_response
            {
                let service_c_group_inbound = from_service_c.recv().await.unwrap();
                let owner_group_key = *group_key;
                info!(target: "citadel","Service C: {service_c_group_inbound:?}");
                if let InternalServiceResponse::GroupInvitation(GroupInvitation {
                    cid: _,
                    peer_cid,
                    group_key,
                    request_id: _,
                }) = &service_c_group_inbound
                {
                    let service_c_group_outbound = InternalServiceRequest::GroupRespondRequest {
                        cid: *cid_c,
                        peer_cid: *peer_cid,
                        group_key: *group_key,
                        response: false,
                        request_id: Uuid::new_v4(),
                        invitation: true,
                    };
                    info!(target: "citadel","Service C Sending Invite Response");
                    to_service_c.send(service_c_group_outbound).unwrap();
                    let deserialized_service_c_payload_response =
                        from_service_c.recv().await.unwrap();
                    info!(target: "citadel","Service C Response Sent");
                    if let InternalServiceResponse::GroupRespondRequestSuccess(
                        GroupRespondRequestSuccess {
                            cid: _,
                            group_key,
                            request_id: _,
                        },
                    ) = &deserialized_service_c_payload_response
                    {
                        assert_eq!(*group_key, owner_group_key.clone());
                        info!(target: "citadel","Service C: Successfully Accepted Group Invite");
                    } else if let InternalServiceResponse::GroupRespondRequestFailure(
                        GroupRespondRequestFailure {
                            cid: _,
                            message,
                            request_id: _,
                        },
                    ) = &deserialized_service_c_payload_response
                    {
                        panic!("Service C Failed Upon Responding to Group Invite: {message:?}");
                    }
                    info!(target: "citadel","{deserialized_service_c_payload_response:?}");
                }
            } else {
                panic!("Service A Panicked When looking for Group Invite Response for Service C");
            }
        } else {
            panic! {"Group Creation Error: Service A did not receive success response"};
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_group_request_join() -> Result<(), Box<dyn Error>> {
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

        if let InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess { .. }) =
            &deserialized_service_a_payload_response
        {
            // Service B Requests to Join and Service A Accepts
            let service_b_group_outbound = InternalServiceRequest::GroupListGroupsFor {
                cid: *cid_b,
                peer_cid: *cid_a,
                request_id: Uuid::new_v4(),
            };
            to_service_b.send(service_b_group_outbound).unwrap();
            info!(target: "citadel","Service B Requesting Groups for Service A");
            let service_b_group_inbound = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::GroupListGroupsForSuccess(GroupListGroupsForSuccess {
                cid: _,
                peer_cid: _,
                group_list,
                request_id: _,
            }) = &service_b_group_inbound
            {
                info!(target: "citadel", "Service B Got Success Response with groups: {group_list:?}");
                if let Some(&group_to_join) = group_list.clone().unwrap().first() {
                    info!(target: "citadel","Service B Found Group {group_to_join:?} for Service A");
                    let service_b_group_outbound = InternalServiceRequest::GroupRequestJoin {
                        cid: *cid_b,
                        group_key: group_to_join,
                        request_id: Uuid::new_v4(),
                    };
                    to_service_b.send(service_b_group_outbound).unwrap();
                    info!(target: "citadel","Service B Sending Group Join Request");
                    let service_b_group_inbound = from_service_b.recv().await.unwrap();
                    if let InternalServiceResponse::GroupRequestJoinSuccess(
                        GroupRequestJoinSuccess {
                            cid: _,
                            group_key,
                            request_id: _,
                        },
                    ) = &service_b_group_inbound
                    {
                        assert_eq!(group_to_join, *group_key);
                        info!(target: "citadel","Service B Requested To Join Group");
                    } else if let InternalServiceResponse::GroupRequestJoinFailure(
                        GroupRequestJoinFailure {
                            cid: _,
                            message,
                            request_id: _,
                        },
                    ) = &service_b_group_inbound
                    {
                        panic!("Service B Group Request Join Failure: {message:?}");
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
                            InternalServiceRequest::GroupRespondRequest {
                                cid: *cid_a,
                                peer_cid: *cid_b,
                                group_key: *group_key,
                                response: true,
                                request_id: Uuid::new_v4(),
                                invitation: false,
                            };
                        to_service_a.send(service_a_group_outbound).unwrap();
                        let service_a_group_inbound = from_service_a.recv().await.unwrap();
                        info!(target: "citadel","Service A Received Response {service_a_group_inbound:?}");

                        info!(target: "citadel","Service A Accepted Join Request");

                        let service_b_group_inbound = from_service_b.recv().await.unwrap();
                        if let InternalServiceResponse::GroupMemberStateChanged(
                            GroupMemberStateChanged {
                                cid: _,
                                group_key: joined_group,
                                state,
                                request_id: _,
                            },
                        ) = &service_b_group_inbound
                        {
                            match state {
                                MemberState::EnteredGroup { cids } => {
                                    info!(target: "citadel","Service B {cids:?} Joined Group {joined_group:?}");
                                }
                                _ => {
                                    panic!("Service B Group Join Fatal Error")
                                }
                            }
                        } else {
                            info!(target: "citadel","Service B Waiting for MemberStateChanged - Received {service_b_group_inbound:?}");
                        }
                    } else {
                        info!(target: "citadel","Service A Waiting for GroupJoinRequestReceived - Received {service_a_group_inbound:?}");
                    }
                }
            } else {
                panic!("Service B List Groups Failure");
            }

            // Service C Requests to Join and Service A Declines
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

                    //let _ = from_service_a.recv().await.unwrap(); // Receive MemberStateChanged Response that isn't required here
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
                            InternalServiceRequest::GroupRespondRequest {
                                cid: *cid_a,
                                peer_cid: *cid_c,
                                group_key: *group_key,
                                response: false,
                                request_id: Uuid::new_v4(),
                                invitation: false,
                            };
                        to_service_a.send(service_a_group_outbound).unwrap();
                        info!(target: "citadel","Service A Declined Join Request");
                        let service_c_group_inbound = from_service_c.recv().await.unwrap();
                        if let InternalServiceResponse::GroupRequestDeclined(
                            GroupRequestDeclined { .. },
                        ) = &service_c_group_inbound
                        {
                            info!(target: "citadel", "Service C Successfully Received Decline Response for Request Join");
                        } else {
                            panic!("Service C Waiting for Disconnected Response - Received {service_c_group_inbound:?}");
                        }
                    } else {
                        info!(target: "citadel","Service A Waiting for GroupJoinRequestReceived - Received {service_a_group_inbound:?}");
                    }
                } else {
                    panic!("Service C Panicked While Finding Group To Join");
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
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_b,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                info!(target: "citadel","Service B Sending Invite Response");
                to_service_b.send(group_invite_response).unwrap();
                let deserialized_service_b_payload_response = from_service_b.recv().await.unwrap();
                info!(target: "citadel","Service B Response Sent");
                if let InternalServiceResponse::GroupRespondRequestSuccess(
                    GroupRespondRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_b_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service B: Successfully Declined Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(
                    GroupRespondRequestFailure {
                        cid: _,
                        message,
                        request_id: _,
                    },
                ) = &deserialized_service_b_payload_response
                {
                    panic!("Service B Failed Upon Responding to Group Invite: {message:?}");
                }
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
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_c,
                    peer_cid: *peer_cid,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                info!(target: "citadel","Service C Sending Invite Response");
                to_service_c.send(group_invite_response).unwrap();
                let deserialized_service_c_payload_response = from_service_c.recv().await.unwrap();
                info!(target: "citadel","Service C Response Sent");
                if let InternalServiceResponse::GroupRespondRequestSuccess(
                    GroupRespondRequestSuccess {
                        cid: _,
                        group_key,
                        request_id: _,
                    },
                ) = &deserialized_service_c_payload_response
                {
                    assert_eq!(*group_key, owner_group_key.clone());
                    info!(target: "citadel","Service C: Successfully Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(
                    GroupRespondRequestFailure {
                        cid: _,
                        message,
                        request_id: _,
                    },
                ) = &deserialized_service_c_payload_response
                {
                    panic!("Service C Failed Upon Responding to Group Invite: {message:?}");
                }
            } else {
                panic!("Service C Invitation Not Received");
            }

            // Service C Leaves Group
            let service_c_outbound = InternalServiceRequest::GroupLeave {
                cid: *cid_c,
                group_key: owner_group_key,
                request_id: Uuid::new_v4(),
            };
            info!(target: "citadel","Service C Leaving Group");
            to_service_c.send(service_c_outbound).unwrap();
            let service_c_inbound = from_service_c.recv().await.unwrap();
            if let InternalServiceResponse::GroupLeaveSuccess(GroupLeaveSuccess { .. }) =
                &service_c_inbound
            {
                info!(target: "citadel","Service C Successfully Requested to Leave Group");
            } else {
                panic!("Service C panicked while attempting to leave group");
            }
            let service_c_inbound = from_service_c.recv().await.unwrap();
            if let InternalServiceResponse::GroupLeft(GroupLeft {
                cid: _,
                group_key: _,
                success,
                message: _,
                request_id: _,
            }) = &service_c_inbound
            {
                assert!(success);
                info!(target: "citadel","Service C Successfully Left Group");
            } else {
                panic!("Service C Failed to Leave Group");
            }

            // Service A Ends Group
            let service_a_outbound = InternalServiceRequest::GroupEnd {
                cid: *cid_a,
                group_key: owner_group_key,
                request_id: Uuid::new_v4(),
            };
            info!(target: "citadel","Service A Ending Group");
            to_service_a.send(service_a_outbound).unwrap();
            for _ in 0..4 {
                // Receive the four MemberStateChanged Responses that are not needed here
                let _ = from_service_a.recv().await.unwrap();
            }
            let service_a_inbound = from_service_a.recv().await.unwrap();
            if let InternalServiceResponse::GroupEndSuccess(GroupEndSuccess { .. }) =
                &service_a_inbound
            {
                let service_a_inbound = from_service_a.recv().await.unwrap();
                if let InternalServiceResponse::GroupEnded(GroupEnded {
                    cid: _,
                    group_key: ended_group,
                    success,
                    request_id: _,
                }) = &service_a_inbound
                {
                    assert_eq!(ended_group, group_key);
                    assert!(success);
                    info!(target: "citadel","Service A Successfully Ended Group");
                } else {
                    info!(target: "citadel", "Service A Waiting GroupEndSuccess and Received {service_a_inbound:?}");
                }
            } else {
                info!(target: "citadel", "Service A Waiting For GroupEnded Confirmation - Received {service_a_inbound:?}");
            }
        } else {
            panic! {"Group Creation Error: Service A did not receive success response"};
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_group_kick() -> Result<(), Box<dyn Error>> {
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

        if let InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
            cid: _,
            group_key,
            request_id: _,
        }) = &deserialized_service_a_payload_response
        {
            // Service B Accepts Invitation
            let service_b_group_create_invite = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::GroupInvitation(..) = &service_b_group_create_invite {
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_b,
                    peer_cid: *cid_a,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                to_service_b.send(group_invite_response).unwrap();
                let deserialized_service_b_payload_response = from_service_b.recv().await.unwrap();
                if let InternalServiceResponse::GroupRespondRequestSuccess(..) =
                    &deserialized_service_b_payload_response
                {
                    info!(target: "citadel","Service B Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(..) =
                    &deserialized_service_b_payload_response
                {
                    panic!("Service B Failed Upon Responding to Group Invite");
                }
            } else {
                panic!("Service B Invitation Not Received");
            }

            // Service C Accepts Group Invitation
            let service_c_group_create_invite = from_service_c.recv().await.unwrap();
            if let InternalServiceResponse::GroupInvitation(..) = &service_c_group_create_invite {
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_c,
                    peer_cid: *cid_a,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                to_service_c.send(group_invite_response).unwrap();
                let deserialized_service_c_payload_response = from_service_c.recv().await.unwrap();
                if let InternalServiceResponse::GroupRespondRequestSuccess(..) =
                    &deserialized_service_c_payload_response
                {
                    info!(target: "citadel","Service C Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(..) =
                    &deserialized_service_c_payload_response
                {
                    panic!("Service C Failed Upon Responding to Group Invite");
                }
            } else {
                panic!("Service C Invitation Not Received");
            }

            // Service A Kicks the other group members
            let service_a_outbound = InternalServiceRequest::GroupKick {
                cid: *cid_a,
                peer_cid: *cid_b,
                group_key: *group_key,
                request_id: Uuid::new_v4(),
            };
            to_service_a.send(service_a_outbound).unwrap();

            let _ = from_service_a.recv().await.unwrap(); // Receive unnecessary MemberStateChanged
            let _ = from_service_a.recv().await.unwrap(); // responses from Service B and C joining
            let service_a_inbound = from_service_a.recv().await.unwrap();
            if let InternalServiceResponse::GroupKickSuccess(GroupKickSuccess {
                cid,
                group_key: kick_group,
                request_id: _,
            }) = &service_a_inbound
            {
                assert_eq!(cid, cid_a);
                assert_eq!(kick_group, group_key);
                info!(target: "citadel", "Service B was successfully kicked from the group {kick_group:?}");
            } else if let InternalServiceResponse::GroupKickFailure(GroupKickFailure {
                cid: _,
                message,
                request_id: _,
            }) = &service_a_inbound
            {
                panic!("Group Kick Error: Service B could not be kicked - {message:?}");
            } else {
                panic!("Group Kick Error: Received Unexpected Response {service_a_inbound:?}");
            }

            let service_a_outbound = InternalServiceRequest::GroupKick {
                cid: *cid_a,
                peer_cid: *cid_c,
                group_key: *group_key,
                request_id: Uuid::new_v4(),
            };
            to_service_a.send(service_a_outbound).unwrap();

            let service_a_inbound = from_service_a.recv().await.unwrap();
            if let InternalServiceResponse::GroupKickSuccess(GroupKickSuccess {
                cid,
                group_key: kick_group,
                request_id: _,
            }) = &service_a_inbound
            {
                assert_eq!(cid, cid_a);
                assert_eq!(kick_group, group_key);
                info!(target: "citadel", "Service C was successfully kicked from the group {kick_group:?}");
            } else if let InternalServiceResponse::GroupKickFailure(GroupKickFailure {
                cid: _,
                message,
                request_id: _,
            }) = &service_a_inbound
            {
                panic!("Group Kick Error: Service C could not be kicked - {message:?}");
            } else {
                panic!("Group Kick Error: Received Unexpected Response {service_a_inbound:?}");
            }

            // Service B is notified that it was kicked
            let _ = from_service_b.recv().await.unwrap(); // MemberStateChanged from Service C Joining
            let service_b_inbound = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::GroupDisconnected(GroupDisconnected {
                cid: _,
                group_key: disconnected_group,
                request_id: _,
            }) = &service_b_inbound
            {
                assert_eq!(group_key, disconnected_group);
            } else {
                panic! {"Service B did not received expected kick notification - instead received {service_b_inbound:?}"};
            }

            // Service C is notified that it was kicked
            let _ = from_service_c.recv().await.unwrap(); // MemberStateChanged from Service B getting kicked
            let _ = from_service_c.recv().await.unwrap(); // Extra MemberStateChanged Not Needed here
            let service_c_inbound = from_service_c.recv().await.unwrap();
            if let InternalServiceResponse::GroupDisconnected(GroupDisconnected {
                cid: _,
                group_key: disconnected_group,
                request_id: _,
            }) = &service_c_inbound
            {
                assert_eq!(group_key, disconnected_group);
            } else {
                panic! {"Service C did not received expected kick notification - instead received {service_c_inbound:?}"};
            }
        } else {
            panic! {"Group Creation Error: Service A did not receive success response"};
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_citadel_workspace_service_group_message() -> Result<(), Box<dyn Error>> {
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

        if let InternalServiceResponse::GroupCreateSuccess(GroupCreateSuccess {
            cid: _,
            group_key,
            request_id: _,
        }) = &deserialized_service_a_payload_response
        {
            // Service B Accepts Invitation
            let service_b_group_create_invite = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::GroupInvitation(..) = &service_b_group_create_invite {
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_b,
                    peer_cid: *cid_a,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                to_service_b.send(group_invite_response).unwrap();
                let deserialized_service_b_payload_response = from_service_b.recv().await.unwrap();
                if let InternalServiceResponse::GroupRespondRequestSuccess(..) =
                    &deserialized_service_b_payload_response
                {
                    info!(target: "citadel","Service B Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(..) =
                    &deserialized_service_b_payload_response
                {
                    panic!("Service B Failed Upon Responding to Group Invite");
                }
            } else {
                panic!("Service B Invitation Not Received");
            }

            // Service C Accepts Group Invitation
            let service_c_group_create_invite = from_service_c.recv().await.unwrap();
            if let InternalServiceResponse::GroupInvitation(..) = &service_c_group_create_invite {
                let group_invite_response = InternalServiceRequest::GroupRespondRequest {
                    cid: *cid_c,
                    peer_cid: *cid_a,
                    group_key: *group_key,
                    response: true,
                    request_id: Uuid::new_v4(),
                    invitation: true,
                };
                to_service_c.send(group_invite_response).unwrap();
                let deserialized_service_c_payload_response = from_service_c.recv().await.unwrap();
                if let InternalServiceResponse::GroupRespondRequestSuccess(..) =
                    &deserialized_service_c_payload_response
                {
                    info!(target: "citadel","Service C Accepted Group Invite");
                } else if let InternalServiceResponse::GroupRespondRequestFailure(..) =
                    &deserialized_service_c_payload_response
                {
                    panic!("Service C Failed Upon Responding to Group Invite");
                }
            } else {
                panic!("Service C Invitation Not Received");
            }

            let _ = from_service_a.recv().await.unwrap(); // Receive Unnecessary MemberStateChanged Responses
            let _ = from_service_a.recv().await.unwrap(); // from Service B and Service C Joining Group
            let _ = from_service_b.recv().await.unwrap();

            let service_a_message = "Service A Test Message".serialize_to_vector().unwrap();
            let service_b_message = "Service B Test Message".serialize_to_vector().unwrap();

            // Service A Sends a Message
            let service_a_outbound = InternalServiceRequest::GroupMessage {
                cid: *cid_a,
                message: service_a_message.clone(),
                group_key: *group_key,
                request_id: Uuid::new_v4(),
            };
            to_service_a.send(service_a_outbound).unwrap();
            let service_a_inbound = from_service_a.recv().await.unwrap();
            if let InternalServiceResponse::GroupMessageSuccess(GroupMessageSuccess { .. }) =
                &service_a_inbound
            {
                info!(target: "citadel","Service A Received GroupMessageSuccess");

                // All Services Receive Message
                let service_b_inbound = from_service_b.recv().await.unwrap();
                if let InternalServiceResponse::GroupMessageReceived(GroupMessageReceived {
                    cid: _,
                    peer_cid: _,
                    message,
                    group_key: _,
                    request_id: _,
                }) = &service_b_inbound
                {
                    info!(target: "citadel","Service B received message from Group A");
                    assert_eq!(*message, service_a_message.clone());
                } else {
                    panic!("Service B Did Not Receive Message - instead received {service_b_inbound:?}");
                }
                let service_c_inbound = from_service_c.recv().await.unwrap();
                if let InternalServiceResponse::GroupMessageReceived(GroupMessageReceived {
                    cid: _,
                    peer_cid: _,
                    message,
                    group_key: _,
                    request_id: _,
                }) = &service_c_inbound
                {
                    info!(target: "citadel","Service C received message from Group A");
                    assert_eq!(*message, service_a_message.clone());
                } else {
                    panic!("Service C Did Not Receive Message - instead received {service_c_inbound:?}");
                }
                let service_a_inbound = from_service_a.recv().await.unwrap();
                if let InternalServiceResponse::GroupMessageResponse(GroupMessageResponse {
                    cid: _,
                    group_key: _,
                    success,
                    request_id: _,
                }) = &service_a_inbound
                {
                    if *success {
                        info!(target: "citadel","Service A Successfully received Group Message Response");
                    } else {
                        panic!("Service A Group Message Response was unsuccessful");
                    }
                } else {
                    panic!("Service A Did Not Receive Message Response - instead received {service_a_inbound:?}");
                }
            } else {
                panic!("Service A Did Not Receive GroupMessageSuccess - instead received {service_a_inbound:?}");
            }

            // Service B Sends a Message
            let service_b_outbound = InternalServiceRequest::GroupMessage {
                cid: *cid_b,
                message: service_b_message.clone(),
                group_key: *group_key,
                request_id: Uuid::new_v4(),
            };
            to_service_b.send(service_b_outbound).unwrap();
            let service_b_inbound = from_service_b.recv().await.unwrap();
            if let InternalServiceResponse::GroupMessageSuccess(GroupMessageSuccess { .. }) =
                &service_b_inbound
            {
                info!(target: "citadel","Service B Received GroupMessageSuccess");

                // All Services Receive Message
                let service_a_inbound = from_service_a.recv().await.unwrap();
                if let InternalServiceResponse::GroupMessageReceived(GroupMessageReceived {
                    cid: _,
                    peer_cid: _,
                    message,
                    group_key: _,
                    request_id: _,
                }) = &service_a_inbound
                {
                    info!(target: "citadel","Service A received message from Service B in Group");
                    assert_eq!(*message, service_b_message.clone());
                } else {
                    panic!("Service A Did Not Receive Message - instead received {service_a_inbound:?}");
                }
                let service_c_inbound = from_service_c.recv().await.unwrap();
                if let InternalServiceResponse::GroupMessageReceived(GroupMessageReceived {
                    cid: _,
                    peer_cid: _,
                    message,
                    group_key: _,
                    request_id: _,
                }) = &service_c_inbound
                {
                    info!(target: "citadel","Service C received message from Service B in Group");
                    assert_eq!(*message, service_b_message.clone());
                } else {
                    panic!("Service C Did Not Receive Message - instead received {service_c_inbound:?}");
                }
                let service_b_inbound = from_service_b.recv().await.unwrap();
                if let InternalServiceResponse::GroupMessageResponse(GroupMessageResponse {
                    cid: _,
                    group_key: _,
                    success,
                    request_id: _,
                }) = &service_b_inbound
                {
                    if *success {
                        info!(target: "citadel","Service B Successfully received Group Message Response");
                    } else {
                        panic!("Service B Group Message Response was unsuccessful");
                    }
                } else {
                    panic!("Service B Did Not Receive Message Response - instead received {service_b_inbound:?}");
                }
            } else {
                panic!("Service B Did Not Receive GroupMessageSuccess - instead received {service_b_inbound:?}");
            }
        } else {
            panic! {"Group Creation Error: Service A did not receive success response"};
        }

        Ok(())
    }
}
