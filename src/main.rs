use hbb_common::{
    bytes::BytesMut,
    protobuf::Message as _,
    rendezvous_proto::*,
    tcp::{new_listener, FramedStream},
    tokio,
    udp::FramedSocket,
};

#[tokio::main(basic_scheduler)]
async fn main() {
    let mut socket = FramedSocket::new("0.0.0.0:21116").await.unwrap();
    let mut listener = new_listener("0.0.0.0:21116", false).await.unwrap();
    let mut rlistener = new_listener("0.0.0.0:21117", false).await.unwrap();
    let mut id_map = std::collections::HashMap::new();
    let relay_server = std::env::var("IP").unwrap();
    let mut saved_stream = None;
    loop {
        tokio::select! {
            Some(Ok((bytes, addr))) = socket.next() => {
                handle_udp(&mut socket, bytes, addr, &mut id_map).await;
            }
            Ok((stream, addr)) = listener.accept() => {
                let mut stream = FramedStream::from(stream);
                if let Some(Ok(bytes)) = stream.next_timeout(3000).await {
                    if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
                        match msg_in.union {
                            Some(RendezvousMessage_oneof_union::punch_hole_request(ph)) => {
                                println!("punch_hole_request {:?}", addr);
                                if let Some(addr) = id_map.get(&ph.id) {
                                    let mut msg_out = RendezvousMessage::new();
                                    msg_out.set_request_relay(RequestRelay {
                                        relay_server: relay_server.clone(),
                                        ..Default::default()
                                    });
                                    socket.send(&msg_out, addr.clone()).await.ok();
                                    saved_stream = Some(stream);
                                }
                            }
                            Some(RendezvousMessage_oneof_union::relay_response(_)) => {
                                println!("relay_response {:?}", addr);
                                let mut msg_out = RendezvousMessage::new();
                                msg_out.set_relay_response(RelayResponse {
                                    relay_server: relay_server.clone(),
                                    ..Default::default()
                                });
                                if let Some(mut stream) = saved_stream.take() {
                                    stream.send(&msg_out).await.ok();
                                    if let Ok((stream_a, _)) = rlistener.accept().await {
                                        let mut stream_a = FramedStream::from(stream_a);
                                        stream_a.next_timeout(3_000).await;
                                        if let Ok((stream_b, _)) = rlistener.accept().await {
                                            let mut stream_b = FramedStream::from(stream_b);
                                            stream_b.next_timeout(3_000).await;
                                            relay(stream_a, stream_b, &mut socket, &mut id_map).await;
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

async fn relay(
    stream: FramedStream,
    peer: FramedStream,
    socket: &mut FramedSocket,
    id_map: &mut std::collections::HashMap<String, std::net::SocketAddr>,
) {
    let mut peer = peer;
    let mut stream = stream;
    peer.set_raw();
    stream.set_raw();
    loop {
        tokio::select! {
            Some(Ok((bytes, addr))) = socket.next() => {
                handle_udp(socket, bytes, addr, id_map).await;
            }
            res = peer.next() => {
                if let Some(Ok(bytes)) = res {
                    stream.send_bytes(bytes.into()).await.ok();
                } else {
                    break;
                }
            },
            res = stream.next() => {
                if let Some(Ok(bytes)) = res {
                    peer.send_bytes(bytes.into()).await.ok();
                } else {
                    break;
                }
            },
        }
    }
}

async fn handle_udp(
    socket: &mut FramedSocket,
    bytes: BytesMut,
    addr: std::net::SocketAddr,
    id_map: &mut std::collections::HashMap<String, std::net::SocketAddr>,
) {
    if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
        match msg_in.union {
            Some(RendezvousMessage_oneof_union::register_peer(rp)) => {
                println!("register_peer {:?}", addr);
                id_map.insert(rp.id, addr);
                let mut msg_out = RendezvousMessage::new();
                msg_out.set_register_peer_response(RegisterPeerResponse::new());
                socket.send(&msg_out, addr).await.ok();
            }
            Some(RendezvousMessage_oneof_union::register_pk(_)) => {
                println!("register_pk {:?}", addr);
                let mut msg_out = RendezvousMessage::new();
                msg_out.set_register_pk_response(RegisterPkResponse {
                    result: RegisterPkResponse_Result::OK,
                    ..Default::default()
                });
                socket.send(&msg_out, addr).await.ok();
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use hbb_common::{protobuf::Message as _, rendezvous_proto::*};

    fn decode(bytes: &[u8]) -> RendezvousMessage {
        RendezvousMessage::parse_from_bytes(bytes).expect("rendezvous message should decode")
    }

    #[test]
    fn decodes_register_peer_payload() {
        // What a client sends for RegisterPeer { id: "ab", serial: 1 },
        // wrapped as RendezvousMessage.register_peer (field 6).
        let bytes = [0x32, 0x06, 0x0a, 0x02, b'a', b'b', 0x10, 0x01];
        match decode(&bytes).union {
            Some(RendezvousMessage_oneof_union::register_peer(peer)) => {
                assert_eq!(peer.id, "ab");
                assert_eq!(peer.serial, 1);
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn decodes_punch_hole_request_payload() {
        // RendezvousMessage.punch_hole_request (field 8) with id "peer".
        let bytes = [0x42, 0x06, 0x0a, 0x04, b'p', b'e', b'e', b'r'];
        match decode(&bytes).union {
            Some(RendezvousMessage_oneof_union::punch_hole_request(request)) => {
                assert_eq!(request.id, "peer");
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn register_peer_response_round_trips() {
        let mut msg = RendezvousMessage::new();
        msg.set_register_peer_response(RegisterPeerResponse::new());
        let parsed = decode(&msg.write_to_bytes().unwrap());
        assert!(parsed.has_register_peer_response());
    }

    #[test]
    fn register_pk_response_ok_round_trips() {
        let mut msg = RendezvousMessage::new();
        msg.set_register_pk_response(RegisterPkResponse {
            result: RegisterPkResponse_Result::OK,
            ..Default::default()
        });
        let parsed = decode(&msg.write_to_bytes().unwrap());
        match parsed.union {
            Some(RendezvousMessage_oneof_union::register_pk_response(response)) => {
                assert_eq!(response.result, RegisterPkResponse_Result::OK);
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }
}
