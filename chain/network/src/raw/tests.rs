
use crate::network_protocol::{
    Encoding, Handshake, PeerIdOrHash, PeerMessage, Pong, RawRoutedMessage, RoutedMessageBody,
};
use crate::raw;
use crate::time::Utc;
use near_crypto::{KeyType, SecretKey};
use near_primitives::network::PeerId;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug)]
enum PeerMsgType {
    Handshake,
    Pong(u64),
    // the significance of this is that it's a message that we don't care to receive
    // in the ReceivedMessage type, and it's the easiest one to construct
    PeersRequest,
}

#[derive(Debug, PartialEq, Eq)]
enum ReceivedMessageType {
    // not testing this for now, but include it to make implementing From possible
    AnnounceAccounts,
    Pong(u64),
}

impl From<raw::ReceivedMessage> for ReceivedMessageType {
    fn from(m: raw::ReceivedMessage) -> Self {
        match m {
            raw::ReceivedMessage::AnnounceAccounts(_) => Self::AnnounceAccounts,
            raw::ReceivedMessage::Pong { nonce, .. } => Self::Pong(nonce),
        }
    }
}

enum RecvResult {
    Ok(&'static [ReceivedMessageType]),
    ConnectErr,
}

struct TestCase {
    msg_sequence: &'static [PeerMsgType],
    want: RecvResult,
}

static TESTCASES: &[TestCase] = &[
    TestCase {
        msg_sequence: &[PeerMsgType::Handshake, PeerMsgType::Pong(1)],
        want: RecvResult::Ok(&[ReceivedMessageType::Pong(1)]),
    },
    // no handshake = bad
    TestCase {
        msg_sequence: &[PeerMsgType::PeersRequest, PeerMsgType::Pong(1)],
        want: RecvResult::ConnectErr,
    },
    // same as first one but w/ some ignored ones before
    TestCase {
        msg_sequence: &[
            PeerMsgType::Handshake,
            PeerMsgType::PeersRequest,
            PeerMsgType::PeersRequest,
            PeerMsgType::Pong(1),
        ],
        want: RecvResult::Ok(&[ReceivedMessageType::Pong(1)]),
    },
    TestCase {
        msg_sequence: &[
            PeerMsgType::Handshake,
            PeerMsgType::PeersRequest,
            PeerMsgType::Pong(1),
            PeerMsgType::PeersRequest,
            PeerMsgType::Pong(2),
            PeerMsgType::PeersRequest,
            PeerMsgType::Pong(3),
        ],
        want: RecvResult::Ok(&[
            ReceivedMessageType::Pong(1),
            ReceivedMessageType::Pong(2),
            ReceivedMessageType::Pong(3),
        ]),
    },
];

struct Server {
    secret_key: SecretKey,
    peer_id: PeerId,
    listener: TcpListener,
}

async fn write_message<S: AsyncWrite + Unpin>(stream: &mut S, msg: &PeerMessage) -> io::Result<()> {
    let mut msg = msg.serialize(Encoding::Proto);
    let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
    buf.append(&mut msg);
    stream.write_all(&buf).await
}

impl Server {
    async fn new() -> Self {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let peer_id = PeerId::new(secret_key.public_key());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self { secret_key, peer_id, listener }
    }

    async fn run(self, msg_sequence: &'static [PeerMsgType]) -> anyhow::Result<()> {
        let (mut stream, _addr) = self.listener.accept().await.unwrap();
        let (read, mut write) = stream.split();
        let mut read = tokio::io::BufReader::new(read);

        let len = read.read_u32_le().await.unwrap();
        let mut buf = vec![0; len as usize];
        read.read_exact(&mut buf).await.unwrap();

        let msg = PeerMessage::deserialize(Encoding::Proto, &buf).unwrap();

        let my_handshake = match msg {
            PeerMessage::Handshake(h) => {
                Handshake {
                    protocol_version: h.protocol_version,
                    oldest_supported_version: h.oldest_supported_version,
                    sender_peer_id: self.peer_id.clone(),
                    target_peer_id: h.sender_peer_id,
                    sender_listen_port: Some(24567),
                    sender_chain_info: h.sender_chain_info,
                    // Note that this is wrong, but we aren't looking at the handshake right now,
                    // so not worth the effort to do this correctly
                    partial_edge_info: h.partial_edge_info,
                }
            }
            _ => anyhow::bail!("received bad first message: {:?}", &msg),
        };

        for m in msg_sequence {
            match m {
                PeerMsgType::Handshake => {
                    write_message(&mut write, &PeerMessage::Handshake(my_handshake.clone()))
                        .await
                        .unwrap();
                }
                PeerMsgType::Pong(nonce) => {
                    let body = RoutedMessageBody::Pong(Pong {
                        nonce: *nonce,
                        source: self.peer_id.clone(),
                    });
                    let msg = RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(my_handshake.target_peer_id.clone()),
                        body,
                    }
                    .sign(&self.secret_key, 100, Some(Utc::now_utc()));

                    write_message(&mut write, &PeerMessage::Routed(Box::new(msg))).await?;
                }
                PeerMsgType::PeersRequest => {
                    write_message(&mut write, &PeerMessage::PeersRequest).await.unwrap();
                }
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_raw_connection() {
    for t in TESTCASES.iter() {
        let server = Server::new().await;
        let peer_id = server.peer_id.clone();
        let addr = server.listener.local_addr().unwrap();

        let server_handle = tokio::spawn(server.run(&t.msg_sequence));

        let conn =
            raw::Connection::connect(addr, peer_id, None, "unittestnet", Default::default(), 0, 1)
                .await;
        let msgs_wanted = match &t.want {
            RecvResult::Ok(m) => m,
            RecvResult::ConnectErr => {
                if !conn.is_err() {
                    panic!(
                        "conn succeeded when we wanted it to fail with msg sequence {:?}",
                        &t.msg_sequence
                    );
                }
                continue;
            }
        };
        let mut conn = conn.unwrap();
        let mut got = Vec::new();

        loop {
            match conn.recv().await {
                Ok((m, _timestamp)) => {
                    got.push(m);
                }
                Err(_) => {
                    break;
                }
            }
        }

        let got = got.into_iter().map(Into::into).collect::<Vec<ReceivedMessageType>>();
        assert_eq!(&got, msgs_wanted);
        server_handle.await.unwrap().unwrap();
    }
}
