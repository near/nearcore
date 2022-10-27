use crate::broadcast;

#[tokio::test]
async fn channel() {
    let (send, mut recv) = broadcast::unbounded_channel();
    send.send(1);
    send.send(2);
    send.send(3);
    assert_eq!(1, recv.recv().await);
    let mut recv2 = recv.clone();
    assert_eq!(2, recv.recv().await);
    assert_eq!(3, recv.recv().await);
    assert_eq!(2, recv2.recv().await);
    assert_eq!(3, recv2.recv().await);
}
