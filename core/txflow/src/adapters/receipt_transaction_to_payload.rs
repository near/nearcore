//! A simple task converting transactions to payloads.
use futures::sync::mpsc::{Receiver, Sender};
use futures::{Future, Sink, Stream};
use primitives::types::{ChainPayload, Transaction, ReceiptTransaction};

pub fn spawn_task(receiver: Receiver<ReceiptTransaction>, sender: Sender<ChainPayload>) {
    let task = receiver
        .map(|t| ChainPayload { body: vec![Transaction::Receipt(t)] })
        .forward(
            sender.sink_map_err(|err| error!("Error sending payload down the sink: {:?}", err)),
        )
        .map(|_| ())
        .map_err(|err| error!("Error while converting receipt transaction to payload: {:?}", err));
    tokio::spawn(task);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{lazy, stream};
    use futures::sync::mpsc::channel;
    use primitives::types::{Transaction, ReceiptTransaction, ReceiptBody};
    use primitives::hash::CryptoHash;

    #[test]
    fn pass_through() {
        tokio::run(lazy(|| {
            let (transaction_tx, transaction_rx) = channel(1024);
            let (payload_tx, payload_rx) = channel(1024);
            spawn_task(transaction_rx, payload_tx);
            let mut transactions = vec![];
            for i in 0..10 {
                transactions.push(
                    ReceiptTransaction::new(CryptoHash::default(), CryptoHash::default(), vec![],
                                            ReceiptBody::Refund(i)
                ));
            }

            let expected: Vec<u64> = (0..10).collect();

            let assert_task = stream::iter_ok(transactions)
                .forward(
                    transaction_tx.sink_map_err(|err| panic!("Error sending fake transactions {:?}", err))
                )
                .and_then(|_|
                payload_rx.map(|p| match p.body[0] {
                    Transaction::Receipt(
                        ReceiptTransaction { body: ReceiptBody::Refund(r), .. }) => r,
                    _ => panic!("Unexpected transaction")
                })
                    .collect().map(move |actual| {
                    assert_eq!(actual, expected);
                }).map_err(|err| error!("Assertion error {:?}", err))
            );
            tokio::spawn(assert_task);
            Ok(())
        }));
    }
}
