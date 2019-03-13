//! A simple task converting transactions to payloads.
use futures::sync::mpsc::{Receiver, Sender};
use futures::{Future, Sink, Stream};
use primitives::chain::ChainPayload;

// TODO(#265): Include transaction verification here.
pub fn spawn_task<T: Send + 'static>(
    receiver: Receiver<T>,
    f: fn(T) -> ChainPayload,
    sender: Sender<ChainPayload>,
) {
    let task = receiver
        .map(f)
        .forward(
            sender.sink_map_err(|err| error!("Error sending payload down the sink: {:?}", err)),
        )
        .map(|_| ())
        .map_err(|err| error!("Error while converting signed transaction to payload: {:?}", err));
    tokio::spawn(task);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::sync::mpsc::channel;
    use futures::{lazy, stream};
    use primitives::signature::DEFAULT_SIGNATURE;
    use primitives::transaction::{SendMoneyTransaction, SignedTransaction, TransactionBody};

    #[test]
    fn pass_through() {
        tokio::run(lazy(|| {
            let (transaction_tx, transaction_rx) = channel(1024);
            let (payload_tx, payload_rx) = channel(1024);
            spawn_task(
                transaction_rx,
                |t| ChainPayload::new(vec![t], vec![]),
                payload_tx,
            );
            let mut transactions = vec![];
            for i in 0..10 {
                let t = SendMoneyTransaction {
                    nonce: i,
                    originator: "alice.near".to_string(),
                    receiver: "bob".to_string(),
                    amount: i,
                };
                let t = TransactionBody::SendMoney(t);
                transactions.push(SignedTransaction::new(DEFAULT_SIGNATURE, t));
            }

            let expected: Vec<u64> = (0..10).collect();

            let assert_task = stream::iter_ok(transactions)
                .forward(
                    transaction_tx
                        .sink_map_err(|err| panic!("Error sending fake transactions {:?}", err)),
                )
                .and_then(|_| {
                    payload_rx
                        .map(|p| match p.transactions[0] {
                            SignedTransaction {
                                body:
                                    TransactionBody::SendMoney(SendMoneyTransaction {
                                        amount: a, ..
                                    }),
                                ..
                            } => a,
                            _ => panic!("Unexpected transaction"),
                        })
                        .collect()
                        .map(move |actual| {
                            assert_eq!(actual, expected);
                        })
                        .map_err(|err| error!("Assertion error {:?}", err))
                });
            tokio::spawn(assert_task);
            Ok(())
        }));
    }
}
