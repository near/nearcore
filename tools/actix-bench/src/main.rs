use actix::prelude::*;

// this is our Message
// we have to define the response type (rtype)
#[derive(Message)]
#[rtype(usize)]
struct Sum(usize, usize);

// Actor definition
struct Calculator;

impl Actor for Calculator {
    type Context = Context<Self>;
}

// now we need to implement `Handler` on `Calculator` for the `Sum` message.
impl Handler<Sum> for Calculator {
    type Result = usize; // <- Message response type

    fn handle(&mut self, msg: Sum, _ctx: &mut Context<Self>) -> Self::Result {
        if msg.0 == 1234 {
            eprintln!("Sleeping...");
            std::thread::sleep(std::time::Duration::from_millis(10000)); // simulate some heavy computation
            eprintln!("Woke up!");
        }
        msg.0 + msg.1
    }
}

#[actix::main] // <- starts the system and block until future resolves
async fn main() {
    let addr = Calculator.start();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let inner = {
        let addr = addr.clone();
        runtime.spawn(async move {
            let futures = (0..1000000)
                .map(|_| {
                    let addr = addr.clone(); // <- clone the address to send messages
                    tokio::spawn(async move {
                        // eprintln!("About to send");
                        let res = addr.send(Sum(10, 5)).await; // <- send message and get future for result
                        match res {
                            Ok(result) => result,
                            _ => panic!("Communication to the actor has failed"),
                        }
                    })
                })
                .collect::<Vec<_>>();
            let result = futures::future::join_all(futures).await; // <- wait for all futures to complete
            let sum = result.into_iter().fold(0, |acc, x| acc + x.unwrap()); // <- sum all results
            println!("Sum of all sums: {}", sum);
        })
    };
    addr.do_send(Sum(1234, 5678));
    inner.await;
}
