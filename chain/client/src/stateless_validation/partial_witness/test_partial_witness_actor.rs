#![allow(dead_code)]

use near_async::actix_wrapper::spawn_actix_actor;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{Actor, Handler, HandlerWithContext};
use near_o11y::WithSpanContextExt;

pub struct TestActorA {}

impl Actor for TestActorA {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {}
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub enum Msg1 {
    A,
    B,
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub enum Msg2 {
    C,
    D,
}

impl Handler<Msg1> for TestActorA {
    fn handle(&mut self, msg: Msg1) {
        match msg {
            Msg1::A => println!("A"),
            Msg1::B => println!("B"),
        }
    }
}

impl HandlerWithContext<Msg2> for TestActorA {
    fn handle(&mut self, msg: Msg2, _ctx: &mut dyn DelayedActionRunner<Self>) {
        match msg {
            Msg2::C => println!("C"),
            Msg2::D => println!("D"),
        }
    }
}

// ************************************************************************************************

#[actix_rt::test]
async fn test() {
    let actor_a = TestActorA {};
    let (addr, _) = spawn_actix_actor(actor_a);
    let _res = addr.send(Msg1::A.with_span_context());
    let _res = addr.send(Msg2::C.with_span_context());
}

// ************************************************************************************************

// #[shreyan_handler(Msg1, Msg2)]
// pub trait ActorBtoActorAMessageSender {}

// expands to
// pub trait ActorBtoActorAMessageSender: Send<Msg1> + Send<Msg2> {}

// impl dyn ActorBtoActorAMessageSender {
//     pub fn from_actix_addr<T>(
//         addr: actix::Addr<ActixWrapper<T>>,
//     ) -> Arc<dyn ActorBtoActorAMessageSender>
//     where
//         T: Handler<Msg1> + Handler<Msg2> + Sized + Unpin + 'static,
//     {
//         Arc::new(_InternalActixSenderWrapper { sender: addr })
//     }

//     pub fn from_actor<T>(actor: T) -> Arc<dyn ActorBtoActorAMessageSender>
//     where
//         T: Handler<Msg1> + Handler<Msg2> + Sized + Unpin + 'static,
//     {
//     }
// }

// pub struct SyncActorBtoActorAMessageSender<T>
// where
//     T: Handler<Msg1> + Handler<Msg2>,
// {
//     actor: T,
// }

// pub struct _InternalActixSenderWrapper<T>
// where
//     T: Sized + Unpin + 'static,
// {
//     sender: actix::Addr<ActixWrapper<T>>,
// }

// impl<T> ActorBtoActorAMessageSender for _InternalActixSenderWrapper<T> where
//     T: Handler<Msg1> + Handler<Msg2> + Sized + Unpin + 'static
// {
// }

// impl<M, T> Send<M> for _InternalActixSenderWrapper<T>
// where
//     M: actix::Message + core::fmt::Debug + std::marker::Send + 'static,
//     T: Handler<M> + Sized + Unpin + 'static,
//     <M as actix::Message>::Result:
//         actix::dev::MessageResponse<ActixWrapper<T>, M> + std::marker::Send,
// {
//     fn send(&self, message: M) {
//         println!("sending message {:?}", message);
//         // How exactly do I call acitx send message?
//         self.sender.do_send(message);
//     }
// }

// pub fn from_actix_addr<T>(addr: actix::Addr<ActixWrapper<T>>) -> Arc<dyn MultiMessage>
// where
//     T: MultiMessageHandler + Sized + Unpin + 'static,
// {
//     Arc::new(SenderWrapper { sender: addr })
// }

// pub struct SenderWrapper<T>
// where
//     T: Sized + Unpin + 'static,
// {
//     sender: actix::Addr<ActixWrapper<T>>,
// }

// impl<M, T> Send<M> for SenderWrapper<T>
// where
//     M: actix::Message,
//     T: Sized + Unpin + 'static,
// {
//     fn send(&self, message: M) {
//         // self.sender.send(message);
//     }
// }

// impl dyn MultiMessage {
//     pub fn from_actix_addr<T: actix::Actor>(addr: actix::Addr<T>) {}
// }

// need send for MultiMessage
