use crate::messaging::Sender;

#[derive(Debug, PartialEq, Eq)]
pub struct ReportSumMsg(pub i64);

#[derive(Debug)]
pub enum SumRequest {
    Number(i64),
    GetSum,
}

// Mimics a typical backing component of some actor in nearcore. Handles request
// messages, and sends some other messages to another actor. The other actor is
// abstracted with an Sender here. We'll show how to test this in
// sum_numbers_test.rs.
pub struct SumNumbersComponent {
    result_sender: Sender<ReportSumMsg>,
    numbers: Vec<i64>,
}

impl SumNumbersComponent {
    pub fn new(result_sender: Sender<ReportSumMsg>) -> Self {
        Self { result_sender, numbers: vec![] }
    }

    pub fn handle(&mut self, msg: SumRequest) {
        match msg {
            SumRequest::Number(n) => self.numbers.push(n),
            SumRequest::GetSum => {
                let sum = self.numbers.iter().sum();
                self.numbers.clear();
                self.result_sender.send(ReportSumMsg(sum));
            }
        }
    }
}
