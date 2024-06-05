use super::sum_numbers::{ReportSumMsg, SumNumbersComponent, SumRequest};
use crate::messaging::{CanSend, IntoSender};
use crate::test_loop::delay_sender::DelaySender;
use crate::{time, v2};

/// Let's pretend that when we send a remote request, the number gets sent to
/// every other instance in the setup as a local request.
fn forward_remote_request_to_other_instances(
    sum_request_streams: &[DelaySender<SumRequest>],
    source_index: usize,
    request: i64,
) {
    for i in 0..sum_request_streams.len() {
        if i != source_index {
            sum_request_streams[i].send(SumRequest::Number(request));
        }
    }
}

#[test]
fn test_multi_instance() {
    let mut test = v2::TestLoop::new();
    let mut sum_request_streams = Vec::new(); // 5 event handles
    let mut all_sums = Vec::new(); // 5 data handles
    let mut remote_requests = Vec::new(); // 5 event handles
    for _ in 0..5 {
        let report_sum_stream = test.register_event::<ReportSumMsg>();
        let summer =
            test.add_data(SumNumbersComponent::new(report_sum_stream.sender().into_sender()));
        let sums = test.add_data(Vec::<ReportSumMsg>::new());

        report_sum_stream.add_handler1(&mut test, sums, |msg, data| {
            data.push(msg);
            Ok(())
        });

        let sum_request = test.register_event::<SumRequest>();
        sum_request.add_handler1(&mut test, summer, |msg, data| {
            data.handle(msg);
            Ok(())
        });

        sum_request_streams.push(sum_request);
        all_sums.push(sums);

        let remote_request = test.register_event::<i64>();
        remote_requests.push(remote_request);
    }

    for i in 0..5 {
        let sum_request_senders =
            sum_request_streams.iter().map(|s| s.sender()).collect::<Vec<_>>();
        remote_requests[i].add_handler0(&mut test, move |request| {
            forward_remote_request_to_other_instances(&sum_request_senders, i, request);
            Ok(())
        });
    }

    // Send a RemoteRequest from each instance.
    remote_requests[0].sender().send(1);
    remote_requests[1].sender().send(2);
    remote_requests[2].sender().send(3);
    remote_requests[3].sender().send(4);
    remote_requests[4].sender().send(5);

    // Then send a GetSum request for each instance; we use a delay so that we can ensure
    // these messages arrive later. (In a real test we wouldn't do this - the component would
    // automatically emit some events and we would assert on these events. But for this
    // contrived test we'll do it manually as a demonstration.)
    for i in 0..5 {
        sum_request_streams[i]
            .sender()
            .send_with_delay(SumRequest::GetSum, time::Duration::milliseconds(1));
    }
    test.run_for(time::Duration::milliseconds(2));
    assert_eq!(test.data(all_sums[0]), &vec![ReportSumMsg(14)]);
    assert_eq!(test.data(all_sums[1]), &vec![ReportSumMsg(13)]);
    assert_eq!(test.data(all_sums[2]), &vec![ReportSumMsg(12)]);
    assert_eq!(test.data(all_sums[3]), &vec![ReportSumMsg(11)]);
    assert_eq!(test.data(all_sums[4]), &vec![ReportSumMsg(10)]);
}
