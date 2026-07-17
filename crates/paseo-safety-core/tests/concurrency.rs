use std::sync::{Arc, Barrier, Mutex};

use paseo_safety_core::{
    Applied, Command, InteractionSequence, ProposalId, ReplyId, ResponseBody, SafetyCore,
    SummaryId, ThreadId,
};

#[test]
fn concurrent_confirmations_authorize_at_most_one_dispatch() {
    let summary = SummaryId::new("summary").expect("valid summary ID");
    let proposal = ProposalId::new("proposal").expect("valid proposal ID");
    let mut core = SafetyCore::new(100);
    core.apply(Command::ObserveReply {
        summary_id: summary.clone(),
        source_thread_id: ThreadId::new("thread").expect("valid thread ID"),
        source_reply_id: ReplyId::new("reply").expect("valid reply ID"),
        observed_at_ms: 1,
    })
    .expect("observe reply");
    core.apply(Command::MarkSummaryReady {
        summary_id: summary.clone(),
    })
    .expect("ready summary");
    core.apply(Command::ActivateNext).expect("activate summary");
    core.apply(Command::ProposeResponse {
        proposal_id: proposal.clone(),
        summary_id: summary,
        response: ResponseBody::new("response").expect("valid response"),
        interaction: InteractionSequence::new(1),
        now_ms: 1,
    })
    .expect("propose response");

    let core = Arc::new(Mutex::new(core));
    let barrier = Arc::new(Barrier::new(3));
    let handles = (0..2)
        .map(|_| {
            let core = Arc::clone(&core);
            let barrier = Arc::clone(&barrier);
            let proposal = proposal.clone();
            std::thread::spawn(move || {
                barrier.wait();
                core.lock()
                    .expect("state lock")
                    .apply(Command::ConfirmResponse {
                        proposal_id: proposal,
                        interaction: InteractionSequence::new(2),
                        now_ms: 2,
                    })
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();

    let authorizations = handles
        .into_iter()
        .map(|handle| handle.join().expect("confirmation thread"))
        .filter(|result| matches!(result, Ok(Applied::DispatchAuthorized(_))))
        .count();
    assert_eq!(authorizations, 1);
}
