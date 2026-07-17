use paseo_safety_core::{
    Applied, Command, DeliveryOutcome, InteractionSequence, ProposalId, ReplyId, ResponseBody,
    SafetyCore, SafetyError, SummaryId, ThreadId,
};
use proptest::prelude::*;

fn summary_id(value: &str) -> SummaryId {
    SummaryId::new(value).expect("valid summary ID")
}

fn thread_id(value: &str) -> ThreadId {
    ThreadId::new(value).expect("valid thread ID")
}

fn reply_id(value: &str) -> ReplyId {
    ReplyId::new(value).expect("valid reply ID")
}

fn proposal_id(value: &str) -> ProposalId {
    ProposalId::new(value).expect("valid proposal ID")
}

fn observe_ready_and_activate(
    core: &mut SafetyCore,
    summary: &SummaryId,
    thread: &ThreadId,
    reply: &str,
) {
    core.apply(Command::ObserveReply {
        summary_id: summary.clone(),
        source_thread_id: thread.clone(),
        source_reply_id: reply_id(reply),
        observed_at_ms: 10,
    })
    .expect("observe reply");
    core.apply(Command::MarkSummaryReady {
        summary_id: summary.clone(),
    })
    .expect("mark ready");
    core.apply(Command::ActivateNext).expect("activate summary");
}

fn propose(
    core: &mut SafetyCore,
    summary: &SummaryId,
    proposal: &ProposalId,
    interaction: u64,
    now_ms: u64,
) {
    core.apply(Command::ProposeResponse {
        proposal_id: proposal.clone(),
        summary_id: summary.clone(),
        response: ResponseBody::new("  exact response\r\n").expect("valid response"),
        interaction: InteractionSequence::new(interaction),
        now_ms,
    })
    .expect("propose response");
}

#[test]
fn proposal_destination_is_derived_from_immutable_summary_provenance() {
    let mut core = SafetyCore::new(120_000);
    let summary = summary_id("summary-a");
    let thread = thread_id("thread-a");

    observe_ready_and_activate(&mut core, &summary, &thread, "reply-a");
    propose(&mut core, &summary, &proposal_id("proposal-a"), 7, 20);

    let applied = core
        .apply(Command::ConfirmResponse {
            proposal_id: proposal_id("proposal-a"),
            interaction: InteractionSequence::new(8),
            now_ms: 21,
        })
        .expect("confirm response");

    let Applied::DispatchAuthorized(authorization) = applied else {
        panic!("expected dispatch authorization");
    };
    assert_eq!(authorization.destination_thread_id(), &thread);
    assert_eq!(authorization.response().as_str(), "  exact response\r\n");
}

#[test]
fn confirmation_requires_a_later_interaction_and_unexpired_proposal() {
    let mut core = SafetyCore::new(100);
    let summary = summary_id("summary");
    let proposal = proposal_id("proposal");
    observe_ready_and_activate(&mut core, &summary, &thread_id("thread"), "reply");
    propose(&mut core, &summary, &proposal, 7, 20);

    assert_eq!(
        core.apply(Command::ConfirmResponse {
            proposal_id: proposal.clone(),
            interaction: InteractionSequence::new(7),
            now_ms: 119,
        }),
        Err(SafetyError::ConfirmationNotLater)
    );
    assert!(matches!(
        core.apply(Command::ConfirmResponse {
            proposal_id: proposal.clone(),
            interaction: InteractionSequence::new(8),
            now_ms: 119,
        }),
        Ok(Applied::DispatchAuthorized(_))
    ));

    let mut expired = SafetyCore::new(100);
    observe_ready_and_activate(&mut expired, &summary, &thread_id("thread"), "reply");
    propose(&mut expired, &summary, &proposal, 7, 20);
    assert_eq!(
        expired.apply(Command::ConfirmResponse {
            proposal_id: proposal,
            interaction: InteractionSequence::new(8),
            now_ms: 120,
        }),
        Err(SafetyError::ProposalExpired)
    );
}

#[test]
fn source_replies_are_deduplicated_and_ready_items_keep_observation_order() {
    let mut core = SafetyCore::new(100);
    let first = summary_id("summary-first");
    let second = summary_id("summary-second");
    let first_thread = thread_id("thread-first");

    core.apply(Command::ObserveReply {
        summary_id: first.clone(),
        source_thread_id: first_thread.clone(),
        source_reply_id: reply_id("reply-first"),
        observed_at_ms: 50,
    })
    .expect("observe first");
    assert_eq!(
        core.apply(Command::ObserveReply {
            summary_id: summary_id("ignored-duplicate"),
            source_thread_id: first_thread,
            source_reply_id: reply_id("reply-first"),
            observed_at_ms: 51,
        }),
        Ok(Applied::DuplicateReply)
    );
    core.apply(Command::ObserveReply {
        summary_id: second.clone(),
        source_thread_id: thread_id("thread-second"),
        source_reply_id: reply_id("reply-second"),
        observed_at_ms: 1,
    })
    .expect("observe second");

    core.apply(Command::MarkSummaryReady { summary_id: second })
        .expect("ready second");
    core.apply(Command::MarkSummaryReady {
        summary_id: first.clone(),
    })
    .expect("ready first");
    assert_eq!(
        core.apply(Command::ActivateNext),
        Ok(Applied::SummaryActivated(first))
    );
    assert_eq!(
        core.apply(Command::ActivateNext),
        Err(SafetyError::ActiveContextExists)
    );
}

#[test]
fn a_new_proposal_replaces_the_pending_proposal_and_cancel_is_terminal() {
    let mut core = SafetyCore::new(100);
    let summary = summary_id("summary");
    let old = proposal_id("old");
    let replacement = proposal_id("replacement");
    observe_ready_and_activate(&mut core, &summary, &thread_id("thread"), "reply");
    propose(&mut core, &summary, &old, 1, 1);
    propose(&mut core, &summary, &replacement, 2, 2);

    assert_eq!(
        core.apply(Command::ConfirmResponse {
            proposal_id: old,
            interaction: InteractionSequence::new(3),
            now_ms: 3,
        }),
        Err(SafetyError::InvalidProposalState)
    );
    assert_eq!(
        core.apply(Command::CancelResponse {
            proposal_id: replacement.clone(),
        }),
        Ok(Applied::ResponseCancelled)
    );
    assert_eq!(
        core.apply(Command::ConfirmResponse {
            proposal_id: replacement,
            interaction: InteractionSequence::new(3),
            now_ms: 3,
        }),
        Err(SafetyError::InvalidProposalState)
    );
}

#[test]
fn delivery_can_be_recorded_once_and_unknown_is_distinct() {
    let mut core = SafetyCore::new(100);
    let summary = summary_id("summary");
    let proposal = proposal_id("proposal");
    observe_ready_and_activate(&mut core, &summary, &thread_id("thread"), "reply");
    propose(&mut core, &summary, &proposal, 1, 1);
    core.apply(Command::ConfirmResponse {
        proposal_id: proposal.clone(),
        interaction: InteractionSequence::new(2),
        now_ms: 2,
    })
    .expect("authorize dispatch");

    assert_eq!(
        core.apply(Command::RecordDelivery {
            proposal_id: proposal.clone(),
            outcome: DeliveryOutcome::OutcomeUnknown,
        }),
        Ok(Applied::DeliveryRecorded(DeliveryOutcome::OutcomeUnknown))
    );
    assert_eq!(
        core.apply(Command::RecordDelivery {
            proposal_id: proposal,
            outcome: DeliveryOutcome::Delivered,
        }),
        Err(SafetyError::InvalidProposalState)
    );
}

proptest! {
    #[test]
    fn arbitrary_confirmation_replays_authorize_at_most_once(
        interactions in prop::collection::vec(0_u64..20, 0..40),
    ) {
        let mut core = SafetyCore::new(100);
        let summary = summary_id("summary");
        let proposal = proposal_id("proposal");
        observe_ready_and_activate(&mut core, &summary, &thread_id("thread"), "reply");
        propose(&mut core, &summary, &proposal, 5, 1);

        let authorizations = interactions
            .into_iter()
            .filter(|interaction| {
                matches!(
                    core.apply(Command::ConfirmResponse {
                        proposal_id: proposal.clone(),
                        interaction: InteractionSequence::new(*interaction),
                        now_ms: 2,
                    }),
                    Ok(Applied::DispatchAuthorized(_))
                )
            })
            .count();

        prop_assert!(authorizations <= 1);
    }
}
