use paseo_safety_core::{Observation, ReplyId, SafetyError, SummaryId, SummaryQueue, ThreadId};

fn summary_id(value: &str) -> SummaryId {
    SummaryId::new(value).expect("valid summary ID")
}

fn thread_id(value: &str) -> ThreadId {
    ThreadId::new(value).expect("valid thread ID")
}

fn reply_id(value: &str) -> ReplyId {
    ReplyId::new(value).expect("valid reply ID")
}

fn observe(
    queue: &mut SummaryQueue,
    summary: &str,
    thread: &str,
    reply: &str,
    at_ms: u64,
) -> Observation {
    queue
        .observe_reply(
            summary_id(summary),
            thread_id(thread),
            reply_id(reply),
            at_ms,
        )
        .expect("observe reply")
}

#[test]
fn a_new_reply_is_observed_and_a_repeated_source_reply_is_a_duplicate() {
    let mut queue = SummaryQueue::new();
    assert_eq!(
        observe(&mut queue, "summary-a", "thread-a", "reply-a", 10),
        Observation::Observed
    );

    // Same (thread, reply) with a different summary ID is a content-free duplicate.
    assert_eq!(
        queue.observe_reply(
            summary_id("summary-different"),
            thread_id("thread-a"),
            reply_id("reply-a"),
            11,
        ),
        Ok(Observation::Duplicate)
    );
}

#[test]
fn reusing_a_summary_id_for_different_provenance_is_rejected() {
    let mut queue = SummaryQueue::new();
    observe(&mut queue, "summary-a", "thread-a", "reply-a", 10);
    assert_eq!(
        queue.observe_reply(
            summary_id("summary-a"),
            thread_id("thread-b"),
            reply_id("reply-b"),
            11,
        ),
        Err(SafetyError::DuplicateIdentifier)
    );
}

#[test]
fn mark_ready_requires_an_observed_summary() {
    let mut queue = SummaryQueue::new();
    assert_eq!(
        queue.mark_ready(&summary_id("missing")),
        Err(SafetyError::UnknownSummary)
    );

    observe(&mut queue, "summary-a", "thread-a", "reply-a", 10);
    assert_eq!(queue.mark_ready(&summary_id("summary-a")), Ok(()));
    // A second mark is no longer in the Observed state.
    assert_eq!(
        queue.mark_ready(&summary_id("summary-a")),
        Err(SafetyError::InvalidSummaryState)
    );
}

#[test]
fn ready_summaries_activate_in_observation_order_regardless_of_ready_order() {
    let mut queue = SummaryQueue::new();
    // Observe first, then second, so first has the earlier observation sequence.
    observe(
        &mut queue,
        "summary-first",
        "thread-first",
        "reply-first",
        50,
    );
    observe(
        &mut queue,
        "summary-second",
        "thread-second",
        "reply-second",
        1,
    );

    // Mark them ready in reverse order; activation must still follow observation order.
    queue
        .mark_ready(&summary_id("summary-second"))
        .expect("ready second");
    queue
        .mark_ready(&summary_id("summary-first"))
        .expect("ready first");
    assert_eq!(queue.ready_len(), 2);

    assert_eq!(queue.activate_next(), Ok(summary_id("summary-first")));
    assert_eq!(queue.ready_len(), 1);
    assert_eq!(queue.active_summary(), Some(&summary_id("summary-first")));

    // Only one context is active at a time.
    assert_eq!(queue.activate_next(), Err(SafetyError::ActiveContextExists));
}

#[test]
fn activate_next_without_a_ready_summary_reports_none_ready() {
    let mut queue = SummaryQueue::new();
    assert_eq!(queue.activate_next(), Err(SafetyError::NoReadySummary));
}

#[test]
fn defer_active_frees_the_slot_for_the_next_ready_summary() {
    let mut queue = SummaryQueue::new();
    observe(&mut queue, "summary-a", "thread-a", "reply-a", 1);
    observe(&mut queue, "summary-b", "thread-b", "reply-b", 2);
    queue.mark_ready(&summary_id("summary-a")).expect("ready a");
    queue.mark_ready(&summary_id("summary-b")).expect("ready b");
    queue.activate_next().expect("activate a");

    assert_eq!(queue.defer_active(), Ok(summary_id("summary-a")));
    assert_eq!(queue.active_summary(), None);
    // The next ready summary can now be activated.
    assert_eq!(queue.activate_next(), Ok(summary_id("summary-b")));
}

#[test]
fn defer_without_an_active_context_is_rejected() {
    let mut queue = SummaryQueue::new();
    assert_eq!(queue.defer_active(), Err(SafetyError::UnknownSummary));
}

#[test]
fn consume_active_returns_the_immutable_source_thread_and_frees_the_slot() {
    let mut queue = SummaryQueue::new();
    observe(&mut queue, "summary-a", "thread-a", "reply-a", 1);
    queue.mark_ready(&summary_id("summary-a")).expect("ready a");
    queue.activate_next().expect("activate a");

    assert_eq!(
        queue.consume_active(&summary_id("summary-a")),
        Ok(thread_id("thread-a"))
    );
    assert_eq!(queue.active_summary(), None);
    // A consumed context cannot be consumed again.
    assert_eq!(
        queue.consume_active(&summary_id("summary-a")),
        Err(SafetyError::InvalidSummaryState)
    );
}

#[test]
fn settle_releases_active_and_ready_selection_but_keeps_dedup() {
    let mut queue = SummaryQueue::new();
    observe(
        &mut queue,
        "summary-active",
        "thread-active",
        "reply-active",
        1,
    );
    observe(
        &mut queue,
        "summary-ready",
        "thread-ready",
        "reply-ready",
        2,
    );
    queue
        .mark_ready(&summary_id("summary-active"))
        .expect("ready active");
    queue.activate_next().expect("activate");
    queue
        .mark_ready(&summary_id("summary-ready"))
        .expect("ready the queued one");
    assert_eq!(queue.active_summary(), Some(&summary_id("summary-active")));
    assert_eq!(queue.ready_len(), 1);

    queue.settle();

    // No active context and nothing queued after settling.
    assert_eq!(queue.active_summary(), None);
    assert_eq!(queue.ready_len(), 0);
    // The already-observed replies stay deduplicated (announce-once survives).
    assert_eq!(
        queue.observe_reply(
            summary_id("summary-active-again"),
            thread_id("thread-active"),
            reply_id("reply-active"),
            3,
        ),
        Ok(Observation::Duplicate)
    );
    assert_eq!(
        queue.observe_reply(
            summary_id("summary-ready-again"),
            thread_id("thread-ready"),
            reply_id("reply-ready"),
            4,
        ),
        Ok(Observation::Duplicate)
    );
}

#[test]
fn a_settled_queue_activates_a_freshly_read_reply_not_a_carried_over_one() {
    let mut queue = SummaryQueue::new();
    // A prior connection observed and queued a summary, then the queue settled.
    observe(&mut queue, "summary-old", "thread-old", "reply-old", 1);
    queue
        .mark_ready(&summary_id("summary-old"))
        .expect("ready old");
    queue.settle();

    // A fresh connection reads a brand new reply.
    observe(&mut queue, "summary-new", "thread-new", "reply-new", 2);
    queue
        .mark_ready(&summary_id("summary-new"))
        .expect("ready new");

    // Activation selects the freshly read reply, never the settled carry-over,
    // even though the old summary has the earlier observation sequence.
    assert_eq!(queue.activate_next(), Ok(summary_id("summary-new")));
    assert_eq!(queue.active_summary(), Some(&summary_id("summary-new")));
}

#[test]
fn consume_rejects_a_summary_that_is_not_the_active_context() {
    let mut queue = SummaryQueue::new();
    observe(&mut queue, "summary-a", "thread-a", "reply-a", 1);
    observe(&mut queue, "summary-b", "thread-b", "reply-b", 2);
    queue.mark_ready(&summary_id("summary-a")).expect("ready a");
    queue.activate_next().expect("activate a");

    assert_eq!(
        queue.consume_active(&summary_id("summary-b")),
        Err(SafetyError::InvalidSummaryState)
    );
    // The real active context is untouched and still consumable.
    assert_eq!(
        queue.consume_active(&summary_id("summary-a")),
        Ok(thread_id("thread-a"))
    );
}
