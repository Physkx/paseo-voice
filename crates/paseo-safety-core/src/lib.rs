//! Pure safety-domain module for the future Paseo Voice control plane.
//!
//! It owns provenance, queue, proposal, confirmation, and delivery state. It
//! deliberately has no I/O capability.

#![forbid(unsafe_code)]

use std::{collections::HashMap, fmt::Write as _};

use sha2::{Digest, Sha256};

/// Validation failures for values accepted by the safety-core interface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationError {
    /// An opaque identifier violated the canonical identifier contract.
    InvalidIdentifier,
    /// A response body violated the canonical body contract.
    InvalidResponseBody,
}

/// Canonical opaque identifier used as the storage for strongly typed IDs.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Identifier(String);

impl Identifier {
    /// Validate an opaque identifier without changing its bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError::InvalidIdentifier`] when the value violates
    /// the canonical identifier contract.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || value.trim() != value
            || value.chars().any(|character| character.is_ascii_control())
        {
            return Err(ValidationError::InvalidIdentifier);
        }
        Ok(Self(value))
    }

    /// Borrow the exact identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! typed_identifier {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Identifier);

        impl $name {
            /// Validate and construct the typed identifier.
            ///
            /// # Errors
            ///
            /// Returns [`ValidationError::InvalidIdentifier`] when the value
            /// violates the canonical identifier contract.
            pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
                Identifier::new(value).map(Self)
            }

            /// Borrow the exact identifier text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }
    };
}

typed_identifier!(SummaryId, "Opaque identity of a summary context.");
typed_identifier!(ThreadId, "Opaque identity of a Paseo source thread.");
typed_identifier!(ReplyId, "Opaque identity of an observed Paseo reply.");
typed_identifier!(ProposalId, "Opaque identity of a response proposal.");

/// Validated response text whose exact UTF-8 bytes are immutable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseBody {
    text: String,
    sha256: [u8; 32],
}

impl ResponseBody {
    /// Validate and capture a response body without trimming or normalising it.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError::InvalidResponseBody`] when the text violates
    /// the canonical response-body contract.
    pub fn new(text: impl Into<String>) -> Result<Self, ValidationError> {
        let text = text.into();
        if text.is_empty()
            || text.len() > 65_536
            || text.contains('\0')
            || !text.chars().any(|character| !character.is_whitespace())
        {
            return Err(ValidationError::InvalidResponseBody);
        }
        let sha256 = Sha256::digest(text.as_bytes()).into();
        Ok(Self { text, sha256 })
    }

    /// Borrow the exact response text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Return the lowercase SHA-256 digest of the exact response bytes.
    #[must_use]
    pub fn sha256_hex(&self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.sha256 {
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }
}

/// Trusted sequence assigned to a broker-observed user interaction.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InteractionSequence(u64);

impl InteractionSequence {
    /// Construct a trusted interaction sequence.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the numeric sequence.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Lifecycle of a summary context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SummaryState {
    /// The source reply has been observed but summarisation is incomplete.
    Observed,
    /// Summarisation completed and the context is waiting in the queue.
    Ready,
    /// The context is the sole active response target.
    Active,
    /// A confirmed proposal consumed the context.
    Consumed,
}

/// Terminal result of a write attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    /// The receiver authoritatively acknowledged the write.
    Delivered,
    /// The receiver authoritatively rejected the write before acceptance.
    Rejected,
    /// Acceptance may have occurred but no authoritative receipt was obtained.
    OutcomeUnknown,
}

/// Commands accepted by the single state owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    /// Capture immutable source provenance for a new summary context.
    ObserveReply {
        /// Caller-assigned summary identity.
        summary_id: SummaryId,
        /// Authoritative source thread observed by the broker.
        source_thread_id: ThreadId,
        /// Authoritative source reply observed by the broker.
        source_reply_id: ReplyId,
        /// Injected monotonic observation time.
        observed_at_ms: u64,
    },
    /// Mark an observed summary as complete and queue it.
    MarkSummaryReady {
        /// Summary to queue.
        summary_id: SummaryId,
    },
    /// Activate the first ready summary when no context is active.
    ActivateNext,
    /// Store an exact response proposal for the active summary.
    ProposeResponse {
        /// Caller-assigned proposal identity.
        proposal_id: ProposalId,
        /// Active summary from which the response originated.
        summary_id: SummaryId,
        /// Exact validated response bytes.
        response: ResponseBody,
        /// Trusted interaction that created the proposal.
        interaction: InteractionSequence,
        /// Injected monotonic proposal time.
        now_ms: u64,
    },
    /// Confirm a proposal using evidence from a later interaction.
    ConfirmResponse {
        /// Proposal to confirm.
        proposal_id: ProposalId,
        /// Trusted confirmation interaction.
        interaction: InteractionSequence,
        /// Injected monotonic confirmation time.
        now_ms: u64,
    },
    /// Cancel a pending proposal.
    CancelResponse {
        /// Proposal to cancel.
        proposal_id: ProposalId,
    },
    /// Record the terminal result returned by the write adapter.
    RecordDelivery {
        /// Dispatching proposal whose result is known.
        proposal_id: ProposalId,
        /// Authoritative or conservative adapter result.
        outcome: DeliveryOutcome,
    },
}

/// Successful state transitions returned by [`SafetyCore::apply`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Applied {
    /// A new source reply was captured.
    ReplyObserved,
    /// The source reply was already captured, so state did not change.
    DuplicateReply,
    /// A summary entered the ready queue.
    SummaryReady,
    /// A summary became active.
    SummaryActivated(SummaryId),
    /// A response proposal became pending.
    ResponseProposed,
    /// A pending response proposal was cancelled.
    ResponseCancelled,
    /// Confirmation atomically moved a proposal into dispatching state.
    DispatchAuthorized(DispatchAuthorization),
    /// A terminal delivery result was recorded.
    DeliveryRecorded(DeliveryOutcome),
}

/// Exact immutable values that the caller may supply to the write adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchAuthorization {
    proposal_id: ProposalId,
    destination_thread_id: ThreadId,
    response: ResponseBody,
}

impl DispatchAuthorization {
    /// Return the proposal identity associated with this dispatch.
    #[must_use]
    pub const fn proposal_id(&self) -> &ProposalId {
        &self.proposal_id
    }

    /// Return the destination derived from immutable source provenance.
    #[must_use]
    pub const fn destination_thread_id(&self) -> &ThreadId {
        &self.destination_thread_id
    }

    /// Return the exact stored response body.
    #[must_use]
    pub const fn response(&self) -> &ResponseBody {
        &self.response
    }
}

/// Rejected safety transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SafetyError {
    /// The requested summary does not exist.
    UnknownSummary,
    /// The summary is not in the state required by the command.
    InvalidSummaryState,
    /// Another summary is already active.
    ActiveContextExists,
    /// No ready summary is available to activate.
    NoReadySummary,
    /// The requested proposal does not exist.
    UnknownProposal,
    /// The proposal is not in the state required by the command.
    InvalidProposalState,
    /// A supplied identifier is already bound to different state.
    DuplicateIdentifier,
    /// Confirmation did not originate from a later trusted interaction.
    ConfirmationNotLater,
    /// The proposal expired at or before the confirmation time.
    ProposalExpired,
    /// Monotonic-time addition overflowed.
    TimeOverflow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SummaryRecord {
    source_thread_id: ThreadId,
    source_reply_id: ReplyId,
    observed_at_ms: u64,
    observation_sequence: u64,
    state: SummaryState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProposalState {
    Pending,
    Cancelled,
    Replaced,
    Expired,
    Dispatching,
    Complete(DeliveryOutcome),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProposalRecord {
    summary_id: SummaryId,
    response: ResponseBody,
    created_interaction: InteractionSequence,
    expires_at_ms: u64,
    state: ProposalState,
}

/// Pure, deterministic owner of all safety-critical response state.
#[derive(Debug)]
pub struct SafetyCore {
    proposal_ttl_ms: u64,
    next_observation_sequence: u64,
    summaries: HashMap<SummaryId, SummaryRecord>,
    source_replies: HashMap<(ThreadId, ReplyId), SummaryId>,
    ready_queue: Vec<SummaryId>,
    active_summary: Option<SummaryId>,
    proposals: HashMap<ProposalId, ProposalRecord>,
    pending_by_summary: HashMap<SummaryId, ProposalId>,
}

impl SafetyCore {
    /// Construct an empty state owner with an injected proposal lifetime.
    #[must_use]
    pub fn new(proposal_ttl_ms: u64) -> Self {
        Self {
            proposal_ttl_ms,
            next_observation_sequence: 0,
            summaries: HashMap::new(),
            source_replies: HashMap::new(),
            ready_queue: Vec::new(),
            active_summary: None,
            proposals: HashMap::new(),
            pending_by_summary: HashMap::new(),
        }
    }

    /// Apply one command atomically to the in-memory safety state.
    ///
    /// # Errors
    ///
    /// Returns [`SafetyError`] when a command would violate a state or safety
    /// invariant. Errors do not authorize external I/O.
    pub fn apply(&mut self, command: Command) -> Result<Applied, SafetyError> {
        match command {
            Command::ObserveReply {
                summary_id,
                source_thread_id,
                source_reply_id,
                observed_at_ms,
            } => self.observe_reply(
                summary_id,
                source_thread_id,
                source_reply_id,
                observed_at_ms,
            ),
            Command::MarkSummaryReady { summary_id } => self.mark_summary_ready(&summary_id),
            Command::ActivateNext => self.activate_next(),
            Command::ProposeResponse {
                proposal_id,
                summary_id,
                response,
                interaction,
                now_ms,
            } => self.propose_response(proposal_id, summary_id, response, interaction, now_ms),
            Command::ConfirmResponse {
                proposal_id,
                interaction,
                now_ms,
            } => self.confirm_response(&proposal_id, interaction, now_ms),
            Command::CancelResponse { proposal_id } => self.cancel_response(&proposal_id),
            Command::RecordDelivery {
                proposal_id,
                outcome,
            } => self.record_delivery(&proposal_id, outcome),
        }
    }

    fn observe_reply(
        &mut self,
        summary_id: SummaryId,
        source_thread_id: ThreadId,
        source_reply_id: ReplyId,
        observed_at_ms: u64,
    ) -> Result<Applied, SafetyError> {
        let source_key = (source_thread_id.clone(), source_reply_id.clone());
        if self.source_replies.contains_key(&source_key) {
            return Ok(Applied::DuplicateReply);
        }
        if self.summaries.contains_key(&summary_id) {
            return Err(SafetyError::DuplicateIdentifier);
        }
        let observation_sequence = self.next_observation_sequence;
        self.next_observation_sequence = self
            .next_observation_sequence
            .checked_add(1)
            .ok_or(SafetyError::TimeOverflow)?;
        self.source_replies.insert(source_key, summary_id.clone());
        self.summaries.insert(
            summary_id,
            SummaryRecord {
                source_thread_id,
                source_reply_id,
                observed_at_ms,
                observation_sequence,
                state: SummaryState::Observed,
            },
        );
        Ok(Applied::ReplyObserved)
    }

    fn mark_summary_ready(&mut self, summary_id: &SummaryId) -> Result<Applied, SafetyError> {
        let summary = self
            .summaries
            .get_mut(summary_id)
            .ok_or(SafetyError::UnknownSummary)?;
        if summary.state != SummaryState::Observed {
            return Err(SafetyError::InvalidSummaryState);
        }
        summary.state = SummaryState::Ready;
        self.ready_queue.push(summary_id.clone());
        self.ready_queue.sort_by(|left, right| {
            let left_record = &self.summaries[left];
            let right_record = &self.summaries[right];
            left_record
                .observation_sequence
                .cmp(&right_record.observation_sequence)
                .then_with(|| left.cmp(right))
        });
        Ok(Applied::SummaryReady)
    }

    fn activate_next(&mut self) -> Result<Applied, SafetyError> {
        if self.active_summary.is_some() {
            return Err(SafetyError::ActiveContextExists);
        }
        let summary_id = self
            .ready_queue
            .first()
            .cloned()
            .ok_or(SafetyError::NoReadySummary)?;
        self.ready_queue.remove(0);
        let summary = self
            .summaries
            .get_mut(&summary_id)
            .ok_or(SafetyError::UnknownSummary)?;
        if summary.state != SummaryState::Ready {
            return Err(SafetyError::InvalidSummaryState);
        }
        summary.state = SummaryState::Active;
        self.active_summary = Some(summary_id.clone());
        Ok(Applied::SummaryActivated(summary_id))
    }

    fn propose_response(
        &mut self,
        proposal_id: ProposalId,
        summary_id: SummaryId,
        response: ResponseBody,
        interaction: InteractionSequence,
        now_ms: u64,
    ) -> Result<Applied, SafetyError> {
        if self.proposals.contains_key(&proposal_id) {
            return Err(SafetyError::DuplicateIdentifier);
        }
        if self.active_summary.as_ref() != Some(&summary_id) {
            return Err(SafetyError::InvalidSummaryState);
        }
        let expires_at_ms = now_ms
            .checked_add(self.proposal_ttl_ms)
            .ok_or(SafetyError::TimeOverflow)?;
        if let Some(previous_id) = self
            .pending_by_summary
            .insert(summary_id.clone(), proposal_id.clone())
        {
            let previous = self
                .proposals
                .get_mut(&previous_id)
                .ok_or(SafetyError::UnknownProposal)?;
            if previous.state == ProposalState::Pending {
                previous.state = ProposalState::Replaced;
            }
        }
        self.proposals.insert(
            proposal_id,
            ProposalRecord {
                summary_id,
                response,
                created_interaction: interaction,
                expires_at_ms,
                state: ProposalState::Pending,
            },
        );
        Ok(Applied::ResponseProposed)
    }

    fn confirm_response(
        &mut self,
        proposal_id: &ProposalId,
        interaction: InteractionSequence,
        now_ms: u64,
    ) -> Result<Applied, SafetyError> {
        let proposal = self
            .proposals
            .get_mut(proposal_id)
            .ok_or(SafetyError::UnknownProposal)?;
        if proposal.state != ProposalState::Pending {
            return Err(SafetyError::InvalidProposalState);
        }
        if now_ms >= proposal.expires_at_ms {
            proposal.state = ProposalState::Expired;
            self.pending_by_summary.remove(&proposal.summary_id);
            return Err(SafetyError::ProposalExpired);
        }
        if interaction <= proposal.created_interaction {
            return Err(SafetyError::ConfirmationNotLater);
        }
        if self.active_summary.as_ref() != Some(&proposal.summary_id) {
            return Err(SafetyError::InvalidSummaryState);
        }
        let summary = self
            .summaries
            .get_mut(&proposal.summary_id)
            .ok_or(SafetyError::UnknownSummary)?;
        if summary.state != SummaryState::Active {
            return Err(SafetyError::InvalidSummaryState);
        }
        proposal.state = ProposalState::Dispatching;
        summary.state = SummaryState::Consumed;
        self.active_summary = None;
        self.pending_by_summary.remove(&proposal.summary_id);
        Ok(Applied::DispatchAuthorized(DispatchAuthorization {
            proposal_id: proposal_id.clone(),
            destination_thread_id: summary.source_thread_id.clone(),
            response: proposal.response.clone(),
        }))
    }

    fn cancel_response(&mut self, proposal_id: &ProposalId) -> Result<Applied, SafetyError> {
        let proposal = self
            .proposals
            .get_mut(proposal_id)
            .ok_or(SafetyError::UnknownProposal)?;
        if proposal.state != ProposalState::Pending {
            return Err(SafetyError::InvalidProposalState);
        }
        proposal.state = ProposalState::Cancelled;
        self.pending_by_summary.remove(&proposal.summary_id);
        Ok(Applied::ResponseCancelled)
    }

    fn record_delivery(
        &mut self,
        proposal_id: &ProposalId,
        outcome: DeliveryOutcome,
    ) -> Result<Applied, SafetyError> {
        let proposal = self
            .proposals
            .get_mut(proposal_id)
            .ok_or(SafetyError::UnknownProposal)?;
        if proposal.state != ProposalState::Dispatching {
            return Err(SafetyError::InvalidProposalState);
        }
        proposal.state = ProposalState::Complete(outcome);
        Ok(Applied::DeliveryRecorded(outcome))
    }
}
