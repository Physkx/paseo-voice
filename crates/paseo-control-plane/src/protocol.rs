//! Strict, versioned framing around the pure safety core.

use std::{
    collections::HashMap,
    io::{Read, Write},
};

use paseo_safety_core::{
    Applied, Command, DeliveryOutcome, InteractionSequence, ProposalId, ReplyId, ResponseBody,
    SafetyCore, SafetyError, SummaryId, ThreadId, ValidationError,
};
use serde::{Deserialize, Deserializer, Serialize, de::MapAccess, de::Visitor};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Current local protocol version.
pub const PROTOCOL_VERSION: u16 = 1;
/// Maximum JSON payload accepted in one frame.
pub const MAX_FRAME_BYTES: usize = 131_072;

/// Failures that prevent a request from reaching the state owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    /// The four-byte length prefix is missing.
    MissingLength,
    /// The declared payload exceeds the protocol limit.
    FrameTooLarge,
    /// The frame contains fewer bytes than its declared length.
    TruncatedFrame,
    /// Bytes follow the single declared frame.
    TrailingData,
    /// The payload is not a strict supported request.
    InvalidRequest,
    /// Encoding a response failed.
    EncodeResponse,
}

/// Failure while serving framed requests on an already-open local stream.
#[derive(Debug)]
pub enum ServeError {
    /// Reading or writing the local stream failed.
    Io(std::io::Error),
    /// A malformed request failed the protocol boundary.
    Protocol(ProtocolError),
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "local stream failed: {error}"),
            Self::Protocol(error) => write!(formatter, "protocol failed: {error:?}"),
        }
    }
}

impl std::error::Error for ServeError {}

impl From<std::io::Error> for ServeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ProtocolError> for ServeError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

/// Supported protocol operations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    /// Report protocol health without changing state.
    Health,
    /// Observe immutable source provenance.
    ObserveReply {
        /// Summary identity.
        summary_id: String,
        /// Source thread identity.
        source_thread_id: String,
        /// Source reply identity.
        source_reply_id: String,
        /// Injected monotonic observation time.
        observed_at_ms: u64,
    },
    /// Queue a completed summary.
    MarkSummaryReady {
        /// Summary identity.
        summary_id: String,
    },
    /// Activate the oldest ready summary.
    ActivateNext,
    /// Defer the active context and invalidate its proposal.
    DeferActive,
    /// Store an exact response for the active summary.
    ProposeResponse {
        /// Proposal identity.
        proposal_id: String,
        /// Summary identity.
        summary_id: String,
        /// Exact response text.
        response: String,
        /// Trusted interaction sequence.
        interaction: u64,
        /// Injected monotonic time.
        now_ms: u64,
    },
    /// Confirm a pending response.
    ConfirmResponse {
        /// Proposal identity.
        proposal_id: String,
        /// Trusted later interaction sequence.
        interaction: u64,
        /// Injected monotonic time.
        now_ms: u64,
    },
    /// Cancel a pending response.
    CancelResponse {
        /// Proposal identity.
        proposal_id: String,
    },
    /// Record the terminal delivery result.
    RecordDelivery {
        /// Proposal identity.
        proposal_id: String,
        /// Adapter result.
        outcome: WireDeliveryOutcome,
    },
}

/// Delivery result accepted on the protocol boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WireDeliveryOutcome {
    /// Authoritatively delivered.
    Delivered,
    /// Authoritatively rejected.
    Rejected,
    /// Acceptance cannot be determined.
    OutcomeUnknown,
}

impl From<WireDeliveryOutcome> for DeliveryOutcome {
    fn from(value: WireDeliveryOutcome) -> Self {
        match value {
            WireDeliveryOutcome::Delivered => Self::Delivered,
            WireDeliveryOutcome::Rejected => Self::Rejected,
            WireDeliveryOutcome::OutcomeUnknown => Self::OutcomeUnknown,
        }
    }
}

#[derive(Debug)]
struct RequestEnvelope {
    version: u16,
    request_id: String,
    operation: Operation,
}

#[derive(Debug)]
struct UniqueObject(serde_json::Map<String, Value>);

impl<'de> Deserialize<'de> for UniqueObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UniqueObjectVisitor;

        impl<'de> Visitor<'de> for UniqueObjectVisitor {
            type Value = UniqueObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object with unique fields")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = serde_json::Map::new();
                while let Some((key, value)) = access.next_entry::<String, Value>()? {
                    if fields.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate field"));
                    }
                }
                Ok(UniqueObject(fields))
            }
        }

        deserializer.deserialize_map(UniqueObjectVisitor)
    }
}

#[derive(Debug, Serialize)]
struct ResponseEnvelope<'a> {
    version: u16,
    request_id: &'a str,
    #[serde(flatten)]
    outcome: ResponseOutcome,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ResponseOutcome {
    Ok { result: Value },
    Error { error: ErrorBody },
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
}

#[derive(Debug)]
struct ReplayEntry {
    request_digest: [u8; 32],
    framed_response: Vec<u8>,
}

/// Single owner of protocol replay state and the safety state machine.
#[derive(Debug)]
pub struct ProtocolServer {
    core: SafetyCore,
    replay: HashMap<String, ReplayEntry>,
}

impl ProtocolServer {
    /// Construct an empty protocol service.
    #[must_use]
    pub fn new(proposal_ttl_ms: u64) -> Self {
        Self {
            core: SafetyCore::new(proposal_ttl_ms),
            replay: HashMap::new(),
        }
    }

    /// Decode and apply exactly one length-delimited request.
    ///
    /// Completed semantic requests, including rejected transitions, are
    /// replayed byte-for-byte when the request ID and payload are identical.
    ///
    /// # Errors
    ///
    /// Returns `ProtocolError` for malformed framing, invalid JSON, unknown
    /// fields or variants, unsupported versions, and response encoding failure.
    pub fn handle_frame(&mut self, frame: &[u8]) -> Result<Vec<u8>, ProtocolError> {
        let payload = decode_frame(frame)?;
        let request = decode_request(payload)?;
        if request.version != PROTOCOL_VERSION {
            return Err(ProtocolError::InvalidRequest);
        }
        let request_id = paseo_safety_core::Identifier::new(request.request_id)
            .map_err(|_| ProtocolError::InvalidRequest)?;
        let digest: [u8; 32] = Sha256::digest(payload).into();

        if let Some(previous) = self.replay.get(request_id.as_str()) {
            if previous.request_digest == digest {
                return Ok(previous.framed_response.clone());
            }
            return encode_response(&ResponseEnvelope {
                version: PROTOCOL_VERSION,
                request_id: request_id.as_str(),
                outcome: ResponseOutcome::Error {
                    error: ErrorBody {
                        code: "request_id_conflict",
                    },
                },
            });
        }

        let outcome = self.apply(request.operation);
        let framed_response = encode_response(&ResponseEnvelope {
            version: PROTOCOL_VERSION,
            request_id: request_id.as_str(),
            outcome,
        })?;
        self.replay.insert(
            request_id.as_str().to_owned(),
            ReplayEntry {
                request_digest: digest,
                framed_response: framed_response.clone(),
            },
        );
        Ok(framed_response)
    }

    fn apply(&mut self, operation: Operation) -> ResponseOutcome {
        if operation == Operation::Health {
            return ResponseOutcome::Ok {
                result: json!({
                    "status": "healthy",
                    "protocol_version": PROTOCOL_VERSION,
                }),
            };
        }
        match operation_to_command(operation)
            .and_then(|command| self.core.apply(command).map_err(CommandError::Safety))
        {
            Ok(applied) => ResponseOutcome::Ok {
                result: applied_to_value(applied),
            },
            Err(error) => ResponseOutcome::Error {
                error: ErrorBody { code: error.code() },
            },
        }
    }
}

fn decode_request(payload: &[u8]) -> Result<RequestEnvelope, ProtocolError> {
    let UniqueObject(mut fields) =
        serde_json::from_slice(payload).map_err(|_| ProtocolError::InvalidRequest)?;
    let version = fields
        .remove("version")
        .and_then(|value| value.as_u64())
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(ProtocolError::InvalidRequest)?;
    let request_id = fields
        .remove("request_id")
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or(ProtocolError::InvalidRequest)?;
    if !operation_fields_are_exact(&fields) {
        return Err(ProtocolError::InvalidRequest);
    }
    let operation =
        serde_json::from_value(Value::Object(fields)).map_err(|_| ProtocolError::InvalidRequest)?;
    Ok(RequestEnvelope {
        version,
        request_id,
        operation,
    })
}

fn operation_fields_are_exact(fields: &serde_json::Map<String, Value>) -> bool {
    let Some(operation) = fields.get("op").and_then(Value::as_str) else {
        return false;
    };
    let allowed: &[&str] = match operation {
        "health" | "activate_next" | "defer_active" => &["op"],
        "mark_summary_ready" => &["op", "summary_id"],
        "cancel_response" => &["op", "proposal_id"],
        "observe_reply" => &[
            "op",
            "summary_id",
            "source_thread_id",
            "source_reply_id",
            "observed_at_ms",
        ],
        "propose_response" => &[
            "op",
            "proposal_id",
            "summary_id",
            "response",
            "interaction",
            "now_ms",
        ],
        "confirm_response" => &["op", "proposal_id", "interaction", "now_ms"],
        "record_delivery" => &["op", "proposal_id", "outcome"],
        _ => return false,
    };
    fields.len() == allowed.len() && fields.keys().all(|key| allowed.contains(&key.as_str()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandError {
    Validation(ValidationError),
    Safety(SafetyError),
}

impl CommandError {
    const fn code(self) -> &'static str {
        match self {
            Self::Validation(ValidationError::InvalidIdentifier) => "invalid_identifier",
            Self::Validation(ValidationError::InvalidResponseBody) => "invalid_response_body",
            Self::Safety(SafetyError::UnknownSummary) => "unknown_summary",
            Self::Safety(SafetyError::InvalidSummaryState) => "invalid_summary_state",
            Self::Safety(SafetyError::ActiveContextExists) => "active_context_exists",
            Self::Safety(SafetyError::NoReadySummary) => "no_ready_summary",
            Self::Safety(SafetyError::UnknownProposal) => "unknown_proposal",
            Self::Safety(SafetyError::InvalidProposalState) => "invalid_proposal_state",
            Self::Safety(SafetyError::DuplicateIdentifier) => "duplicate_identifier",
            Self::Safety(SafetyError::ConfirmationNotLater) => "confirmation_not_later",
            Self::Safety(SafetyError::ProposalExpired) => "proposal_expired",
            Self::Safety(SafetyError::TimeOverflow) => "time_overflow",
        }
    }
}

fn operation_to_command(operation: Operation) -> Result<Command, CommandError> {
    let validation = CommandError::Validation;
    match operation {
        Operation::Health => unreachable!("health is handled before command conversion"),
        Operation::ObserveReply {
            summary_id,
            source_thread_id,
            source_reply_id,
            observed_at_ms,
        } => Ok(Command::ObserveReply {
            summary_id: SummaryId::new(summary_id).map_err(validation)?,
            source_thread_id: ThreadId::new(source_thread_id).map_err(validation)?,
            source_reply_id: ReplyId::new(source_reply_id).map_err(validation)?,
            observed_at_ms,
        }),
        Operation::MarkSummaryReady { summary_id } => Ok(Command::MarkSummaryReady {
            summary_id: SummaryId::new(summary_id).map_err(validation)?,
        }),
        Operation::ActivateNext => Ok(Command::ActivateNext),
        Operation::DeferActive => Ok(Command::DeferActive),
        Operation::ProposeResponse {
            proposal_id,
            summary_id,
            response,
            interaction,
            now_ms,
        } => Ok(Command::ProposeResponse {
            proposal_id: ProposalId::new(proposal_id).map_err(validation)?,
            summary_id: SummaryId::new(summary_id).map_err(validation)?,
            response: ResponseBody::new(response).map_err(validation)?,
            interaction: InteractionSequence::new(interaction),
            now_ms,
        }),
        Operation::ConfirmResponse {
            proposal_id,
            interaction,
            now_ms,
        } => Ok(Command::ConfirmResponse {
            proposal_id: ProposalId::new(proposal_id).map_err(validation)?,
            interaction: InteractionSequence::new(interaction),
            now_ms,
        }),
        Operation::CancelResponse { proposal_id } => Ok(Command::CancelResponse {
            proposal_id: ProposalId::new(proposal_id).map_err(validation)?,
        }),
        Operation::RecordDelivery {
            proposal_id,
            outcome,
        } => Ok(Command::RecordDelivery {
            proposal_id: ProposalId::new(proposal_id).map_err(validation)?,
            outcome: outcome.into(),
        }),
    }
}

fn applied_to_value(applied: Applied) -> Value {
    match applied {
        Applied::ReplyObserved => json!({ "status": "reply_observed" }),
        Applied::DuplicateReply => json!({ "status": "duplicate_reply" }),
        Applied::SummaryReady => json!({ "status": "summary_ready" }),
        Applied::SummaryActivated(summary_id) => {
            json!({ "status": "summary_activated", "summary_id": summary_id.as_str() })
        }
        Applied::SummaryDeferred => json!({ "status": "summary_deferred" }),
        Applied::ResponseProposed => json!({ "status": "response_proposed" }),
        Applied::ResponseCancelled => json!({ "status": "response_cancelled" }),
        Applied::DispatchAuthorized(authorization) => json!({
            "status": "dispatch_authorized",
            "proposal_id": authorization.proposal_id().as_str(),
            "destination_thread_id": authorization.destination_thread_id().as_str(),
            "response": authorization.response().as_str(),
            "response_sha256": authorization.response().sha256_hex(),
        }),
        Applied::DeliveryRecorded(outcome) => json!({
            "status": "delivery_recorded",
            "outcome": delivery_outcome_name(outcome),
        }),
    }
}

const fn delivery_outcome_name(outcome: DeliveryOutcome) -> &'static str {
    match outcome {
        DeliveryOutcome::Delivered => "delivered",
        DeliveryOutcome::Rejected => "rejected",
        DeliveryOutcome::OutcomeUnknown => "outcome_unknown",
    }
}

fn decode_frame(frame: &[u8]) -> Result<&[u8], ProtocolError> {
    let prefix: [u8; 4] = frame
        .get(..4)
        .ok_or(ProtocolError::MissingLength)?
        .try_into()
        .map_err(|_| ProtocolError::MissingLength)?;
    let length =
        usize::try_from(u32::from_be_bytes(prefix)).map_err(|_| ProtocolError::FrameTooLarge)?;
    if length > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let expected = 4_usize
        .checked_add(length)
        .ok_or(ProtocolError::FrameTooLarge)?;
    match frame.len().cmp(&expected) {
        std::cmp::Ordering::Less => Err(ProtocolError::TruncatedFrame),
        std::cmp::Ordering::Greater => Err(ProtocolError::TrailingData),
        std::cmp::Ordering::Equal => Ok(&frame[4..]),
    }
}

/// Add the protocol length prefix to one JSON payload.
///
/// # Errors
///
/// Returns `FrameTooLarge` when the payload exceeds the configured bound or
/// cannot fit in the four-byte prefix.
pub fn frame_payload(payload: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let length = u32::try_from(payload.len()).map_err(|_| ProtocolError::FrameTooLarge)?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Serve requests sequentially until the input reaches a clean EOF.
///
/// This function does not open a socket, authenticate a remote peer, retry a
/// request, or own any credential. The parent process owns the already-open
/// streams and child lifecycle.
///
/// # Errors
///
/// Returns [`ServeError`] on partial input, malformed requests, or stream I/O
/// failure. It exits cleanly only when EOF occurs between frames.
pub fn serve_stdio<R: Read, W: Write>(
    mut input: R,
    mut output: W,
    proposal_ttl_ms: u64,
) -> Result<(), ServeError> {
    let mut server = ProtocolServer::new(proposal_ttl_ms);
    loop {
        let mut prefix = [0_u8; 4];
        let read = input.read(&mut prefix)?;
        if read == 0 {
            return Ok(());
        }
        input.read_exact(&mut prefix[read..])?;
        let length = usize::try_from(u32::from_be_bytes(prefix))
            .map_err(|_| ProtocolError::FrameTooLarge)?;
        if length > MAX_FRAME_BYTES {
            return Err(ProtocolError::FrameTooLarge.into());
        }
        let mut frame = Vec::with_capacity(4 + length);
        frame.extend_from_slice(&prefix);
        frame.resize(4 + length, 0);
        input.read_exact(&mut frame[4..])?;
        let response = server.handle_frame(&frame)?;
        output.write_all(&response)?;
        output.flush()?;
    }
}

fn encode_response(response: &ResponseEnvelope<'_>) -> Result<Vec<u8>, ProtocolError> {
    let payload = serde_json::to_vec(&response).map_err(|_| ProtocolError::EncodeResponse)?;
    frame_payload(&payload)
}
