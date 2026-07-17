use paseo_control_plane::protocol::{
    MAX_FRAME_BYTES, PROTOCOL_VERSION, ProtocolError, ProtocolServer, frame_payload,
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
struct Fixtures {
    version: u16,
    valid: Vec<ValidFixture>,
    invalid: Vec<InvalidFixture>,
}

#[derive(Debug, Deserialize)]
struct ValidFixture {
    name: String,
    request: Value,
    expected_status: String,
}

#[derive(Debug, Deserialize)]
struct InvalidFixture {
    name: String,
    request: Value,
}

fn fixtures() -> Fixtures {
    serde_json::from_str(include_str!("../../../docs/RUST_PROTOCOL_FIXTURES.json"))
        .expect("valid shared fixtures")
}

fn request(value: &Value) -> Vec<u8> {
    frame_payload(&serde_json::to_vec(value).expect("encode request")).expect("frame request")
}

fn response(frame: &[u8]) -> Value {
    let length = u32::from_be_bytes(frame[..4].try_into().expect("length prefix")) as usize;
    assert_eq!(frame.len(), length + 4);
    serde_json::from_slice(&frame[4..]).expect("JSON response")
}

#[test]
fn shared_contract_fixtures_are_enforced() {
    let fixtures = fixtures();
    assert_eq!(fixtures.version, PROTOCOL_VERSION);
    for fixture in fixtures.valid {
        let mut server = ProtocolServer::new(120_000);
        let reply = server
            .handle_frame(&request(&fixture.request))
            .unwrap_or_else(|error| panic!("{} failed: {error:?}", fixture.name));
        assert_eq!(
            response(&reply)["result"]["status"],
            fixture.expected_status,
            "{}",
            fixture.name
        );
    }
    for fixture in fixtures.invalid {
        let mut server = ProtocolServer::new(120_000);
        assert_eq!(
            server.handle_frame(&request(&fixture.request)),
            Err(ProtocolError::InvalidRequest),
            "{}",
            fixture.name
        );
    }
}

#[test]
fn identical_request_replays_exact_response_and_conflicting_reuse_is_rejected() {
    let mut server = ProtocolServer::new(120_000);
    let first = request(&json!({
        "version": 1,
        "request_id": "request-a",
        "op": "health",
    }));
    let original = server.handle_frame(&first).expect("first response");
    assert_eq!(
        server.handle_frame(&first),
        Ok(original),
        "identical bytes replay the original bytes"
    );

    let conflict = server
        .handle_frame(&request(&json!({
            "version": 1,
            "request_id": "request-a",
            "op": "activate_next",
        })))
        .expect("structured conflict response");
    assert_eq!(response(&conflict)["error"]["code"], "request_id_conflict");
}

#[test]
fn malformed_truncated_oversized_duplicate_and_trailing_frames_fail_closed() {
    let mut server = ProtocolServer::new(120_000);
    assert_eq!(server.handle_frame(&[]), Err(ProtocolError::MissingLength));
    assert_eq!(
        server.handle_frame(&[0, 0, 0, 10, b'{']),
        Err(ProtocolError::TruncatedFrame)
    );
    let oversized_length = u32::try_from(MAX_FRAME_BYTES + 1)
        .expect("bounded test size")
        .to_be_bytes();
    assert_eq!(
        server.handle_frame(&oversized_length),
        Err(ProtocolError::FrameTooLarge)
    );
    let mut trailing = request(&json!({
        "version": 1,
        "request_id": "trailing",
        "op": "health",
    }));
    trailing.push(0);
    assert_eq!(
        server.handle_frame(&trailing),
        Err(ProtocolError::TrailingData)
    );
    let duplicate_field = br#"{"version":1,"version":1,"request_id":"duplicate","op":"health"}"#;
    assert_eq!(
        server.handle_frame(&frame_payload(duplicate_field).expect("frame duplicate")),
        Err(ProtocolError::InvalidRequest)
    );
}

#[test]
fn protocol_never_accepts_a_destination_during_proposal_or_confirmation() {
    let mut server = ProtocolServer::new(120_000);
    let operations = [
        json!({
            "version": 1, "request_id": "observe", "op": "observe_reply",
            "summary_id": "summary-a", "source_thread_id": "thread-a",
            "source_reply_id": "reply-a", "observed_at_ms": 1
        }),
        json!({
            "version": 1, "request_id": "ready", "op": "mark_summary_ready",
            "summary_id": "summary-a"
        }),
        json!({"version": 1, "request_id": "active", "op": "activate_next"}),
        json!({
            "version": 1, "request_id": "propose", "op": "propose_response",
            "proposal_id": "proposal-a", "summary_id": "summary-a",
            "response": "exact response", "interaction": 5, "now_ms": 10
        }),
    ];
    for operation in operations {
        server
            .handle_frame(&request(&operation))
            .expect("operation accepted");
    }
    let confirmed = server
        .handle_frame(&request(&json!({
            "version": 1, "request_id": "confirm", "op": "confirm_response",
            "proposal_id": "proposal-a", "interaction": 6, "now_ms": 11
        })))
        .expect("confirmation response");
    assert_eq!(
        response(&confirmed)["result"]["destination_thread_id"],
        "thread-a"
    );

    let substituted = json!({
        "version": 1, "request_id": "substitute", "op": "confirm_response",
        "proposal_id": "proposal-a", "interaction": 7, "now_ms": 12,
        "destination_thread_id": "thread-b"
    });
    assert_eq!(
        server.handle_frame(&request(&substituted)),
        Err(ProtocolError::InvalidRequest)
    );
}
