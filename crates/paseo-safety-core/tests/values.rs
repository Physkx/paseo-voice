use paseo_safety_core::{Identifier, ResponseBody, ValidationError};

#[test]
fn identifiers_accept_opaque_values_and_reject_routing_ambiguity() {
    let id = Identifier::new("reply_123").expect("valid identifier");
    assert_eq!(id.as_str(), "reply_123");

    for invalid in ["", " leading", "trailing ", "line\nbreak", "nul\0byte"] {
        assert_eq!(
            Identifier::new(invalid),
            Err(ValidationError::InvalidIdentifier)
        );
    }
    assert_eq!(
        Identifier::new("x".repeat(129)),
        Err(ValidationError::InvalidIdentifier),
    );
}

#[test]
fn response_body_preserves_exact_utf8_and_has_a_stable_digest() {
    let text = "  ngā mihi\r\nrun tests  ";
    let body = ResponseBody::new(text).expect("valid response body");

    assert_eq!(body.as_str(), text);
    assert_eq!(
        body.sha256_hex(),
        "83bece510c0b683ad5737fa9967358482595690a311c2c61075e5ab0266d75c4",
    );

    for invalid in ["", " \n\t ", "contains\0nul"] {
        assert_eq!(
            ResponseBody::new(invalid),
            Err(ValidationError::InvalidResponseBody)
        );
    }
    assert_eq!(
        ResponseBody::new("x".repeat(65_537)),
        Err(ValidationError::InvalidResponseBody),
    );
}
