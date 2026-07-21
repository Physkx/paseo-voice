import test from "node:test";
import assert from "node:assert/strict";

import { isInteractiveTarget, validateShortcuts } from "../public/shortcut-config.js";

test("accepts distinct allowlisted shortcuts", () => {
  const result = validateShortcuts({ hold: "Space", toggle: "KeyD", cancel: "Escape" });
  assert.equal(result.valid, true);
});

test("rejects conflicts and unknown key codes", () => {
  assert.equal(
    validateShortcuts({ hold: "Space", toggle: "Space", cancel: "Escape" }).valid,
    false,
  );
  assert.equal(validateShortcuts({ hold: "F12", toggle: "KeyD", cancel: "Escape" }).valid, false);
});

test("guards native interactive and editable targets from page shortcuts", () => {
  for (const tagName of ["BUTTON", "INPUT", "SELECT", "TEXTAREA", "SUMMARY"]) {
    assert.equal(isInteractiveTarget({ tagName }), true);
  }
  assert.equal(isInteractiveTarget({ tagName: "DIV", isContentEditable: true }), true);
  assert.equal(isInteractiveTarget({ tagName: "DIV" }), false);
});
