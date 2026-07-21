const allowedCodes = new Set(["Space", "KeyD", "KeyR", "KeyC", "Escape"]);

/** Validate page-scoped shortcut codes and reject ambiguous bindings. */
export function validateShortcuts(shortcuts) {
  const entries = Object.entries(shortcuts);
  for (const [name, code] of entries) {
    if (!allowedCodes.has(code)) return { valid: false, message: `${name} shortcut is invalid.` };
  }
  if (new Set(entries.map(([, code]) => code)).size !== entries.length) {
    return { valid: false, message: "Each shortcut must use a different key." };
  }
  return { valid: true, value: Object.freeze({ ...shortcuts }) };
}

/** Page shortcuts never intercept native interactive or editable controls. */
export function isInteractiveTarget(target) {
  const tagName = target?.tagName?.toLowerCase();
  return (
    target?.isContentEditable === true ||
    ["button", "input", "select", "summary", "textarea"].includes(tagName)
  );
}
