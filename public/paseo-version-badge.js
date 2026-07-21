(() => {
  const id = "paseo-version-badge";
  if (document.getElementById(id)) return;
  const badge = document.createElement("aside");
  badge.id = id;
  badge.textContent = "v0.4.1";
  Object.assign(badge.style, {
    position: "fixed",
    right: "12px",
    bottom: "12px",
    zIndex: "2147483647",
    padding: "4px 7px",
    borderRadius: "999px",
    background: "rgba(15, 23, 42, .82)",
    color: "#fff",
    font: "500 12px/1 ui-monospace, SFMono-Regular, Menlo, monospace",
    pointerEvents: "none",
  });
  document.body.append(badge);
})();
