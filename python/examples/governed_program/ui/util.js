export function esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// "gVisor" unless the launcher reports another runtime, which is then shown as it is.
export function runtimeName(runtime) {
  if (!runtime || /^gvisor|runsc/i.test(runtime)) return "gVisor";
  return runtime.split(" (")[0];
}

export function clock(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString("en-GB", { hour12: false });
}
