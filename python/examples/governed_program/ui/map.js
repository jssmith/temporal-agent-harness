// The system map: three zones (untrusted sandbox, boundary, trusted side), the links between their
// parts lit by the current beat, and one row per call leaving the sandbox showing where it stopped.
import { esc, runtimeName } from "./util.js";

const W = 760;
const X_WALL = 232;
const X_PROXY = 380;
const X_TRUSTED = 636;
const LANE_TOP = 256;
const LANE_GAP = 15;
const MAX_LANES = 9;

function node(x, y, w, h, title, sub, { active = false, dim = false } = {}) {
  const cls = ["node", active ? "active" : "", dim ? "dim" : ""].join(" ");
  const ty = sub ? y + 15 : y + h / 2 + 4;
  return `<g class="${cls}"><rect x="${x}" y="${y}" width="${w}" height="${h}" rx="5"/>
    <text class="t" x="${x + 10}" y="${ty}">${esc(title)}</text>
    ${sub ? `<text class="sub" x="${x + 10}" y="${y + 28}">${esc(sub)}</text>` : ""}</g>`;
}

function cross(x, y, cls) {
  return `<path class="${cls}" d="M${x - 4} ${y - 4} L${x + 4} ${y + 4} M${x + 4} ${y - 4} L${x - 4} ${y + 4}" stroke-width="2" fill="none"/>`;
}

function truncate(s, n) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

function lane(l, y) {
  const tone = l.kind === "allow" ? "green" : l.kind === "pending" ? "yellow" : "red";
  const sw = l.fresh ? 2 : 1.2;
  const verdict = (x) => `<text class="verdict" x="${x}" y="${y + 3.5}" fill="var(--${tone})">${esc(l.verdict)}</text>`;
  const line = (x1, x2, width = sw, extra = "") =>
    `<line x1="${x1}" y1="${y}" x2="${x2}" y2="${y}" stroke="var(--${tone})" stroke-width="${width}" ${extra}/>`;
  let body = "";
  if (l.kind === "allow") {
    body = line(X_WALL, X_TRUSTED) +
      `<circle class="ring" cx="${X_PROXY}" cy="${y}" r="2.5" stroke="var(--${tone})"/>` +
      `<circle cx="${X_TRUSTED}" cy="${y}" r="4" fill="var(--${tone})"/>` + verdict(X_TRUSTED + 10);
  } else if (l.kind === "pending") {
    body = line(X_WALL, X_PROXY - 6) +
      `<circle class="ring pulse" cx="${X_PROXY}" cy="${y}" r="4.5" stroke="var(--${tone})" stroke-width="1.6"/>` +
      line(X_PROXY + 8, X_TRUSTED, 1, 'stroke-dasharray="2 3" opacity="0.6"') +
      verdict(X_PROXY + 12);
  } else if (l.kind === "deny-proxy") {
    body = line(X_WALL, X_PROXY - 6) + cross(X_PROXY, y, tone) + verdict(X_PROXY + 12);
  } else {
    body = cross(X_WALL, y, tone) + verdict(X_WALL + 12);
  }
  return `<g class="lane ${l.fresh ? "fresh" : ""}"><title>${esc(l.label)}</title>
    <text class="lane-label" x="20" y="${y + 3.5}">${esc(truncate(l.label, 38))}</text>${body}</g>`;
}

export function renderMap(s) {
  const edges = new Set(s.map.active_edges);
  const on = (e) => (edges.has(e) ? "on" : "");
  const sb = s.sandbox;
  const launched = sb.state !== "none";
  const killed = sb.state === "killed";
  const lanes = s.lanes.slice(-MAX_LANES);
  const H = Math.max(344, LANE_TOP + lanes.length * LANE_GAP + 8);
  const policy = s.policy;
  const policyTitle = `${policy.name}${policy.hash ? ` · ${policy.hash}` : ""}${policy.rules ? ` · ${policy.rules} rules` : ""}`;

  const zones = [
    [8, "UNTRUSTED", "sandbox: generated program + guest worker"],
    [264, "BOUNDARY", "proxy + verifier + policy"],
    [520, "TRUSTED", "tool workers + Temporal + control plane"],
  ].map(([x, label, sub]) => `<rect class="zone" x="${x}" y="8" width="232" height="${H - 16}" rx="6"/>
      <text class="zone-label" x="${x + 12}" y="26">${label}</text><text class="sub" x="${x + 12}" y="40">${sub}</text>`).join("");

  const runtime = runtimeName(sb.runtime);
  const box = runtime === "gVisor" ? "gVisor sandbox" : `Sandbox (${runtime})`;
  const sandboxTitle = launched ? `${box} · ${sb.name}${killed ? " · killed" : ""}` : `${box} · not started`;
  const sandboxStroke = killed ? "var(--red)" : launched ? "var(--stroke-1)" : "var(--stroke-2)";
  const dash = killed || !launched ? 'stroke-dasharray="4 3"' : "";
  const kernelLine = launched
    ? truncate(`${runtime === "gVisor" ? "runsc" : runtime} · ${sb.kernel} · network ${sb.network}`, 40)
    : "runsc · network none";

  const held = s.held > 0
    ? `<g class="held"><rect x="424" y="148" width="56" height="20" rx="10"/><text x="452" y="162" text-anchor="middle">${s.held} held</text></g>`
    : "";

  const laneRows = lanes.length
    ? lanes.map((l, i) => lane(l, LANE_TOP + i * LANE_GAP)).join("")
    : `<text class="muted" x="20" y="258" style="font-size:10px">Each call from the program appears here as it crosses, or fails to cross, the boundary.</text>`;

  return `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="System map">
    ${zones}
    <g opacity="${launched ? 1 : 0.35}">
      <rect x="20" y="52" width="208" height="136" rx="6" fill="none" stroke="${sandboxStroke}" ${dash}/>
      <text x="32" y="69" font-size="10.5" font-weight="600" fill="${killed ? "var(--red)" : "var(--text-1)"}">${esc(sandboxTitle)}</text>
      <text class="sub" x="32" y="82">${esc(kernelLine)}</text>
      ${node(32, 92, 184, 36, "Generated program", `activity ${s.program.activity || "not written yet"}`, { active: !!on("pg"), dim: killed })}
      ${node(32, 140, 184, 36, "Guest worker", "queue governed-program-guest", { active: !!on("g2p"), dim: killed })}
      <line class="edge ${on("pg")}" x1="124" y1="128" x2="124" y2="140"/>
      <circle class="wall-hole" cx="${X_WALL}" cy="158" r="4" ${killed ? 'stroke="var(--stroke-2)"' : ""}/>
      <text class="sub" x="216" y="185" text-anchor="end" style="font-size:9px">proxy.sock (only way out)</text>
    </g>
    ${node(276, 52, 208, 26, policyTitle, "", { active: !!on("v2pol") })}
    ${node(276, 88, 208, 36, "Dogwood verifier", "judges every tool call (Cedar)", { active: !!on("p2v") })}
    ${node(276, 140, 208, 36, "Policy proxy", "Unix socket · RPC allowlist", { active: !!on("g2p") })}
    <line class="edge ${on("v2pol")}" x1="${X_PROXY}" y1="78" x2="${X_PROXY}" y2="88"/>
    <line class="edge ${on("p2v")}" x1="${X_PROXY}" y1="124" x2="${X_PROXY}" y2="140"/>
    ${held}
    ${node(532, 88, 208, 36, "Tool worker", "governed-program-tools · holds creds", { active: !!on("t2w") })}
    ${node(532, 140, 208, 36, "Temporal server", "history · task queues · :7233", { active: !!on("p2t") })}
    ${node(532, 188, 208, 24, "Control plane · launcher", "", { active: !!on("launch") })}
    <line class="edge ${on("t2w")}" x1="${X_TRUSTED}" y1="124" x2="${X_TRUSTED}" y2="140"/>
    <line class="edge ${on("g2p")}" x1="216" y1="158" x2="276" y2="158"/>
    <line class="edge ${on("p2t")}" x1="484" y1="158" x2="532" y2="158"/>
    <path class="edge ${on("launch")}" d="M532 200 H124 V188" stroke-dasharray="4 3"/>
    <text class="muted ${on("launch")}" x="${X_PROXY}" y="213" text-anchor="middle">launch: scoped key + runsc (bypasses proxy)</text>
    <line class="track" x1="14" y1="224" x2="${W - 14}" y2="224"/>
    <text x="20" y="240" font-size="9.5" font-weight="650" letter-spacing="0.6" fill="var(--text-3)">CALLS LEAVING THE SANDBOX</text>
    ${laneRows}
  </svg>`;
}
