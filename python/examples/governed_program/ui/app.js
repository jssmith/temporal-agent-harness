// Playpen: renders one guest run from the feed service's state stream. Each pane re-renders only
// when its part of the state changes, keeping scroll positions and the reviewer's clicks intact.
import { renderMap } from "./map.js";
import { clock, esc, runtimeName } from "./util.js";

const params = new URLSearchParams(location.search);
const panes = {
  agent: document.getElementById("pane-agent"),
  map: document.getElementById("pane-map"),
  stream: document.getElementById("pane-stream"),
  approvals: document.getElementById("pane-approvals"),
  sandbox: document.getElementById("pane-sandbox"),
  history: document.getElementById("pane-history"),
};
const topbar = document.getElementById("topbar");
const cache = {};
const busy = {};
const errors = {};
let state = null;
let connection = "connecting";
let workflowId = params.get("wid");

const dot = (tone, small = true) => `<span class="dot ${small ? "s" : ""} tone-${tone || "neutral"}"></span>`;
const head = (title, trailing = "") =>
  `<div class="pane-head"><span class="pane-title">${esc(title)}</span><span class="spacer"></span>${trailing}</div>`;
const kv = (label, value, { tone, mono } = {}) =>
  `<div class="kv"><span class="k">${esc(label)}</span><span class="val ${mono ? "mono" : ""} ${tone ? `tone-${tone}` : ""}">${esc(value)}</span></div>`;

// ------------------------------------------------------------------ top bar

function renderTopbar() {
  const s = state;
  const wid = s?.started ? s.workflow_id : null;
  const status = s?.status ?? { label: connection === "connecting" ? "Connecting" : "No session", tone: "neutral" };
  const down = s ? Object.entries(s.sources) : [];
  const conn = connection === "lost" ? `<span class="sources-down">feed reconnecting…</span>` : "";
  const srcs = down.length
    ? `<span class="sources-down" title="${esc(down.map(([k, v]) => `${k}: ${v}`).join("\n"))}">${down.length} source${down.length > 1 ? "s" : ""} unreachable: ${esc(down.map(([k]) => k).join(", "))}</span>`
    : "";
  const html = `<span class="brand">Playpen</span><span class="labs">LABS</span><span class="vsep"></span>
    <span class="k">session</span><span class="v">${esc(s?.session_id || "—")}</span>
    <span class="k">workflow</span><span class="v ${wid ? "" : "dim"}">${esc(wid || "not started")}</span>
    <span style="flex:1"></span>${conn}${srcs}
    <span class="badge" title="${esc(s?.sandbox.runtime || "")}"><span class="k">runtime</span> ${esc(runtimeName(s?.sandbox.runtime))}</span>
    <span class="badge"><span class="k">policy</span> ${esc(s?.policy.name || "bank.dw")} <span class="mono k">${esc(s?.policy.hash || "")}</span></span>
    <span class="status ${status.tone === "neutral" ? "tone-neutral" : `tone-${status.tone}`}">${dot(status.tone, false)}${esc(status.label)}</span>`;
  if (cache.topbar !== html) {
    topbar.innerHTML = html;
    cache.topbar = html;
  }
}

// ------------------------------------------------------------------ agent

function message(m) {
  const fresh = m.fresh ? "fresh" : "";
  if (m.role === "user") return `<div class="msg-user">${esc(m.text)}</div>`;
  if (m.role === "agent") return `<div class="msg-agent ${fresh}"><span class="who">Agent</span>${esc(m.text)}</div>`;
  const quote = m.quote ? `<div class="quote">${esc(m.quote)}</div>` : "";
  return `<div class="msg-event ${fresh}">${dot(m.tone)}<div class="text">${esc(m.text)}${quote}</div></div>`;
}

function agentPane(s) {
  const p = s.program;
  const lines = p.lines;
  const injected = lines.some((l) => l.injected);
  const code = lines.length
    ? `<div class="program">
        <div class="program-head"><span class="file">${esc(p.filename)}</span><span class="meta">generated · activity code</span>
          <span style="flex:1"></span><span class="meta">${lines.length} lines</span></div>
        <div class="code">${lines
          .map((l) => {
            const hot = p.current_step && l.step === p.current_step;
            return `<div class="ln ${hot ? "hot" : ""} ${l.injected ? "injected" : ""}"><span class="no">${l.n}</span><span class="bar"></span><span class="src">${esc(l.text) || " "}</span></div>`;
          })
          .join("")}</div>
        <div class="program-foot">Workflow: <span class="mono">${esc(p.workflow)}</span>, a fixed template. Trusted, never generated.${
          injected ? ` <span class="tone-red">Red bar: lines added after the agent read the injected row.</span>` : ""
        }</div>
      </div>`
    : `<div class="composer"><div><span>Ask the analyst agent…</span><span>Enter to send</span></div></div>`;
  return head("Agent", `<span class="meta">${esc(s.agent.name || "")}</span>`) +
    `<div class="pane-body"><div class="conv">${s.conversation.map(message).join("")}</div>${code}</div>`;
}

// ------------------------------------------------------------------ stream

function streamPane(s) {
  const rows = s.decisions;
  const body = rows.length
    ? `<table class="stream"><thead><tr><th>Time</th><th>Call</th><th>Rule</th><th>Verdict</th><th>Decided by</th><th>Policy</th></tr></thead><tbody>${rows
        .map(
          (r) => `<tr class="${r.fresh ? "fresh" : ""}" title="${esc(r.message || r.args_summary)}">
            <td class="time">${clock(r.time)}</td><td class="call">${esc(r.call)}</td><td class="rule">${esc(r.rule)}</td>
            <td><span class="verdict-cell tone-${r.tone}">${dot(r.tone)}${esc(r.verdict)}${r.attempts > 1 ? `<span class="attempts">×${r.attempts}</span>` : ""}</span></td>
            <td>${esc(r.decided_by)}</td><td class="hash">${esc(r.policy_hash)}</td></tr>`,
        )
        .join("")}</tbody></table>`
    : `<div class="empty">Decisions appear once the guest worker makes its first call through the proxy.</div>`;
  return head("Decision stream", `<span class="meta">${rows.length} records · audit log</span>`) + `<div class="pane-body">${body}</div>`;
}

// ------------------------------------------------------------------ approvals

function card(a, s) {
  const pending = a.state === "pending";
  const tone = pending ? "yellow" : a.state === "approved" ? "green" : "red";
  const title = pending ? "Read a risk table" : a.state === "approved" ? "Approved" : "Denied";
  const [queue, tool] = (a.call || "").split("/");
  const err = errors[a.id] ? `<div class="error">${esc(errors[a.id])}</div>` : "";
  const footer = pending
    ? `<div class="actions"><button class="btn primary" id="approve" data-decide="approve" data-id="${esc(a.id)}" ${busy[a.id] ? "disabled" : ""}>Approve</button>
         <button class="btn" id="deny" data-decide="deny" data-id="${esc(a.id)}" ${busy[a.id] ? "disabled" : ""}>Deny</button>
         <span class="as">as Risk reviewer</span></div>${err}`
    : `<div class="resolved">${a.state === "approved" ? "Approved" : "Denied"} by ${esc(a.resolved_by || "Risk reviewer")}${
        a.resolved_at ? ` at ${clock(a.resolved_at)}` : ""
      }. ${
        a.state === "approved"
          ? "Recorded; a retry of this call in this run will not ask again."
          : "The workflow gets a PolicyDenied failure and carries on without it."
      }</div>`;
  return `<div class="card">
    <div class="card-head">${dot(tone, false)}<span class="title">${title}</span><span class="spacer"></span><span class="when">${clock(a.created_at)}</span></div>
    <div class="callbox">${esc(queue)}${tool ? `/${esc(tool)}` : ""}<br>${esc(a.summary)}</div>
    ${a.rule ? kv("Rule", a.rule, { mono: true }) : ""}
    ${kv("Reason", a.reason)}
    ${kv("Workflow", a.workflow_id || s.workflow_id, { mono: true })}
    ${kv("Decision", a.short_id, { mono: true })}
    ${footer}</div>`;
}

function approvalsPane(s) {
  const waiting = s.approvals.filter((a) => a.state === "pending").length;
  const trailing = waiting
    ? `<span class="meta tone-yellow" style="display:inline-flex;align-items:center;gap:5px">${dot("yellow")} ${waiting} waiting</span>`
    : `<span class="meta">0 waiting</span>`;
  const body = s.approvals.length
    ? `<div class="cards">${s.approvals.map((a) => card(a, s)).join("")}</div>`
    : `<div class="empty">Inbox clear. Calls the policy escalates wait here for a reviewer.</div>`;
  return head("Approvals", trailing) + `<div class="pane-body">${body}</div>`;
}

// ------------------------------------------------------------------ sandbox

function sandboxPane(s) {
  const sb = s.sandbox;
  if (sb.state === "none") {
    return head("Sandbox", `<span class="meta">not started</span>`) +
      `<div class="pane-body"><div class="empty">Starts when the program is ready. The launcher reports runtime, kernel, network, and socket here.</div></div>`;
  }
  const killed = sb.state === "killed";
  const trailing = `<span class="meta" style="display:inline-flex;align-items:center;gap:5px">${dot(killed ? "red" : "green")} ${killed ? "killed" : esc(sb.state)}</span>`;
  const rows = [
    kv("Sandbox", sb.name, { mono: true }),
    kv("State", killed ? "Killed" : sb.restarted ? "Running (restarted)" : "Running", { tone: killed ? "red" : "green" }),
    kv("Runtime", sb.runtime),
    kv("Kernel", sb.kernel, { mono: true }),
    kv("Network", sb.network, { mono: true }),
    kv("Sockets", `1 · ${sb.socket}`, { mono: true }),
    kv("Key", `scoped to ${sb.key_scope}`),
    kv("Uptime", killed ? `${sb.uptime} (stopped)` : sb.uptime, { mono: true }),
  ];
  if (sb.egress_attempts) {
    rows.push(kv("Egress", `${sb.egress_attempts} attempt${sb.egress_attempts > 1 ? "s" : ""}, ${sb.egress_last_error}`, { tone: "red" }));
  }
  if (sb.previous.length || killed) {
    rows.push(`<div class="hr"></div>`);
    for (const p of sb.previous) {
      rows.push(kv("Previous", `${p.name} ${p.reason || "stopped"} ${clock(p.stopped_at)} after ${p.uptime}`, { tone: "red" }));
    }
    if (killed) rows.push(kv("Stopped", `${sb.name} killed ${clock(sb.stopped_at)}; waiting for a new sandbox`, { tone: "red" }));
    else rows.push(kv("Resumed", `${sb.name} at ${clock(sb.started_at)} from history`, { tone: "green" }));
  }
  return head("Sandbox", trailing) + `<div class="pane-body"><div class="kvs">${rows.join("")}</div></div>`;
}

// ------------------------------------------------------------------ history

function historyPane(s) {
  const h = s.history;
  const link = h.temporal_ui_url
    ? `<a class="btn ghost" id="open-temporal" href="${esc(h.temporal_ui_url)}" target="_blank" rel="noopener">Open in Temporal UI ↗</a>`
    : `<span class="btn ghost disabled">Open in Temporal UI</span>`;
  const trailing = `<span class="meta mono">${s.started ? `${esc(s.workflow_id)} · ${h.event_count} events` : "no workflow yet"}</span>${link}`;
  const body = s.started
    ? `<div class="strip"><div class="strip-row"><div class="earlier" hidden></div>${h.items
        .map((i) =>
          i.kind === "note"
            ? `<div class="note tone-${i.tone}">${esc(i.text)}</div>`
            : `<div class="ev ${i.fresh ? "fresh" : ""}" title="${esc(clock(i.time))}"><div class="top"><span class="n">${i.n}</span>
                 <span class="type ${i.tone ? `tone-${i.tone}` : ""}">${esc(i.type)}</span></div><div class="detail">${esc(i.detail)}</div></div>`,
        )
        .join("")}</div></div>`
    : `<div class="empty">The control plane starts the guest workflow at launch; its history streams here from Temporal.</div>`;
  return head("Temporal history · guest workflow", trailing) + `<div class="pane-body">${body}</div>`;
}

// ------------------------------------------------------------------ rendering

const SCROLLERS = { agent: [".conv", ".code"], stream: [".pane-body"], approvals: [".pane-body"], sandbox: [".pane-body"] };

function paint(name, html, after) {
  if (cache[name] === html) return;
  const el = panes[name];
  const saved = (SCROLLERS[name] || []).map((sel) => {
    const node = el.querySelector(sel);
    return node && { sel, top: node.scrollTop, left: node.scrollLeft, atEnd: node.scrollHeight - node.scrollTop - node.clientHeight < 24 };
  });
  const first = cache[name] === undefined;
  el.innerHTML = html;
  cache[name] = html;
  for (const s of saved) {
    const node = s && el.querySelector(s.sel);
    if (node) {
      node.scrollTop = s.top;
      node.scrollLeft = s.left;
    }
  }
  after?.(el, first);
}

function render() {
  renderTopbar();
  if (!state) {
    for (const [name, el] of Object.entries(panes)) {
      const html = `${head(name)}<div class="pane-body"><div class="empty">${connection === "none" ? "Waiting for a session from the control plane…" : "Connecting to the feed…"}</div></div>`;
      if (cache[name] !== html) { el.innerHTML = html; cache[name] = html; }
    }
    return;
  }
  const s = state;
  paint("agent", agentPane(s), (el) => {
    const conv = el.querySelector(".conv");
    // A fresh quoted row (the injected text) stays in view; otherwise follow the latest message.
    const quoted = [...(conv?.querySelectorAll(".msg-event.fresh .quote") || [])].pop()?.closest(".msg-event");
    if (quoted) conv.scrollTop = quoted.offsetTop - conv.offsetTop - 8;
    else if (conv) conv.scrollTop = conv.scrollHeight;
    const hot = el.querySelector(".ln.hot");
    const code = el.querySelector(".code");
    if (code) {
      if (hot) code.scrollTop = Math.max(0, hot.offsetTop - code.offsetTop - 2 * hot.offsetHeight);
      else code.scrollTop = code.scrollHeight;
    }
  });
  paint("map", head("System map", `<span class="legend"><span>${dot("green")} allowed</span><span>${dot("yellow")} waiting</span><span>${dot("red")} stopped</span></span>`) +
    `<div class="pane-body">${renderMap(s)}</div>`);
  paint("stream", streamPane(s), (el) => {
    const body = el.querySelector(".pane-body");
    body.scrollTop = body.scrollHeight;
  });
  paint("approvals", approvalsPane(s));
  paint("sandbox", sandboxPane(s));
  paint("history", historyPane(s), fitStrip);
}

// The latest events are kept whole at the right; earlier ones give way to a count.
function fitStrip(el = panes.history) {
  const strip = el.querySelector(".strip");
  const row = strip?.querySelector(".strip-row");
  if (!row) return;
  const more = row.querySelector(".earlier");
  const items = [...row.children].filter((c) => c !== more);
  for (const i of items) i.hidden = false;
  more.hidden = true;
  const room = strip.clientWidth - 24;
  let dropped = 0;
  while (row.offsetWidth > room && dropped < items.length - 1) {
    items[dropped++].hidden = true;
    more.hidden = false;
    more.textContent = `‹ ${dropped} earlier`;
  }
}
window.addEventListener("resize", () => fitStrip());

// ------------------------------------------------------------------ focus (for the recorded walkthrough)

function setFocus(list) {
  const want = new Set(list || []);
  for (const [name, el] of Object.entries(panes)) el.classList.toggle("focused", want.has(name));
}

// ------------------------------------------------------------------ approve / deny

panes.approvals.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-decide]");
  if (!button) return;
  const id = button.dataset.id;
  busy[id] = true;
  delete errors[id];
  render();
  try {
    const r = await fetch(`api/decisions/${encodeURIComponent(id)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ decision: button.dataset.decide, reason: "Risk reviewer in Playpen" }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok || !body.ok) errors[id] = r.status === 409 ? "Already answered." : `The proxy did not record it (${body.proxy_status ?? r.status}).`;
  } catch (err) {
    errors[id] = `Could not reach the feed: ${err}`;
  } finally {
    delete busy[id];
    cache.approvals = undefined;
    render();
  }
});

// ------------------------------------------------------------------ data

async function pickSession() {
  for (;;) {
    try {
      const r = await fetch("api/sessions");
      const list = r.ok ? await r.json() : [];
      if (Array.isArray(list) && list.length) return list[list.length - 1].workflow_id;
    } catch {}
    connection = "none";
    render();
    await new Promise((res) => setTimeout(res, 1500));
  }
}

function connect() {
  const source = new EventSource(`api/session/${encodeURIComponent(workflowId)}/stream`);
  source.onmessage = (event) => {
    state = JSON.parse(event.data);
    window.playpenState = state;
    connection = "open";
    render();
  };
  source.onerror = () => {
    connection = "lost";
    render();
  };
}

window.playpen = { setFocus, get state() { return state; } };
setFocus((params.get("focus") || "").split(",").filter(Boolean));
render();
if (!workflowId) workflowId = await pickSession();
connect();
