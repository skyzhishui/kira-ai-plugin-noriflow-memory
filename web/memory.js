/* ============================================================
   Memory maintenance page (self-contained plugin, no build tooling)
   API: this plugin's contributed /api/plugin/kira-ai-plugin-noriflow-memory/memory/*;
   Auth reuses the same-origin session cookie (host WebUI login state);
   theme: host plugin-bridge stamps [data-theme] and follows host switches.
   ============================================================ */

const API_BASE = "/api/plugin/kira-ai-plugin-noriflow-memory/memory";

/* ---------------- Icons (inline element-plus paths, text color) ---------------- */
const ICON_PATHS = {
  Search: ["m795.9 750.7 125 125a32 32 0 0 1-45.3 45.2l-125-125a416 416 0 1 1 45.3-45.2zM480 832a352 352 0 1 0 0-704 352 352 0 0 0 0 704"],
  EditPen: ["m199 672.6 194 112 224-388-193.9-112-224 388.1zm-23.8 60.2L208 881.1 353 835.4zM455 229.2l194 112 56.7-98-194-112zM104.3 708.8l384-665 304.8 176-384 665h.1l-248.4 78.3zm384 254.3v-64h448v64z"],
  Delete: ["M160 256H96a32 32 0 0 1 0-64h256V96a32 32 0 0 1 32-32h256a32 32 0 0 1 32 32v96h256a32 32 0 1 1 0 64h-64v672a32 32 0 0 1-32 32H192a32 32 0 0 1-32-32zm448-64v-64H416v64zM224 896h576V256H224zm192-128a32 32 0 0 1-32-32V416a32 32 0 0 1 64 0v320a32 32 0 0 1-32 32m192 0a32 32 0 0 1-32-32V416a32 32 0 0 1 64 0v320a32 32 0 0 1-32 32"],
  DocumentChecked: ["M805.5 320 640 154.5V320zm26.5 64H576V128H192v768h640zM160 64h480l256 256v608a32 32 0 0 1-32 32H160a32 32 0 0 1-32-32V96a32 32 0 0 1 32-32m318.4 582.1 181-181 45.2 45.3-226.2 226.2L320 578.3l45.2-45.3z"],
  CircleCloseFilled: ["M512 64a448 448 0 1 1 0 896 448 448 0 0 1 0-896m0 393.7L408 353.6a38.4 38.4 0 1 0-54.4 54.3l104 104.1-104 104a38.4 38.4 0 1 0 54.3 54.4l104.1-104 104 104a38.4 38.4 0 1 0 54.4-54.3L566.4 512l104-104a38.4 38.4 0 1 0-54.3-54.4z"],
  MagicStick: ["M512 64h64v192h-64zm0 576h64v192h-64zM160 480v-64h192v64zm576 0v-64h192v64zM249.9 199 295 154l135.7 135.7-45.2 45.2-135.7-135.7zm407.3 407.4 45.2-45.2 135.7 135.7-45.2 45.2zM114 923.2 68.8 878l316.8-316.8 45.2 45.2zm588.4-588.4-45.2-45.2 135.7-135.7 45.2 45.2z"],
  Close: ["M764.3 214.6 512 466.9 259.7 214.6a32 32 0 0 0-45.1 45.1L466.8 512 214.5 764.2a32 32 0 1 0 45.1 45.2L512 557.2l252.3 252.3a32 32 0 0 0 45.1-45.1L557.1 512l252.3-252.4a32 32 0 1 0-45.1-45.2z"],
};

const UI = {
  svg(name) {
    const ns = "http://www.w3.org/2000/svg";
    const s = document.createElementNS(ns, "svg");
    s.setAttribute("viewBox", "0 0 1024 1024");
    s.setAttribute("fill", "currentColor");
    s.setAttribute("aria-hidden", "true");
    for (const d of ICON_PATHS[name] || []) {
      const p = document.createElementNS(ns, "path");
      p.setAttribute("d", d);
      s.appendChild(p);
    }
    return s;
  },

  el(tag, attrs = {}, children = []) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") node.className = v;
      else if (k === "html") node.innerHTML = v;
      else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
      else if (v !== null && v !== undefined) node.setAttribute(k, v);
    }
    for (const c of [].concat(children)) {
      if (c === null || c === undefined) continue;
      node.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    }
    return node;
  },

  // Auth: same-origin session cookie (credentials: same-origin), no manual token
  async api(path, opts = {}) {
    const headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
    let resp;
    try {
      resp = await fetch(API_BASE + path, Object.assign({ credentials: "same-origin" }, opts, { headers }));
    } catch (e) {
      throw new Error("网络错误：" + e.message);
    }
    let body = null;
    try { body = await resp.json(); } catch (e) { /* not JSON */ }
    if (resp.status === 401) {
      throw new Error((body && body.detail) || "登录已过期，请刷新页面重新登录");
    }
    if (!resp.ok) {
      // FastAPI request-validation 422 returns detail as an ARRAY of
      // {loc,msg,...} objects — stringify instead of "[object Object],..."
      let detail = (body && body.detail) || ("请求失败 HTTP " + resp.status);
      if (Array.isArray(detail)) {
        detail = detail.map((x) => (x && x.msg) ? x.msg : JSON.stringify(x)).join("; ");
      }
      throw new Error(detail);
    }
    if (body === null) throw new Error("响应不是合法 JSON（HTTP " + resp.status + "）");
    if (opts.meta) {
      // The HTTP Date header is the server clock for this response. Server
      // timestamps (e.g. a backfill finished_at) must be compared against it
      // instead of the browser clock, which may be skewed.
      const hdr = resp.headers.get("date");
      return { data: body, serverNow: hdr ? Date.parse(hdr) : NaN };
    }
    return body;
  },

  badge(text, color = "sky") {
    return this.el("span", { class: "badge " + color }, [text]);
  },

  // List/profile load failure card with a retry affordance (fire-and-forget
  // refreshes have no other place to surface the error).
  errorCard(err, onRetry) {
    const msg = err && err.message ? err.message : String(err);
    return this.el("div", { class: "card" }, [
      this.el("div", { class: "empty" }, [this.svg("CircleCloseFilled"), "加载失败：" + msg]),
      onRetry
        ? this.el("div", { style: "text-align:center;margin-top:8px" }, [
            this.el("button", { class: "btn small ghost", onclick: onRetry }, ["重试"]),
          ])
        : null,
    ]);
  },

  scoreBar(score, max = 10) {
    const pct = Math.max(0, Math.min(100, (score / max) * 100));
    return this.el("span", { class: "score-bar", title: `确信度 ${score}` }, [
      this.el("i", { style: `width:${pct}%` }),
      this.el("span", {}, [String(score)]),
    ]);
  },

  pager(total, page, size, onGo) {
    const pages = Math.max(1, Math.ceil(total / size));
    return this.el("div", { class: "pager" }, [
      this.el("button", { class: "btn small ghost", disabled: page <= 1 ? "" : null, onclick: () => onGo(page - 1) }, ["← 上一页"]),
      this.el("span", {}, [`第 ${page} / ${pages} 页 · 共 ${total} 条`]),
      this.el("button", { class: "btn small ghost", disabled: page >= pages ? "" : null, onclick: () => onGo(page + 1) }, ["下一页 →"]),
    ]);
  },

  fmtTime(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    const p = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
  },

  paw() { return this.el("span", { class: "paw" }, [this.svg("MagicStick")]); },

  toast(msg, type = "") {
    const root = document.getElementById("toast-root");
    const t = this.el("div", { class: "toast " + type }, [msg]);
    root.appendChild(t);
    setTimeout(() => t.remove(), 3600);
  },

  // Keyboard entry for elements whose only action is a pointer click: make
  // them focusable and run the same handler on Enter/Space. No synthetic
  // click is dispatched, so drag gestures and double-click handlers are not
  // affected. Row wrappers with inner controls (e.g. a delete button) keep
  // their own keys: only the wrapper itself activates the row action.
  kbClick(node, handler) {
    node.setAttribute("tabindex", "0");
    node.addEventListener("keydown", (e) => {
      if (e.key !== "Enter" && e.key !== " ") return;
      if (e.target !== node) return;
      e.preventDefault();
      handler(e);
    });
    return node;
  },

  // Focusable children of a container, in DOM order (disabled and hidden
  // nodes excluded). Queried live so dynamically rebuilt modal bodies are
  // included.
  _focusables(box) {
    return Array.from(box.querySelectorAll(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
    )).filter((n) => !n.disabled && n.getClientRects().length > 0);
  },

  // The page behind the dialog is inert while a modal is open: pointer and
  // keyboard navigation cannot reach it, matching the visual overlay. The
  // dialog layer and transient toasts stay active.
  _setBackgroundInert(on) {
    for (const child of document.body.children) {
      if (child.id === "modal-root" || child.id === "toast-root") continue;
      if (on) child.setAttribute("inert", "");
      else child.removeAttribute("inert");
    }
  },

  modal(title, bodyNodes, actions = [], opts = {}) {
    const root = document.getElementById("modal-root");
    // Capture the opener before the previous dialog layer is cleared so
    // closing returns the keyboard user to the control that opened this one.
    const opener = document.activeElement && typeof document.activeElement.focus === "function"
      ? document.activeElement : null;
    root.innerHTML = "";
    if (this._modalKeyHandler) {
      document.removeEventListener("keydown", this._modalKeyHandler);
      this._modalKeyHandler = null;
    }
    let box = null;
    const close = () => {
      // Only the modal that is still mounted may clear the root/inert state:
      // a slow save started in a previous dialog finishing after another
      // modal opened used to wipe the newer one (its own listener cleanup
      // still runs).
      const mounted = box !== null && root.firstChild === box;
      if (mounted) root.innerHTML = "";
      document.removeEventListener("keydown", onKey);
      if (this._modalKeyHandler === onKey) this._modalKeyHandler = null;
      if (!mounted) return; // superseded by a newer dialog: leave its state alone
      this._setBackgroundInert(false);
      if (opener && opener.isConnected) opener.focus();
    };
    const onKey = (e) => {
      if (e.key === "Escape") { close(); return; }
      if (e.key !== "Tab" || box === null) return;
      // Wrap Tab/Shift+Tab inside the dialog. The background is inert, so
      // without an explicit wrap focus would jump to browser chrome.
      const items = this._focusables(box);
      if (!items.length) { e.preventDefault(); box.focus(); return; }
      const first = items[0], last = items[items.length - 1];
      const active = document.activeElement;
      if (!box.contains(active)) { e.preventDefault(); first.focus(); return; }
      if (e.shiftKey && active === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && active === last) { e.preventDefault(); first.focus(); }
    };
    this._modalKeyHandler = onKey;
    box = this.el("div", { class: "modal" + (opts.wide ? " wide" : ""), tabindex: "-1" }, [
      this.el("div", { class: "modal-head" }, [
        this.el("h3", {}, [title]),
        this.el("button", { class: "modal-close", type: "button", title: "关闭", "aria-label": "关闭", onclick: close }, [this.svg("Close")]),
      ]),
      this.el("div", { class: "modal-body" }, bodyNodes),
      this.el("div", { class: "modal-actions" }, actions.concat([
        this.el("button", { class: "btn ghost", onclick: close }, [opts.closeLabel || "关闭"]),
      ])),
    ]);
    root.appendChild(box);
    root.onclick = (e) => { if (e.target === root) close(); };
    document.addEventListener("keydown", onKey);
    this._setBackgroundInert(true);
    // Initial focus goes to the first focusable element (the close button),
    // which also puts screen readers inside the dialog.
    const firstFocusable = this._focusables(box)[0];
    (firstFocusable || box).focus();
    return close;
  },

  confirm(title, text, onYes, yesLabel) {
    const closeP = this.modal(title, [this.el("p", {}, [text])], [
      this.el("button", {
        class: "btn danger",
        onclick: async () => { closeP(); await onYes(); },
      }, [yesLabel || "确认"]),
    ]);
  },
};

/* ============================================================
   力导向关系图谱（零依赖 SVG：_sim 力模拟 + 拖拽/缩放/选中）
   ============================================================ */
class ForceGraph {
  constructor(host, opts = {}) {
    this.host = host;
    this.onSelect = opts.onSelect || (() => {});
    this.W = 1000; this.H = 560;
    // Initial/reset zoom: pre-zoom by viewport (wide 1.35, narrow 2.2)
    this.baseScale = window.matchMedia("(max-width: 720px)").matches ? 2.2 : 1.35;
    this.scale = this.baseScale; this.tx = 0; this.ty = 0;
    this.showActive = true; this.showPending = true; this.showSuperseded = true;
    this.selected = null;               // {type:"node"|"edge", id}
    this.rawNodes = []; this.rawEdges = [];
    this.nodes = []; this.edges = [];
    this._alpha = 1; this._raf = null;
    this._build();
  }

  _svg(tag) { return document.createElementNS("http://www.w3.org/2000/svg", tag); }

  _build() {
    this.svg = this._svg("svg");
    this.svg.setAttribute("viewBox", `${-this.W / 2} ${-this.H / 2} ${this.W} ${this.H}`);
    this.svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
    this.gRoot = this._svg("g");
    this.gEdges = this._svg("g");
    this.gNodes = this._svg("g");
    this.gRoot.appendChild(this.gEdges);
    this.gRoot.appendChild(this.gNodes);
    this.svg.appendChild(this.gRoot);
    this.host.appendChild(this.svg);
    this._bind();
  }

  setData(nodes, edges) {
    this.rawNodes = nodes || [];
    this.rawEdges = edges || [];
    this._applyFilter(true);
  }

  setFilter(showActive, showPending, showSuperseded) {
    this.showActive = showActive; this.showPending = showPending;
    this.showSuperseded = showSuperseded;
    this._applyFilter(false);
  }

  select(type, id) {
    this.selected = type ? { type, id } : null;
    this._applyDim();
    this.onSelect(this.selected);
  }

  resetView() {
    // baseScale from build time goes stale across breakpoints: re-read here
    this.baseScale = window.matchMedia("(max-width: 720px)").matches ? 2.2 : 1.35;
    this.scale = this.baseScale; this.tx = 0; this.ty = 0;
    this._applyTransform();
    this.reheat();
  }

  reheat() { this._alpha = 1; this._still = 0; this._loop(); }

  // ---------------- Data and rendering ----------------

  _applyFilter(fresh) {
    const vis = (s) => s === "active" ? this.showActive
      : s === "pending" ? this.showPending : this.showSuperseded;
    const edges = this.rawEdges.filter((e) => vis(e.status));
    const used = new Set();
    edges.forEach((e) => { used.add(e.subject); used.add(e.object); });
    const prev = new Map(this.nodes.map((n) => [n.id, n]));
    this.nodes = this.rawNodes.filter((n) => used.has(n.id)).map((n, i) => {
      const old = prev.get(n.id);
      const ang = (i / Math.max(1, this.rawNodes.length)) * Math.PI * 2;
      return Object.assign({}, n, {
        x: old ? old.x : Math.cos(ang) * 160,
        y: old ? old.y : Math.sin(ang) * 160,
        vx: 0, vy: 0, fx: null, fy: null, el: null,
      });
    });
    const byId = new Map(this.nodes.map((n) => [n.id, n]));
    this.edges = edges
      .map((e) => Object.assign({}, e, { s: byId.get(e.subject), o: byId.get(e.object) }))
      .filter((e) => e.s && e.o);
    this._renderDom(fresh);
    this._applyDim();
    this.reheat();
  }

  _renderDom(fresh) {
    this.gEdges.innerHTML = "";
    this.gNodes.innerHTML = "";
    for (const e of this.edges) {
      const g = this._svg("g");
      g.setAttribute("class", "fg-edge" + (e.status !== "active" ? " " + e.status : ""));
      g.dataset.edge = String(e.id);
      // Keyboard parity with pointer selection: the group itself is
      // focusable and Enter/Space select it. No synthetic click is
      // dispatched, so drag/zoom gestures are untouched.
      g.setAttribute("tabindex", "0");
      g.setAttribute("focusable", "true");
      g.setAttribute("role", "button");
      g.setAttribute("aria-label", `${e.subject_name || "?"} → ${e.object_name || "?"} ${e.label}`);
      g.addEventListener("keydown", (ev) => {
        if (ev.key !== "Enter" && ev.key !== " ") return;
        ev.preventDefault();
        this.select("edge", String(e.id));
      });
      const line = this._svg("line");
      const hit = this._svg("line");
      hit.setAttribute("class", "hit");
      const text = this._svg("text");
      text.textContent = e.label;
      g.appendChild(line); g.appendChild(hit); g.appendChild(text);
      this.gEdges.appendChild(g);
      e.el = g; e.line = line; e.text = text;
    }
    for (const n of this.nodes) {
      const g = this._svg("g");
      g.setAttribute("class", "fg-node" + (n.is_bot ? " bot" : ""));
      g.dataset.node = n.id;
      g.setAttribute("tabindex", "0");
      g.setAttribute("focusable", "true");
      g.setAttribute("role", "button");
      g.setAttribute("aria-label", `${n.name}${n.is_bot ? "（bot）" : ""}，${n.degree} 条关系边`);
      g.addEventListener("keydown", (ev) => {
        if (ev.key !== "Enter" && ev.key !== " ") return;
        ev.preventDefault();
        this.select("node", n.id);
      });
      const circle = this._svg("circle");
      circle.setAttribute("r", String(9 + Math.min(n.degree, 10)));
      const label = this._svg("text");
      label.setAttribute("dy", "1.4em");
      label.textContent = n.name;
      g.appendChild(circle); g.appendChild(label);
      this.gNodes.appendChild(g);
      n.el = g; n.circle = circle; n.label = label;
    }
    this._applyTransform();
  }

  _applyDim() {
    const sel = this.selected;
    if (!sel) {
      [...this.gEdges.children, ...this.gNodes.children].forEach((g) => g.classList.remove("fg-dim", "sel"));
      return;
    }
    const keepNodes = new Set(); const keepEdges = new Set();
    if (sel.type === "node") {
      keepNodes.add(sel.id);
      for (const e of this.edges) {
        if (e.subject === sel.id || e.object === sel.id) {
          keepEdges.add(String(e.id));
          keepNodes.add(e.subject); keepNodes.add(e.object);
        }
      }
    } else {
      const e = this.edges.find((x) => String(x.id) === String(sel.id));
      if (e) { keepEdges.add(String(e.id)); keepNodes.add(e.subject); keepNodes.add(e.object); }
    }
    for (const e of this.edges) {
      e.el.classList.toggle("fg-dim", !keepEdges.has(String(e.id)));
      e.el.classList.toggle("sel", sel.type === "edge" && String(e.id) === String(sel.id));
    }
    for (const n of this.nodes) {
      n.el.classList.toggle("fg-dim", !keepNodes.has(n.id));
      n.el.classList.toggle("sel", sel.type === "node" && n.id === sel.id);
    }
  }

  // ---------------- Force sim (forces x alpha, cool to 0 -> rest) ----------------

  _tick() {
    const ns = this.nodes, es = this.edges;
    const a = this._alpha;
    const REP = 5200, SPR = 0.03, L = 132, FRICTION = 0.86, GRAV = 0.05;
    // Repulsion is O(n²) for small graphs and dominates the frame budget on
    // big ones (e.g. hundreds of disjoint edges). Up to 160 nodes keep the
    // exact pair loop; above that a uniform spatial grid approximates it so
    // every node still receives repulsion without an O(n²) pass.
    if (ns.length > 160) this._repelGrid(ns, a, REP);
    else this._repelExact(ns, a, REP);
    for (const e of es) {
      const dx = e.o.x - e.s.x, dy = e.o.y - e.s.y;
      const d = Math.sqrt(dx * dx + dy * dy) || 1;
      const f = (d - L) * SPR * a, fx = (dx / d) * f, fy = (dy / d) * f;
      e.s.vx += fx; e.s.vy += fy; e.o.vx -= fx; e.o.vy -= fy;
    }
    for (const n of ns) {
      n.vx += -n.x * GRAV * a; n.vy += -n.y * GRAV * a;
      n.vx *= FRICTION; n.vy *= FRICTION;
      if (n.fx != null) { n.x = n.fx; n.y = n.fy; n.vx = 0; n.vy = 0; }
      else {
        n.x += Math.max(-28, Math.min(28, n.vx));
        n.y += Math.max(-28, Math.min(28, n.vy));
      }
    }
    // Linear cooling to 0: all forces vanish together (repulsion used to
    // ignore alpha decay while gravity decayed -> net drift; friction then
    // drains the residual speed)
    this._alpha = Math.max(0, a - 0.006);
  }

  // Exact O(n²) repulsion: unchanged behaviour for graphs that can afford it.
  _repelExact(ns, a, REP) {
    for (let i = 0; i < ns.length; i++) {
      for (let j = i + 1; j < ns.length; j++) {
        let dx = ns[j].x - ns[i].x, dy = ns[j].y - ns[i].y;
        let d2 = dx * dx + dy * dy;
        if (d2 < 1) { dx = Math.random() + 0.1; dy = Math.random() + 0.1; d2 = dx * dx + dy * dy; }
        const d = Math.sqrt(d2), f = (REP / d2) * a, fx = (dx / d) * f, fy = (dy / d) * f;
        ns[i].vx -= fx; ns[i].vy -= fy; ns[j].vx += fx; ns[j].vy += fy;
      }
    }
  }

  // Approximate repulsion for graphs above the exact threshold.
  //
  // A node is pushed by the nodes in its own 3x3 cell neighbourhood, which
  // covers every pair within one cell edge. The cell edge is derived from the
  // current layout extent (with the viewBox as a floor, so a shrinking layout
  // never collapses the grid) and from the node count, aiming at a handful of
  // nodes per cell. Cells holding more than CELL_MAX nodes are collapsed into
  // a single point mass (count + centroid), so a dense pile-up cannot turn
  // this pass into O(n²): each node does at most 9 * CELL_MAX force
  // evaluations. Every node runs the pass, so all nodes participate (the old
  // global `stride` loop left whole index congruence classes untouched).
  _repelGrid(ns, a, REP) {
    const CELL_MAX = 8;
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const n of ns) {
      if (n.x < minX) minX = n.x;
      if (n.x > maxX) maxX = n.x;
      if (n.y < minY) minY = n.y;
      if (n.y > maxY) maxY = n.y;
    }
    // Degenerate layouts (single point, flat line, initial ring) get a floor
    // proportional to the viewBox so the cell size stays sane. Targeting
    // ~CELL_MAX/2 nodes per cell keeps most cells exact, while the clamp
    // bounds the neighbourhood radius.
    const w = Math.max(maxX - minX, this.W * 0.3);
    const h = Math.max(maxY - minY, this.H * 0.3);
    const cell = Math.max(48, Math.min(240, Math.sqrt((CELL_MAX * w * h) / (2 * ns.length))));

    const cells = new Map();
    for (let i = 0; i < ns.length; i++) {
      const n = ns[i];
      const key = Math.floor((n.x - minX) / cell) + "," + Math.floor((n.y - minY) / cell);
      let c = cells.get(key);
      if (!c) { c = { nodes: [], sx: 0, sy: 0 }; cells.set(key, c); }
      c.nodes.push(i); c.sx += n.x; c.sy += n.y;
    }

    for (let i = 0; i < ns.length; i++) {
      const ni = ns[i];
      const cx = Math.floor((ni.x - minX) / cell);
      const cy = Math.floor((ni.y - minY) / cell);
      for (let ox = -1; ox <= 1; ox++) {
        for (let oy = -1; oy <= 1; oy++) {
          // Missing cells at the grid border are simply skipped.
          const c = cells.get((cx + ox) + "," + (cy + oy));
          if (!c) continue;
          if (c.nodes.length > CELL_MAX) {
            // Point-mass approximation; exclude `ni` from its own cell.
            const own = ox === 0 && oy === 0;
            const mass = c.nodes.length - (own ? 1 : 0);
            if (mass <= 0) continue;
            const mx = (c.sx - (own ? ni.x : 0)) / mass;
            const my = (c.sy - (own ? ni.y : 0)) / mass;
            let dx = mx - ni.x, dy = my - ni.y;
            let d2 = dx * dx + dy * dy;
            if (d2 < 1) { dx = Math.random() + 0.1; dy = Math.random() + 0.1; d2 = dx * dx + dy * dy; }
            const d = Math.sqrt(d2), f = (REP * mass / d2) * a;
            ni.vx -= (dx / d) * f; ni.vy -= (dy / d) * f;
          } else {
            for (const j of c.nodes) {
              if (j === i) continue;
              let dx = ns[j].x - ni.x, dy = ns[j].y - ni.y;
              let d2 = dx * dx + dy * dy;
              if (d2 < 1) { dx = Math.random() + 0.1; dy = Math.random() + 0.1; d2 = dx * dx + dy * dy; }
              const d = Math.sqrt(d2), f = (REP / d2) * a;
              ni.vx -= (dx / d) * f; ni.vy -= (dy / d) * f;
            }
          }
        }
      }
    }
  }

  _draw() {
    for (const e of this.edges) {
      const x1 = e.s.x, y1 = e.s.y, x2 = e.o.x, y2 = e.o.y;
      e.line.setAttribute("x1", x1); e.line.setAttribute("y1", y1);
      e.line.setAttribute("x2", x2); e.line.setAttribute("y2", y2);
      e.el.querySelector(".hit").setAttribute("x1", x1);
      e.el.querySelector(".hit").setAttribute("y1", y1);
      e.el.querySelector(".hit").setAttribute("x2", x2);
      e.el.querySelector(".hit").setAttribute("y2", y2);
      e.text.setAttribute("x", (x1 + x2) / 2);
      e.text.setAttribute("y", (y1 + y2) / 2 - 4);
    }
    for (const n of this.nodes) {
      n.el.setAttribute("transform", `translate(${n.x},${n.y})`);
    }
  }

  _loop() {
    if (this._raf) return;
    const step = () => {
      // A route change can detach the SVG while a drag is in flight: pointerup
      // then never reaches us, `_dragNode` stays set and the loop would keep
      // scheduling frames forever. Stop and drop the drag/pan state instead.
      if (!this.svg.isConnected) {
        this._raf = null;
        if (this._dragNode) { this._dragNode.fx = null; this._dragNode.fy = null; }
        this._dragNode = null; this._panFrom = null; this._down = null;
        this.svg.classList.remove("dragging");
        return;
      }
      this._tick(); this._tick();
      this._draw();
      const energy = this.nodes.reduce((s, n) => s + Math.abs(n.vx) + Math.abs(n.vy), 0);
      const dragging = this._dragNode != null;
      if (this._alpha <= 0 && energy < 0.5 && !dragging) {
        this._raf = null;
        return;
      }
      this._raf = requestAnimationFrame(step);
    };
    this._raf = requestAnimationFrame(step);
  }

  // ---------------- Interaction ----------------

  _clientToWorld(ev) {
    const pt = this.svg.createSVGPoint();
    pt.x = ev.clientX; pt.y = ev.clientY;
    const p = pt.matrixTransform(this.svg.getScreenCTM().inverse());
    return { x: (p.x - this.tx) / this.scale, y: (p.y - this.ty) / this.scale };
  }

  _bind() {
    const svg = this.svg;
    svg.addEventListener("pointerdown", (ev) => {
      svg.setPointerCapture(ev.pointerId);
      // setPointerCapture retargets pointerup/pointercancel to the SVG itself,
      // so remember where the gesture started: `up` resolves the node/edge
      // from this target instead of ev.target (which is always the svg).
      this._down = { x: ev.clientX, y: ev.clientY, moved: false, target: ev.target };
      const nodeG = ev.target.closest && ev.target.closest("[data-node]");
      if (nodeG) {
        const n = this.nodes.find((x) => x.id === nodeG.dataset.node);
        if (n) this._dragNode = n;
      } else {
        this._panFrom = { x: ev.clientX, y: ev.clientY, tx: this.tx, ty: this.ty };
        svg.classList.add("dragging");
      }
    });
    svg.addEventListener("pointermove", (ev) => {
      if (!this._down) return;
      if (Math.hypot(ev.clientX - this._down.x, ev.clientY - this._down.y) > 4) this._down.moved = true;
      if (this._dragNode) {
        const w = this._clientToWorld(ev);
        this._dragNode.fx = w.x; this._dragNode.fy = w.y;
        this.reheat();
      } else if (this._panFrom) {
        // Convert the pointer delta from client pixels into viewBox user
        // units through the live screen CTM: the viewBox is letterboxed by
        // preserveAspectRatio, so a single width ratio drifts. The translation
        // component cancels out by transforming both endpoints.
        const ctm = this.svg.getScreenCTM();
        if (ctm) {
          const inv = ctm.inverse();
          const p0 = this.svg.createSVGPoint();
          p0.x = this._panFrom.x; p0.y = this._panFrom.y;
          const p1 = this.svg.createSVGPoint();
          p1.x = ev.clientX; p1.y = ev.clientY;
          const a = p0.matrixTransform(inv), b = p1.matrixTransform(inv);
          this.tx = this._panFrom.tx + (b.x - a.x);
          this.ty = this._panFrom.ty + (b.y - a.y);
          this._applyTransform();
        }
      }
    });
    const up = (ev) => {
      const down = this._down;
      const wasMoved = down && down.moved;
      const downTarget = down && down.target;
      const dragNode = this._dragNode;
      if (dragNode) { dragNode.fx = null; dragNode.fy = null; this.reheat(); }
      this._dragNode = null; this._panFrom = null; this._down = null;
      this.svg.classList.remove("dragging");
      // A cancelled gesture (touch scroll, browser takeover) must not change
      // the selection; only a real pointerup resolves click targets.
      if (ev.type === "pointercancel") return;
      if (wasMoved || !downTarget || !downTarget.closest) return;
      const nodeG = downTarget.closest("[data-node]");
      if (nodeG) { this.select("node", nodeG.dataset.node); return; }
      const edgeG = downTarget.closest("[data-edge]");
      if (edgeG) { this.select("edge", edgeG.dataset.edge); return; }
      this.select(null, null);
    };
    svg.addEventListener("pointerup", up);
    svg.addEventListener("pointercancel", up);
    svg.addEventListener("wheel", (ev) => {
      ev.preventDefault();
      const old = this.scale;
      this.scale = Math.max(0.3, Math.min(3, this.scale * (ev.deltaY < 0 ? 1.12 : 0.89)));
      const rect = this.svg.getBoundingClientRect();
      const pt = this.svg.createSVGPoint();
      pt.x = ev.clientX; pt.y = ev.clientY;
      const p = pt.matrixTransform(this.svg.getScreenCTM().inverse());
      this.tx = p.x - (p.x - this.tx) * (this.scale / old);
      this.ty = p.y - (p.y - this.ty) * (this.scale / old);
      this._applyTransform();
      void rect;
    }, { passive: false });
  }

  _applyTransform() {
    this.gRoot.setAttribute("transform", `translate(${this.tx},${this.ty}) scale(${this.scale})`);
  }
}

/* ============================================================
   页面主体：六个 tab（总览/事实簇/原始事实/用户画像/关系图谱/注入语料）
   hash 路由（#overview 等），刷新保位
   ============================================================ */
// Shared category options for the cluster/fact filters: every active
// dimension must be selectable in both tabs, so the list lives in one place
// (preference/commitment included).
const _CATEGORIES = [
  ["", "全部"], ["identity", "identity"], ["stable", "stable"],
  ["preference", "preference"], ["commitment", "commitment"],
  ["interaction", "interaction"], ["naming", "naming"],
  ["recent", "recent"], ["uncertain", "uncertain"],
];

const Page = {
  async boot() {
    addEventListener("hashchange", () => this.route());
    this.route();
  },

  async route() {
    const content = document.getElementById("content");
    const tab = (location.hash.replace(/^#\/?/, "") || "overview").split("/")[0];
    // The rail is rebuilt on every route; if the user activated a tab (keyboard
    // or click), restore focus to the now-active tab so Tab navigation does not
    // restart from the top of the document.
    const keepTabFocus = !!(document.activeElement
      && document.activeElement.classList
      && document.activeElement.classList.contains("tab"));
    content.innerHTML = "";
    content.appendChild(this.el("div", { class: "toolbar" }, [
      this.el("div", { class: "tb-cap" }, ["MEMORY ATLAS // RECALL ENGINE"]),
    ]));
    const tabs = this.el("div", { class: "tab-row" });
    // Orbit nav: one star per tab (.tab-node decoration + label), visual only
    const TAB_ICONS = { overview: "✦", clusters: "◈", facts: "✧", profile: "☾", graph: "❋", summaries: "✶", settings: "◉" };
    for (const [key, label] of [["overview", "总览"], ["clusters", "事实簇（确信度）"], ["facts", "原始事实"], ["profile", "用户画像"], ["graph", "关系图谱"], ["summaries", "注入语料"], ["settings", "设置"]]) {
      // Real buttons (not divs) so the nav is keyboard reachable: Tab to focus,
      // Enter/Space activates. UA button chrome is reset in memory.css (.tab).
      tabs.appendChild(this.el("button", {
        type: "button",
        class: "tab" + (tab === key ? " active" : ""),
        "aria-current": tab === key ? "page" : null,
        onclick: () => (location.hash = "#" + key),
      }, [
        this.el("i", { class: "tab-node", "aria-hidden": "true" }, [TAB_ICONS[key] || "✦"]),
        this.el("span", { class: "tab-txt" }, [label]),
      ]));
    }
    content.appendChild(tabs);
    if (keepTabFocus) {
      const activeTab = tabs.querySelector(".tab.active");
      if (activeTab) activeTab.focus();
    }

    const body = this.el("div");
    content.appendChild(body);
    // Top-level error card retries: failures used to leave only dead text, so a
    // refresh meant a full F5
    const renderTab = async () => {
      body.innerHTML = "";
      try {
        if (tab === "overview") await this.renderOverview(body);
        else if (tab === "clusters") await this.renderClusters(body);
        else if (tab === "facts") await this.renderFacts(body);
        else if (tab === "profile") await this.renderProfile(body);
        else if (tab === "graph") await this.renderGraph(body);
        else if (tab === "summaries") await this.renderSummaries(body);
        else if (tab === "settings") await this.renderSettings(body);
      } catch (e) {
        body.appendChild(this.el("div", { class: "card" }, [
          this.el("div", { class: "empty" }, [UI.svg("CircleCloseFilled"), e.message]),
          this.el("div", { style: "text-align:center;margin-top:8px" }, [
            this.el("button", { class: "btn small ghost", onclick: () => renderTab() }, ["重试"]),
          ]),
        ]));
      }
    };
    await renderTab();
  },

  el(...args) { return UI.el(...args); },

  // ---------------- Overview ----------------
  async renderOverview(el) {
    const d = await UI.api("/overview");
    const stat = (num, lbl) => this.el("div", { class: "dash-pod" }, [
      this.el("div", { class: "lbl" }, [lbl]),
      this.el("div", { class: "num" }, [String(num)]),
    ]);
    el.appendChild(this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "记忆库总览"]),
      this.el("div", { class: "card-grid" }, [
        stat(d.summaries, "对话摘要（recall 语料）"),
        stat(d.facts_raw, "原始事实"),
        stat(d.facts_pending, "待处理事实"),
        stat(d.users_with_clusters, "有簇用户"),
      ]),
      this.el("div", { style: "height:12px" }),
      this.el("div", { class: "card-grid" }, [
        stat(d.clusters_active, "活跃簇"),
        stat(d.clusters_profiled, "已入画像簇"),
        stat(d.clusters_pending, "待定簇"),
        stat(d.profile_rows, "画像投影行"),
      ]),
      d.summaries_unsummarized ? this.el("p", { class: "muted" }, [`${d.summaries_unsummarized} 条原文待补编码（暂不参与召回）`]) : null,
      d.summaries_no_vec ? this.el("p", { class: "muted" }, [`${d.summaries_no_vec} 条摘要向量待补算`]) : null,
    ]));
    if (d.kv && d.kv.length) {
      el.appendChild(this.el("div", { class: "card" }, [
        this.el("h3", {}, [UI.paw(), "状态机持久化状态（衰减等）"]),
        // Known keys render as friendly pods; unknown future kv keys fall
        // through to a generic pod so nothing silently disappears.
        this.el("div", { class: "card-grid" },
          d.kv.map((k) => this.kvItem(k.key, k.value, k.updated_at))),
      ]));
    }
  },

  // kv state-machine field -> friendly card (semantics match the classic page)
  kvMeta(key) {
    return ({
      adjudicate_skip_state: { label: "裁定遍 · 毒丸行名单", kind: "skip" },
      reencode_skip_state: { label: "补编码遍 · 毒丸行名单", kind: "skip" },
      last_decay_at: { label: "上次分数衰减/降级", kind: "date" },
      relation_audit_edge_id: { label: "关系边审计水位", kind: "audit-watermark" },
      relation_audit_fail_count: { label: "审计连续失败计数", kind: "audit-fails" },
      relation_audit_fail_mark: { label: "审计跳批标记", kind: "raw" },
      alias_sync_watermark: { label: "别名同步水位", kind: "alias-watermark" },
      // Restored from the old page: these markers are still written by the
      // backend-migration / kira-memory import paths.
      _backend_migration_done: { label: "后端数据迁移", kind: "migration" },
      kira_memory_import_last_run: { label: "Kira 记忆数据迁入", kind: "kira_memory" },
      kira_memory_host_import_last_run: { label: "Kira 宿主库迁入", kind: "kira_memory" },
    })[key];
  },

  kvItem(key, value, updatedAt) {
    const meta = this.kvMeta(key);
    const raw = String(value);
    // Tooltip keeps the exact raw payload for debugging
    const item = this.el("div", { class: "dash-pod kv-item", title: key + " = " + raw });
    item.appendChild(this.el("div", { class: "lbl" }, [meta ? meta.label : key]));
    const val = this.el("div", { class: "val" });
    let tone = "";
    if (!meta) {
      val.append(raw.length > 80 ? raw.slice(0, 80) + "…" : raw);
    } else if (meta.kind === "skip") {
      let sum = null;
      try { sum = JSON.parse(raw); } catch (e) { /* fallthrough */ }
      if (sum && typeof sum === "object" && !Array.isArray(sum)) {
        const entries = Object.entries(sum);
        if (!entries.length) {
          tone = "ok";
          val.append(UI.badge("正常", "ok"), "无卡住行");
        } else {
          tone = "warn";
          const head = entries.slice(0, 3).map(([id, n]) => "#" + id + "（" + n + " 次）").join("、");
          val.append(UI.badge("注意", "warn"),
            entries.length + " 行被跳过：" + head + (entries.length > 3 ? " …" : ""));
        }
      } else {
        tone = "warn";
        val.append(UI.badge("注意", "warn"), "值无法解析：" + raw);
      }
    } else if (meta.kind === "date") {
      const t = Date.parse(raw);
      if (Number.isNaN(t)) {
        val.append(raw);
      } else {
        const days = Math.floor((Date.now() - t) / 86400000);
        val.append(UI.fmtTime(raw) + "（" + (days <= 0 ? "今天" : days + " 天前") + "）");
      }
    } else if (meta.kind === "audit-watermark") {
      tone = "ok";
      const n = parseInt(raw, 10);
      val.append(UI.badge("正常", "ok"), Number.isNaN(n) ? raw : "已审至边 id ≤ " + n);
    } else if (meta.kind === "alias-watermark") {
      tone = "ok";
      const n = parseInt(raw, 10);
      val.append(UI.badge("正常", "ok"), Number.isNaN(n) ? raw : "已对账宿主消息 id ≤ " + n);
    } else if (meta.kind === "audit-fails") {
      const n = parseInt(raw, 10) || 0;
      if (n > 0) tone = "warn";
      val.append(UI.badge(n > 0 ? "注意" : "正常", n > 0 ? "warn" : "ok"),
        n > 0 ? "连续失败 " + n + " / 3（满 3 跳批）" : "无连续失败");
    } else if (meta.kind === "migration") {
      // Restored from the old page (backend data migration marker).
      let m; try { m = JSON.parse(raw); } catch (e) { m = null; }
      if (m && m.migrated_at) {
        const total = m.copied && typeof m.copied === "object"
          ? Object.values(m.copied).reduce((a, b) => a + (Number(b) || 0), 0) : null;
        val.append(UI.badge("已完成", "ok"),
          "已从 " + (m.source || "?") + " 迁入" + (total != null ? " " + total + " 行" : "")
          + "（" + UI.fmtTime(m.migrated_at) + "）");
      } else { tone = "warn"; val.append(UI.badge("注意", "warn"), "值无法解析：" + raw); }
    } else if (meta.kind === "kira_memory") {
      // Restored from the old page (kira-memory import last-run markers).
      let m; try { m = JSON.parse(raw); } catch (e) { m = null; }
      if (m && m.finished_at) {
        const parts = [];
        if (m.summary_rows != null) parts.push("摘要 +" + m.summary_rows);
        if (m.fact_rows != null) parts.push("事实 +" + m.fact_rows);
        if (m.aliases_upserted != null) parts.push("别名 +" + m.aliases_upserted);
        val.append(UI.badge("已完成", "ok"),
          (parts.length ? parts.join(" / ") : "完成") + "（" + UI.fmtTime(m.finished_at) + "）");
      } else { tone = "warn"; val.append(UI.badge("注意", "warn"), "值无法解析：" + raw); }
    } else {
      val.append(raw.length > 80 ? raw.slice(0, 80) + "…" : raw);
    }
    if (tone) item.classList.add(tone);
    item.appendChild(val);
    item.appendChild(this.el("div", { class: "raw" }, [UI.fmtTime(updatedAt) + " 更新"]));
    return item;
  },

  // ---------------- Shared filters ----------------
  filterBar(fields, onSearch) {
    const inputs = {};
    const bar = this.el("div", { class: "filter-row" }, fields.map(([key, label, ph, options]) => {
      let node;
      if (options) {
        node = this.el("select");
        for (const [v, l] of options) node.appendChild(this.el("option", { value: v }, [l]));
      } else {
        node = this.el("input", { type: "text", placeholder: ph || "" });
      }
      inputs[key] = node;
      return this.el("div", { class: "f" }, [this.el("label", {}, [label]), node]);
    }).concat([
      this.el("button", {
        class: "btn small",
        onclick: () => onSearch(Object.fromEntries(Object.entries(inputs).map(([k, n]) => [k, n.value.trim()]))),
      }, [UI.svg("Search"), "查询"]),
    ]));
    return bar;
  },

  tableCard(title, head, rows, pagerNode) {
    return this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), title]),
      rows.length
        ? this.el("table", { class: "tbl" }, [
            this.el("thead", {}, [this.el("tr", {}, head.map((t) => this.el("th", {}, [t])))]),
            this.el("tbody", {}, rows),
          ])
        : this.el("div", { class: "empty" }, ["没有打捞到匹配的记忆残片"]),
      pagerNode,
    ]);
  },

  // ---------------- Fact clusters ----------------
  async renderClusters(el) {
    let query = { page: 1 };
    // Filter/paging requests may land out of order; only the newest one may
    // paint (same pattern as renderFacts/renderSummaries).
    let reqSeq = 0;
    const listBox = this.el("div");
    el.appendChild(this.filterBar([
      ["user_id", "用户 ID", "裸 uid"],
      ["session_id", "会话 ID", "按证据键前缀"],
      ["category", "分类", "", _CATEGORIES],
      ["status", "状态", "", [["", "全部"], ["active", "active"], ["profiled", "profiled"], ["pending_uncertain", "pending_uncertain"], ["replaced", "replaced"], ["dead", "dead"]]],
      ["q", "内容包含", "陈述关键词"],
    ], (vals) => { query = Object.assign({}, vals, { page: 1 }); refresh(); }));
    el.appendChild(listBox);

    // Scoring thresholds for the edit hint come from the live config so the
    // text matches the thresholds actually used for promotion/demotion
    // (0~score_cap, >= promote_threshold / < demote_threshold). Cached for the
    // tab lifetime; an unavailable config falls back to generic wording.
    let cfgValues = null;
    const loadCfgValues = async () => {
      if (cfgValues) return cfgValues;
      try { cfgValues = (await UI.api("/config")).values || {}; } catch (e) { cfgValues = {}; }
      return cfgValues;
    };

    const refresh = async () => {
      const seq = ++reqSeq;
      listBox.innerHTML = '<div class="loading">正在打捞记忆</div>';
      try {
        const p = new URLSearchParams(Object.assign({ size: 30 }, query));
        const d = await UI.api("/clusters?" + p.toString());
        if (seq !== reqSeq) return; // superseded by a newer filter/page request
        const statusColor = { active: "sky", profiled: "ok", pending_uncertain: "warn", replaced: "gray", dead: "bad" };
        const rows = d.items.map((c) => this.el("tr", {}, [
          this.el("td", { class: "mono" }, [String(c.id)]),
          this.el("td", {}, [
            this.el("div", { class: "mono" }, [c.user_id]),
            this.el("div", {}, [UI.badge(c.category, "pink")]),
          ]),
          this.el("td", { class: "wrap" }, [c.statement]),
          this.el("td", {}, [UI.scoreBar(c.score)]),
          this.el("td", {}, [UI.badge(c.status, statusColor[c.status] || "gray")]),
          this.el("td", {}, [`${c.evidence_count} 证据`]),
          this.el("td", {}, [UI.fmtTime(c.updated_at)]),
          this.el("td", {}, [
            this.el("button", { class: "btn small ghost", onclick: async () => this.editCluster(c, refresh, await loadCfgValues()) }, [UI.svg("EditPen"), "修正"]),
          ]),
        ]));
        listBox.innerHTML = "";
        listBox.appendChild(this.tableCard(
          `事实簇（共 ${d.total} 条，按确信度排序）`,
          ["ID", "用户/分类", "规范陈述", "确信度", "状态", "证据", "更新时间", "操作"],
          rows,
          UI.pager(d.total, d.page, d.size, (p2) => { query.page = p2; refresh(); })
        ));
      } catch (e) {
        // Paging/filter callers are fire-and-forget: surface the failure here
        // with a retry instead of leaving the list stuck on "loading".
        if (seq !== reqSeq) return;
        listBox.innerHTML = "";
        listBox.appendChild(UI.errorCard(e, refresh));
      }
    };
    await refresh();
  },

  editCluster(c, refresh, cfg = {}) {
    const num = (v, fb) => (typeof v === "number" && Number.isFinite(v) ? v : fb);
    const cap = num(cfg.score_cap, 10);
    const promote = num(cfg.promote_threshold, null);
    const demote = num(cfg.demote_threshold, null);
    // Use the deployment's real thresholds when available: a hardcoded
    // ">=10 / <4" hint lies as soon as score_cap/promote/demote are tuned.
    const scoreHint = promote !== null && demote !== null
      ? `确信度分数（0~${cap}，≥${promote} 进画像 / <${demote} 降级待定）`
      : `确信度分数（0~${cap}；达到晋升阈值进画像，低于降级阈值移出画像）`;
    const stmt = this.el("textarea", {}, [c.statement]);
    const score = this.el("input", { type: "number", step: "0.5", min: "0", max: String(cap), value: String(c.score) });
    const status = this.el("select");
    // replaced is a merge-pipeline-only tombstone: the Kira side forbids
    // manual move-out, so it is not selectable; when already replaced the
    // dropdown is locked, so a same-status submit is not read as a migration.
    const statusOpts = c.status === "replaced"
      ? ["replaced", "active", "profiled", "pending_uncertain", "dead"]
      : ["active", "profiled", "pending_uncertain", "dead"];
    for (const s of statusOpts) {
      status.appendChild(this.el("option", Object.assign({ value: s }, s === c.status ? { selected: "" } : {}), [s]));
    }
    if (c.status === "replaced") status.disabled = true;
    const closeM = UI.modal(`修正簇 #${c.id}（用户 ${c.user_id}）`, [
      this.el("div", { class: "cfg-field" }, [this.el("label", {}, ["规范陈述（改陈述会重置向量待重算）"]), stmt]),
      this.el("div", { class: "cfg-field" }, [this.el("label", {}, [scoreHint]), score]),
      this.el("div", { class: "cfg-field" }, [this.el("label", {}, ["状态（切出 profiled 会同步移除画像投影）"]), status]),
      this.el("p", { class: "muted" }, [`证据键：${(c.evidence_keys || []).join("、") || "无"}`]),
    ], [
      this.el("button", {
        class: "btn",
        onclick: async () => {
          try {
            // Empty or non-numeric score means "leave unchanged": an empty
            // string must not become Number("") === 0, which would silently
            // reset the score and demote the cluster out of the profile.
            const body = { canonical_statement: stmt.value };
            // Submit a status only when the user actually changed it. Echoing
            // the status captured when the dialog opened would, if the merge
            // pipeline promoted the cluster meanwhile, write that stale value
            // back as a real transition and drop the profile projection.
            if (status.value !== c.status) body.status = status.value;
            const rawScore = score.value.trim();
            const parsedScore = Number(rawScore);
            if (rawScore !== "" && !Number.isNaN(parsedScore)) body.score = parsedScore;
            await UI.api(`/clusters/${c.id}`, {
              method: "PUT",
              body: JSON.stringify(body),
            });
            UI.toast("簇已修正", "ok");
            closeM();
            refresh();
          } catch (e) { UI.toast("修正失败：" + e.message, "err"); }
        },
      }, [UI.svg("DocumentChecked"), "保存修正"]),
    ]);
  },

  // ---------------- Raw facts ----------------
  async renderFacts(el) {
    let query = { page: 1 };
    // Filter/paging requests may land out of order; only the newest one may
    // paint (same pattern as profileReqSeq).
    let reqSeq = 0;
    const listBox = this.el("div");
    el.appendChild(this.filterBar([
      ["user_id", "用户 ID", "裸 uid"],
      ["session_id", "会话 ID", ""],
      ["category", "分类", "", _CATEGORIES],
      ["confidence", "确信度", "", [["", "全部"], ["high", "high"], ["medium", "medium"]]],
      ["extracted", "处理状态", "", [["", "全部"], ["0", "待处理"], ["1", "已入簇"]]],
      ["q", "内容包含", "事实关键词"],
    ], (vals) => { query = Object.assign({}, vals, { page: 1 }); refresh(); }));
    el.appendChild(listBox);

    const refresh = async () => {
      const seq = ++reqSeq;
      listBox.innerHTML = '<div class="loading">正在打捞记忆</div>';
      try {
        const p = new URLSearchParams(Object.assign({ size: 30 }, query));
        const d = await UI.api("/facts?" + p.toString());
        if (seq !== reqSeq) return; // superseded by a newer filter/page request
        const rows = d.items.map((f) => this.el("tr", {}, [
          this.el("td", { class: "mono" }, [String(f.id)]),
          this.el("td", {}, [
            this.el("div", {}, [f.display_name || f.user_id]),
            this.el("div", { class: "muted mono", style: "font-size:11px" }, [f.user_id]),
          ]),
          this.el("td", {}, [UI.badge(f.category, "pink")]),
          this.el("td", { class: "wrap" }, [f.statement]),
          this.el("td", {}, [UI.badge(f.confidence, f.confidence === "high" ? "ok" : "warn")]),
          this.el("td", {}, [UI.badge(f.extracted ? "已入簇" : "待处理", f.extracted ? "gray" : "sky")]),
          this.el("td", { class: "mono", style: "font-size:11px" }, [f.session_id]),
          this.el("td", {}, [UI.fmtTime(f.occurred_at)]),
          this.el("td", {}, [
            this.el("button", {
              class: "btn small danger",
              "aria-label": `删除事实 #${f.id}`,
              title: `删除事实 #${f.id}`,
              onclick: () => UI.confirm("删除原始事实", `确定删除事实 #${f.id} 吗？${f.extracted ? "该事实已入簇，删除不影响簇本身。" : "该事实还未入簇，删除后不会再参与合并。"}`, async () => {
                try {
                  await UI.api(`/facts/${f.id}`, { method: "DELETE" });
                  UI.toast("事实已删除", "ok");
                  refresh();
                } catch (e) { UI.toast("删除失败：" + e.message, "err"); }
              }, "删除"),
            }, [UI.svg("Delete")]),
          ]),
        ]));
        listBox.innerHTML = "";
        listBox.appendChild(this.tableCard(
          `原始事实（共 ${d.total} 条）`,
          ["ID", "用户", "分类", "事实陈述", "确信度", "状态", "来源会话", "发生时间", "操作"],
          rows,
          UI.pager(d.total, d.page, d.size, (p2) => { query.page = p2; refresh(); })
        ));
      } catch (e) {
        // Paging/filter callers are fire-and-forget: surface the failure here with a retry.
        if (seq !== reqSeq) return;
        listBox.innerHTML = "";
        listBox.appendChild(UI.errorCard(e, refresh));
      }
    };
    await refresh();
  },

  // ---------------- User profile ----------------
  async renderProfile(el) {
    const pickerCard = this.el("div", { class: "card" });
    const profileCard = this.el("div");
    el.appendChild(pickerCard);
    el.appendChild(profileCard);

    let page = 1, kw = "";
    // Profile request bookkeeping: profileReqSeq discards stale responses, and
    // profileKey records which user the card currently shows so deleting that
    // user can invalidate the card (and any in-flight request for it).
    let profileReqSeq = 0;
    let profileKey = null;
    // User-list requests race the same way (search by keyword + paging):
    // only the newest response may paint, success or failure.
    let usersReqSeq = 0;
    const loadUsers = async () => {
      const seq = ++usersReqSeq;
      pickerCard.innerHTML = '<div class="loading">正在打捞记忆</div>';
      try {
        const d = await UI.api(`/users?page=${page}&size=30&q=${encodeURIComponent(kw)}`);
        if (seq !== usersReqSeq) return; // superseded by a newer search/page
        pickerCard.innerHTML = "";
        const search = this.el("div", { class: "filter-row" }, [
          this.el("div", { class: "f" }, [this.el("label", {}, ["搜索用户 ID"])]),
        ]);
        const inp = this.el("input", { type: "text", placeholder: "user_id", value: kw });
        search.querySelector(".f").appendChild(inp);
        search.appendChild(this.el("button", {
          class: "btn small",
          "aria-label": "搜索用户",
          title: "搜索用户",
          onclick: () => { kw = inp.value.trim(); page = 1; loadUsers(); },
        }, [UI.svg("Search")]));
        pickerCard.appendChild(this.el("h3", {}, [UI.paw(), `选择用户查看画像（共 ${d.total} 个有记忆用户）`]));
        pickerCard.appendChild(search);
        const rows = d.items.map((u) => {
          const openProfile = () => loadProfile(u.platform, u.user_id, u.display_name);
          // Keyboard entry mirrors the row click; the delete button inside
          // keeps its own keys (kbClick ignores bubbling inner events).
          return UI.kbClick(this.el("tr", { style: "cursor:pointer", onclick: openProfile }, [
            this.el("td", {}, [
              this.el("div", { style: "font-weight:700" }, [u.display_name || "（无名）"]),
              this.el("div", { class: "muted mono", style: "font-size:11px" }, [u.user_id]),
            ]),
            this.el("td", {}, [u.platform]),
            this.el("td", {}, u.active + u.profiled + u.pending === 0
              ? [UI.badge("未成簇", "gray")]
              : [UI.badge(`画像 ${u.profiled}`, u.profiled ? "ok" : "gray"), " ",
                UI.badge(`活跃 ${u.active}`, "sky"), " ",
                u.pending ? UI.badge(`待定 ${u.pending}`, "warn") : null]),
            this.el("td", {}, [UI.scoreBar(u.max_score)]),
            this.el("td", {}, [UI.fmtTime(u.updated_at)]),
            this.el("td", {}, [
              this.el("button", {
                class: "btn small danger",
                onclick: (ev) => {
                  // Row click opens the profile; the button click erases memory.
                  ev.stopPropagation();
                  UI.confirm("清除该用户的全部记忆",
                    `确定删除 ${u.platform}:${u.user_id} 的画像/事实簇/原始事实/别名与关系边吗？此操作不可恢复（对话摘要为会话维度、可能涉及他人，不随此级联删除）。`,
                    async () => {
                      try {
                        const r = await UI.api(`/users/${encodeURIComponent(u.platform)}/${encodeURIComponent(u.user_id)}`, { method: "DELETE" });
                        UI.toast(`已删除：簇 ${r.clusters} · 事实 ${r.facts} · 画像 ${r.profiles} · 别名 ${r.aliases} · 边 ${r.edges}`, "ok");
                        // Only drop the profile card when it belongs to the deleted
                        // user; bump the sequence so a response still in flight for
                        // that user cannot re-render the erased profile.
                        if (profileKey === `${u.platform}:${u.user_id}`) {
                          profileReqSeq++;
                          profileKey = null;
                          profileCard.innerHTML = "";
                          profileCard.appendChild(this.el("div", { class: "card" }, [
                            this.el("div", { class: "empty" }, ["该用户已被删除，画像不再可用"]),
                          ]));
                        }
                        loadUsers();
                      } catch (e) { UI.toast("删除失败：" + e.message, "err"); }
                    }, "删除");
                },
              }, ["清除记忆"]),
            ]),
          ]), openProfile);
        });
        pickerCard.appendChild(this.el("table", { class: "tbl" }, [
          this.el("thead", {}, [this.el("tr", {}, ["用户", "平台", "簇构成", "最高确信度", "最近更新", "操作"].map((t) => this.el("th", {}, [t])))]),
          this.el("tbody", {}, rows),
        ]));
        pickerCard.appendChild(UI.pager(d.total, d.page, d.size, (p) => { page = p; loadUsers(); }));
      } catch (e) {
        // Search/paging callers are fire-and-forget: surface the failure here with a retry.
        // A stale failure must not replace a newer list either.
        if (seq !== usersReqSeq) return;
        pickerCard.innerHTML = "";
        pickerCard.appendChild(UI.errorCard(e, loadUsers));
      }
    };

    const loadProfile = async (platform, userId, displayName) => {
      const seq = ++profileReqSeq;
      profileKey = `${platform}:${userId}`;
      profileCard.innerHTML = '<div class="card"><div class="loading">正在打捞记忆</div></div>';
      try {
        const d = await UI.api(`/profile/${encodeURIComponent(platform)}/${encodeURIComponent(userId)}`);
        // A newer click (or a deletion) superseded this request: drop the result.
        if (seq !== profileReqSeq) return;
        profileCard.innerHTML = "";
        const sectionNames = { identity: "基本信息", naming: "称呼偏好", stable: "已知事实", preference: "喜好偏好", commitment: "约定承诺", interaction: "互动偏好", recent: "近期动态" };
        const sectionCards = Object.entries(sectionNames).map(([cat, title]) =>
          this.el("div", { class: "card", style: "margin:0" }, [
            this.el("h3", {}, [title]),
            (d.sections[cat] && d.sections[cat].length)
              ? this.el("ul", { style: "margin:0;padding-left:18px" }, d.sections[cat].map((s) => this.el("li", {}, [s])))
              : this.el("div", { class: "muted" }, ["暂无"]),
          ])
        );
        sectionCards.push(this.el("div", { class: "card", style: "margin:0" }, [
          this.el("h3", {}, ["待定信息"]),
          d.uncertain.length
            ? this.el("ul", { style: "margin:0;padding-left:18px" }, d.uncertain.map((s) => this.el("li", {}, [s])))
            : this.el("div", { class: "muted" }, ["暂无"]),
        ]));
        profileCard.appendChild(this.el("div", { class: "card" }, [
          this.el("h3", {}, [UI.paw(), `${d.display_name} 的画像（与注入内容逐字一致）`]),
          d.injection_text
            ? this.el("div", { class: "pre-box" }, [d.injection_text])
            : this.el("div", { class: "empty" }, ["该用户暂无可注入的画像内容"]),
        ]));
        profileCard.appendChild(this.el("div", { class: "card-grid", style: "grid-template-columns:repeat(auto-fill,minmax(300px,1fr))" }, sectionCards));
        profileCard.scrollIntoView({ behavior: "smooth" });
      } catch (e) {
        // Row clicks are fire-and-forget: surface the failure here with a retry.
        if (seq !== profileReqSeq) return;
        profileCard.innerHTML = "";
        profileCard.appendChild(UI.errorCard(e, () => loadProfile(platform, userId, displayName)));
      }
    };

    await loadUsers();
  },

  // ---------------- Relation graph ----------------
  async renderGraph(el) {
    // Snapshot time taken from the server (HTTP Date header on the graph
    // response): a backfill that finishes after this moment is NOT reflected
    // in `d`, so its status must trigger a reload instead of being adopted as
    // the poll baseline. Comparing against the server clock avoids skew with
    // the browser clock.
    const { data: d, serverNow } = await UI.api("/relations/graph", { meta: true });
    const snapshotTs = Number.isFinite(serverNow) ? serverNow : null;

    // Stats + backfill cards
    const stat = (num, lbl) => this.el("div", { class: "dash-pod" }, [
      this.el("div", { class: "lbl" }, [lbl]),
      this.el("div", { class: "num" }, [String(num)]),
    ]);
    const bfStatus = this.el("div", { class: "muted", style: "font-size:12px" });
    const bfBar = this.el("div", { class: "bf-progress", style: "display:none" }, [
      this.el("i", { style: "width:0%" }),
    ]);
    const bfBtn = this.el("button", { class: "btn small", disabled: d.backfill_available ? null : "" }, [
      UI.paw(), "从已确认事实回填关系",
    ]);
    const bfFullBtn = this.el("button", { class: "btn small ghost", title: "忽略水位，对全部已确认簇重新提取（不计分，只刷新）", disabled: d.backfill_available ? null : "" }, [
      "全量重跑",
    ]);
    if (!d.backfill_available) bfStatus.textContent = "fast LLM 未配置，回填不可用";
    const startBackfill = (full) => {
      UI.api("/relations/backfill" + (full ? "?full=1" : ""), { method: "POST" })
        .then(() => { UI.toast(full ? "全量重跑已启动" : "回填已启动（每批一次 LLM 调用，完成后自动刷新）", "ok"); pollBackfill(); })
        .catch((e) => UI.toast("回填启动失败：" + e.message, "err"));
    };
    bfBtn.addEventListener("click", () => startBackfill(false));
    bfFullBtn.addEventListener("click", () => startBackfill(true));
    const reload = async () => {
      el.innerHTML = "";
      try {
        await this.renderGraph(el);
      } catch (e) {
        // A failed refresh used to leave the tab blank; surface it with a
        // retry instead. A route change tears `el` down, so only paint while
        // the container is still mounted.
        if (!el.isConnected) return;
        el.innerHTML = "";
        el.appendChild(UI.errorCard(e, reload));
      }
    };
    let pollTimer = null;
    // Full-page refresh only on new completions seen during this stay: the
    // first poll records finished_at as a baseline, otherwise every tab entry
    // would re-render and flicker the screen
    let seenFinished = null;
    let pollFails = 0;
    const pollBackfill = async () => {
      if (pollTimer) clearTimeout(pollTimer);
      let s;
      try { s = await UI.api("/relations/backfill/status"); }
      catch (e) {
        // Transient failures keep polling (a single 401/500 used to kill the
        // poller forever with a frozen progress bar and disabled buttons);
        // give up after 5 consecutive failures with the reason surfaced and
        // the controls restored, otherwise the buttons stay dead forever.
        if (++pollFails >= 5) {
          bfStatus.textContent = "回填状态获取失败：" + e.message;
          bfBtn.disabled = !d.backfill_available;
          bfFullBtn.disabled = !d.backfill_available;
          bfBar.style.display = "none";
          return;
        }
        pollTimer = setTimeout(pollBackfill, 3000);
        return;
      }
      pollFails = 0;
      if (!bfStatus.isConnected) return; // tab switched away, stop polling
      if (seenFinished === null) {
        // Both sides of this comparison are server timestamps: finished_at
        // and the graph response's Date header. A skewed browser clock can
        // therefore not fake (or hide) a new completion. When the header is
        // unavailable, adopt the status as the baseline; a completion missed
        // here still refreshes the view on the next real cycle.
        const finishedTs = s.finished_at ? Date.parse(s.finished_at) : NaN;
        // A completion strictly after our snapshot is a real new completion:
        // leaving seenFinished unset lets the branch below reload once, and
        // that finished_at then becomes this view's baseline.
        const completedAfterSnapshot = snapshotTs !== null && !s.running && s.relations > 0
          && !isNaN(finishedTs) && finishedTs > snapshotTs;
        if (s.running) seenFinished = "";
        else if (!completedAfterSnapshot) seenFinished = s.finished_at;
      }
      if (s.running) {
        bfBtn.disabled = true;
        bfFullBtn.disabled = true;
        bfBar.style.display = "";
        const pct = s.total ? Math.round((s.done / s.total) * 100) : 0;
        bfBar.firstChild.style.width = pct + "%";
        bfStatus.textContent = `回填进行中（${s.mode === "full" ? "全量" : "增量"}）：批次 ${s.done}/${s.total} · 已产出 ${s.relations} 条边`;
        pollTimer = setTimeout(pollBackfill, 1500);
      } else {
        bfBtn.disabled = !d.backfill_available;
        bfFullBtn.disabled = !d.backfill_available;
        bfBar.style.display = "none";
        bfStatus.textContent = s.finished_at
          ? (s.error
              ? `上次回填异常终止：${s.error}`
              : `上次回填（${s.mode === "full" ? "全量" : "增量"}）：${s.done} 批完成（失败 ${s.failed_batches}）· 产出 ${s.relations} 条边 · ${UI.fmtTime(s.finished_at)}`)
          : "";
        if (s.finished_at && !s.error && s.relations > 0 && s.finished_at !== seenFinished) {
          seenFinished = s.finished_at;
          reload();
          return;
        }
        // Idle: keep polling so a backfill started/completed in another tab is
        // still discovered (this is the "waiting for backfill" contract).
        // isConnected stops the chain once this tab was switched away.
        if (bfStatus.isConnected) pollTimer = setTimeout(pollBackfill, 5000);
      }
    };
    el.appendChild(this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "关系图谱（memory_entity_edge）"]),
      this.el("div", { class: "card-grid" }, [
        stat(d.stats.nodes, "实体节点"),
        stat(d.stats.edges, "关系边"),
        stat(d.stats.edges_active, "active（P3 可注入）"),
        stat(d.stats.edges_pending, "pending（bot 边待双证据）"),
        stat(d.stats.edges_superseded, "superseded（墓碑，不注入）"),
      ]),
      this.el("div", { style: "height:10px" }),
      this.el("div", { class: "graph-bf-row" }, [bfBtn, bfFullBtn, bfStatus]),
      bfBar,
    ]));
    if (!d.stats.edges) {
      el.appendChild(this.el("div", { class: "card" }, [
        this.el("div", { class: "empty" }, ["暂无边数据：开启 P2 关系提取开关积累增量，或点上方按钮从已确认事实回填存量。"]),
      ]));
      // Poll even with an empty edge store: a backfill started elsewhere
      // (another tab or session) still needs a refresh (an early return would
      // park the poller before the backfill starts)
      pollBackfill();
      return;
    }

    // Graph + sidebar
    const edgeStatusColor = { active: "ok", pending: "warn", superseded: "gray" };
    const side = this.el("div", { class: "graph-side" });
    const graphHost = this.el("div", { class: "graph-box" });
    const chkActive = this.el("input", { type: "checkbox" });
    const chkPending = this.el("input", { type: "checkbox" });
    const chkSuperseded = this.el("input", { type: "checkbox" });
    chkActive.checked = true; chkPending.checked = true; chkSuperseded.checked = true;
    const renderDetail = (sel) => {
      side.innerHTML = "";
      if (!sel) {
        side.appendChild(this.el("div", { class: "card" }, [
          this.el("h3", {}, ["图例与操作"]),
          this.el("div", { class: "graph-legend" }, [
            this.el("span", {}, [this.el("i", { style: "background:var(--sky)" }), "成员节点（大小=边数）"]),
            this.el("span", {}, [this.el("i", { style: "background:var(--pink)" }), "bot 节点"]),
            this.el("span", {}, [this.el("i", { style: "background:var(--ink-soft)" }), "active 边"]),
            this.el("span", {}, [this.el("i", { style: "background:var(--ink-soft);opacity:.4" }), "pending 边（虚线，bot 边待双证据）"]),
            this.el("span", {}, [this.el("i", { style: "background:var(--ink-soft);opacity:.25" }), "superseded 边（点线，墓碑不注入）"]),
          ]),
          this.el("p", { class: "muted", style: "font-size:12px" }, [
            "拖拽节点调整布局，空白处拖拽平移，滚轮缩放；点击节点/边查看详情（点击空白取消）。",
          ]),
        ]));
        return;
      }
      if (sel.type === "node") {
        const n = d.nodes.find((x) => x.id === sel.id);
        if (!n) return;
        const conn = d.edges.filter((e) => e.subject === n.id || e.object === n.id);
        side.appendChild(this.el("div", { class: "card" }, [
          this.el("h3", {}, [`${n.is_bot ? "🤖 " : ""}${n.name}`]),
          this.el("div", { class: "mono muted", style: "font-size:11px" }, [`${n.platform || "-"} · ${n.uid}`]),
          this.el("p", {}, [`${n.degree} 条关系边`]),
          conn.length
            ? this.el("ul", { style: "margin:8px 0 0;padding-left:16px" }, conn.map((e) => {
                const other = e.subject === n.id ? e.object_name : e.subject_name;
                const arrow = e.subject === n.id ? "→" : "←";
                return UI.kbClick(this.el("li", { style: "cursor:pointer;font-size:13px", onclick: () => fg.select("edge", String(e.id)) }, [
                  `${arrow} ${other}（${e.label}${e.status !== "active" ? " · " + e.status : ""}）`,
                ]), () => fg.select("edge", String(e.id)));
              }))
            : null,
        ]));
      } else {
        const e = d.edges.find((x) => String(x.id) === String(sel.id));
        if (!e) return;
        // Status fix (tombstone guard: once superseded, new evidence never
        // auto-reactivates) + hard delete
        const statusSel = this.el("select");
        for (const s of ["active", "pending", "superseded"]) {
          statusSel.appendChild(this.el("option", Object.assign({ value: s }, s === e.status ? { selected: "" } : {}), [s]));
        }
        const saveBtn = this.el("button", { class: "btn small" }, ["保存状态"]);
        saveBtn.addEventListener("click", async () => {
          try {
            await UI.api(`/relations/edges/${e.id}`, {
              method: "PUT",
              body: JSON.stringify({ status: statusSel.value }),
            });
            UI.toast("边状态已修正", "ok");
            reload();
          } catch (err) { UI.toast("修正失败：" + err.message, "err"); }
        });
        const delBtn = this.el("button", { class: "btn small danger" }, ["删除边"]);
        delBtn.addEventListener("click", () => UI.confirm(
          "删除关系边",
          `确定物理删除边 #${e.id}（${e.subject_name || "?"}→${e.object_name || "?"} ${e.label}）吗？日常下线建议改状态为 superseded（保留可回溯）。`,
          async () => {
            try {
              await UI.api(`/relations/edges/${e.id}`, { method: "DELETE" });
              UI.toast("关系边已删除", "ok");
              reload();
            } catch (err) { UI.toast("删除失败：" + err.message, "err"); }
          },
          "删除"
        ));
        side.appendChild(this.el("div", { class: "card" }, [
          this.el("h3", {}, [`${e.subject_name || "?"} → ${e.object_name || "?"}`]),
          this.el("p", {}, [UI.badge(e.label, "pink"), " ", UI.badge(e.status, edgeStatusColor[e.status] || "gray"), " ", UI.badge(e.confidence, e.confidence === "high" ? "ok" : "gray")]),
          this.el("div", { class: "pre-box" }, [e.statement]),
          this.el("p", { class: "muted", style: "font-size:12px" }, [
            `${e.evidence_count} 证据 · 最近 ${UI.fmtTime(e.last_seen)} · 首次 ${UI.fmtTime(e.occurred_at)}`,
          ]),
          e.status === "superseded" && e.supersede_reason
            ? this.el("p", { class: "muted", style: "font-size:12px" }, [
                `降级原因：${e.supersede_reason}${e.superseded_at ? " · " + UI.fmtTime(e.superseded_at) : ""}`,
              ])
            : null,
          this.el("div", { class: "mono muted", style: "font-size:11px" }, [`${e.subject} → ${e.object}`]),
          this.el("div", { style: "display:flex;gap:8px;align-items:center;margin-top:10px" }, [
            statusSel, saveBtn, delBtn,
          ]),
        ]));
      }
    };
    const fg = new ForceGraph(graphHost, { onSelect: renderDetail });
    fg.setData(d.nodes, d.edges);
    renderDetail(null);
    const applyFilter = () => fg.setFilter(chkActive.checked, chkPending.checked, chkSuperseded.checked);
    chkActive.addEventListener("change", applyFilter);
    chkPending.addEventListener("change", applyFilter);
    chkSuperseded.addEventListener("change", applyFilter);
    el.appendChild(this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "动态图谱"]),
      this.el("div", { class: "graph-toolbar" }, [
        this.el("label", { class: "graph-chk" }, [chkActive, "active 边"]),
        this.el("label", { class: "graph-chk" }, [chkPending, "pending 边"]),
        this.el("label", { class: "graph-chk" }, [chkSuperseded, "superseded 边"]),
        this.el("button", { class: "btn small ghost", onclick: () => fg.resetView() }, ["重置视图"]),
        this.el("span", { class: "muted", style: "font-size:12px;margin-left:auto" }, [
          d.stats.edges_shown < d.stats.edges
            ? `${d.stats.nodes} 节点 · 仅展示 ${d.stats.edges_shown}/${d.stats.edges} 边`
            : `${d.stats.nodes} 节点 · ${d.stats.edges} 边`,
        ]),
      ]),
      this.el("div", { class: "graph-wrap" }, [graphHost, side]),
    ]));

    // Edge table (client-side filter, row click links the selection)
    const listBox = this.el("div");
    const q = this.el("input", { type: "text", placeholder: "label / 名字 / uid / 陈述关键词" });
    const refreshTable = () => {
      const kw = q.value.trim().toLowerCase();
      const rows = d.edges
        .filter((e) => !kw || [e.label, e.statement, e.subject_name, e.object_name, e.subject, e.object]
          .some((v) => String(v || "").toLowerCase().includes(kw)))
        .map((e) => {
          const openEdge = () => {
            fg.select("edge", String(e.id));
            graphHost.scrollIntoView({ behavior: "smooth", block: "center" });
          };
          // Row click and keyboard activation share one action.
          return UI.kbClick(this.el("tr", { style: "cursor:pointer", onclick: openEdge }, [
            this.el("td", {}, [UI.badge(e.label, "pink")]),
            this.el("td", {}, [`${e.subject_name || "?"} → ${e.object_name || "?"}`]),
            this.el("td", {}, [UI.badge(e.status, edgeStatusColor[e.status] || "gray")]),
            this.el("td", {}, [`${e.evidence_count} 证据`]),
            this.el("td", { class: "wrap" }, [e.statement]),
            this.el("td", {}, [UI.fmtTime(e.last_seen)]),
            // Superseded edges carry the downgrade timestamp (same source
            // field the detail panel uses; blank for live edges).
            this.el("td", {}, [UI.fmtTime(e.superseded_at)]),
          ]), openEdge);
        });
      listBox.innerHTML = "";
      listBox.appendChild(this.tableCard(
        d.stats.edges_shown < d.stats.edges
          ? `关系边（共 ${d.stats.edges} 条，按证据数排序，仅展示前 ${d.stats.edges_shown} 条）`
          : `关系边（共 ${d.stats.edges} 条，按证据数排序）`,
        ["label", "端点", "状态", "证据", "陈述", "最近证据", "降级时间"],
        rows
      ));
    };
    q.addEventListener("input", refreshTable);
    el.appendChild(this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "关系边清单"]),
      this.el("div", { class: "filter-row" }, [this.el("div", { class: "f" }, [this.el("label", {}, ["过滤"]), q])]),
      listBox,
    ]));
    refreshTable();
    pollBackfill();
  },

  // ---------------- Injected corpus (summaries) ----------------
  async renderSummaries(el) {
    let query = { page: 1 };
    // Filter/paging requests may land out of order; only the newest one may
    // paint (same pattern as profileReqSeq).
    let reqSeq = 0;
    const listBox = this.el("div");
    // The encode card reports the end of a merge cycle so the rows stop
    // showing「待补编码」without a manual refresh. The callback is quiet: an
    // automatic background sync must not flash the loading placeholder.
    el.appendChild(this.encodeCard(() => { if (listBox.isConnected) refresh(true); }));
    el.appendChild(this.filterBar([
      ["session_id", "会话 ID", ""],
      ["user_id", "触发用户", "裸 uid"],
      ["kind", "类型", "", [["", "全部"], ["chat_summary", "chat_summary"], ["bot_self", "bot_self"]]],
      ["q", "内容包含", "摘要关键词"],
    ], (vals) => { query = Object.assign({}, vals, { page: 1 }); refresh(); }));
    el.appendChild(listBox);

    const refresh = async (quiet = false) => {
      const seq = ++reqSeq;
      // A quiet refresh (automatic post-cycle sync) keeps the rows already on
      // screen instead of flashing the loading placeholder.
      if (!quiet || !listBox.firstChild) listBox.innerHTML = '<div class="loading">正在打捞记忆</div>';
      try {
        const p = new URLSearchParams(Object.assign({ size: 20 }, query));
        const d = await UI.api("/summaries?" + p.toString());
        if (seq !== reqSeq) return; // superseded by a newer filter/page request
        const rows = d.items.map((s) => this.el("tr", {}, [
          this.el("td", { class: "mono" }, [String(s.id)]),
          this.el("td", { class: "mono", style: "font-size:11px" }, [s.session_id]),
          this.el("td", {}, [UI.badge(s.kind, s.kind === "bot_self" ? "pink" : "sky")]),
          this.el("td", { class: "wrap", style: "max-width:520px" }, [s.content]),
          this.el("td", {}, [UI.fmtTime(s.occurred_at)]),
          // Priority: un-encoded rows first ("awaiting encode", not in
          // recall yet), then vector state -- restored from the old page
          // (a raw row shown as "awaiting vector" misleads recall-gap
          // debugging into looking at the wrong pipeline stage).
          this.el("td", {}, [
            s.unsummarized
              ? UI.badge("待补编码", "warn")
              : (s.has_embedding ? UI.badge("已向量化", "ok") : UI.badge("待补算", "warn")),
          ]),
          this.el("td", { style: "white-space:nowrap" }, [
            this.el("button", {
              class: "btn small ghost",
              "aria-label": `编辑摘要 #${s.id}`,
              title: `编辑摘要 #${s.id}`,
              onclick: () => this.editSummary(s, refresh),
            }, [UI.svg("EditPen")]),
            " ",
            this.el("button", {
              class: "btn small danger",
              "aria-label": `删除摘要 #${s.id}`,
              title: `删除摘要 #${s.id}`,
              onclick: () => UI.confirm("删除摘要", "删除后该轮内容不再参与 recall，确定吗？", async () => {
                try {
                  await UI.api(`/summaries/${s.id}`, { method: "DELETE" });
                  UI.toast("摘要已删除", "ok");
                  refresh();
                } catch (e) { UI.toast("删除失败：" + e.message, "err"); }
              }, "删除"),
            }, [UI.svg("Delete")]),
          ]),
        ]));
        listBox.innerHTML = "";
        listBox.appendChild(this.tableCard(
          `对话摘要 = recall 注入语料（共 ${d.total} 条）`,
          ["ID", "会话", "类型", "内容", "发生时间", "向量", "操作"],
          rows,
          UI.pager(d.total, d.page, d.size, (p2) => { query.page = p2; refresh(); })
        ));
      } catch (e) {
        // Paging/filter callers are fire-and-forget: surface the failure here with a retry.
        if (seq !== reqSeq) return;
        listBox.innerHTML = "";
        listBox.appendChild(UI.errorCard(e, () => refresh()));
      }
    };
    await refresh();
  },

  editSummary(s, refresh) {
    const ta = this.el("textarea", { class: "code", style: "min-height:180px" }, [s.content]);
    const closeM = UI.modal(`修正摘要 #${s.id}`, [
      this.el("p", { class: "muted" }, ["修改内容会把向量置空，由补算任务自动重算（约 5 分钟内生效）。"]),
      ta,
    ], [
      this.el("button", {
        class: "btn",
        onclick: async () => {
          try {
            await UI.api(`/summaries/${s.id}`, { method: "PUT", body: JSON.stringify({ content: ta.value }) });
            UI.toast("摘要已修正，向量待重算", "ok");
            closeM();
            refresh();
          } catch (e) { UI.toast("修正失败：" + e.message, "err"); }
        },
      }, [UI.svg("DocumentChecked"), "保存修正"]),
    ]);
  },

  // ---------------- Manual encode kick (wakes the merge loop once) ----------------
  // The merge loop stays the only runner; the button just resolves its
  // inter-cycle wait early. Polling keeps the backlog count and cycle state
  // fresh; the button enables only after a successful status probe.
  encodeCard(onCycleEnd) {
    const encBtn = this.el("button", { class: "btn small", disabled: "" }, ["立即执行补编码"]);
    const encStatus = this.el("span", { class: "muted", style: "font-size:12px" });
    // Shown only after the status probe gives up: explicit retry instead of a
    // dead card.
    const encRetry = this.el("button", { class: "btn small ghost", style: "display:none" }, ["重试状态"]);
    let encTimer = null;
    let encFails = 0;
    // Set on every status that reports a running cycle; the transition to
    // idle marks "a cycle finished while this view was open".
    let cycleRan = false;
    // finished_at of the last cycle already reflected by the list. A change
    // detects cycles that started and ended between two idle polls.
    let seenCycleInit = false;
    let seenCycle = null;
    const pollEncode = async () => {
      if (encTimer) clearTimeout(encTimer);
      let s;
      try { s = await UI.api("/encode/backfill/status"); }
      catch (e) {
        if (++encFails >= 5) {
          // Give up polling but keep the card usable: restore the kick button
          // and expose an explicit retry instead of freezing the status line.
          encStatus.textContent = "补编码状态获取失败：" + e.message;
          encBtn.disabled = false;
          encRetry.style.display = "";
          return;
        }
        encTimer = setTimeout(pollEncode, 5000);
        return;
      }
      encFails = 0;
      encRetry.style.display = "none";
      if (!encStatus.isConnected) return; // tab switched away, stop polling
      if (!s.available) {
        encBtn.disabled = true;
        encStatus.textContent = "合并管线未启用（fast LLM 未配置），补编码不可用";
        return;
      }
      const backlog = (s.backlog === null || s.backlog === undefined)
        ? "未知" : (s.backlog_capped ? "1000+" : String(s.backlog));
      if (s.cycle_running) {
        cycleRan = true;
        encBtn.disabled = true;
        encStatus.textContent = `补编码进行中：待补编码 ${backlog} 条（周期完成自动刷新）`;
        encTimer = setTimeout(pollEncode, 3000);
      } else {
        encBtn.disabled = false;
        const last = s.last_cycle
          ? ` · 上轮周期 ${UI.fmtTime(s.last_cycle.finished_at)} 完成，重编码 ${(s.last_cycle.stats && s.last_cycle.stats.reencoded) || 0} 条`
          : "";
        encStatus.textContent = `待补编码 ${backlog} 条` + last;
        // A finished cycle means rows still shown as「待补编码」may have been
        // encoded meanwhile, so refresh the list (the card promised this).
        // Detected via the observed running→idle transition, or via a new
        // last_cycle.finished_at for cycles that started and ended between
        // two idle polls.
        const curCycle = (s.last_cycle && s.last_cycle.finished_at) || null;
        let ended = cycleRan;
        cycleRan = false;
        // The first idle observation establishes the baseline but still
        // refreshes once: the list was painted independently of this status,
        // so a cycle that ended just before mount would otherwise stay stale
        // until the next change. The list's request sequence guard collapses
        // this into a single paint (no double flicker).
        if (!seenCycleInit) ended = true;
        else if (curCycle !== seenCycle) ended = true;
        seenCycle = curCycle;
        seenCycleInit = true;
        if (ended && typeof onCycleEnd === "function") onCycleEnd();
        // Idle still polls: the merge loop starts cycles on its own, and the
        // card text promises auto-refresh. Slower cadence than a running cycle.
        encTimer = setTimeout(pollEncode, 10000);
      }
    };
    encBtn.addEventListener("click", async () => {
      encBtn.disabled = true;
      // A fresh kick also resets the probe failure budget (see retry above).
      encFails = 0;
      try {
        await UI.api("/encode/backfill", { method: "POST" });
        UI.toast("已触发补编码：合并周期立即开始", "ok");
      } catch (e) { UI.toast("触发失败：" + e.message, "err"); }
      pollEncode();
    });
    encRetry.addEventListener("click", () => {
      encRetry.style.display = "none";
      encFails = 0;
      pollEncode();
    });
    const card = this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "手动补编码"]),
      this.el("p", { class: "muted", style: "font-size:12px" }, [
        "唤醒合并周期立即执行一次补编码（周期仍是唯一执行者，不会并发）；积压数量与周期状态自动刷新。",
      ]),
      this.el("div", { style: "display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-top:8px" }, [encBtn, encStatus, encRetry]),
    ]);
    pollEncode();
    return card;
  },

  // ---------------- Kira memory import (read-only source + idempotent keys) ----------------
  kiraImportCard() {
    const pathInput = this.el("input", { type: "text", class: "mono", style: "flex:1;min-width:220px" });
    const scanBtn = this.el("button", { class: "btn small ghost" }, ["扫描"]);
    const infoBox = this.el("div", { class: "muted", style: "font-size:12px;line-height:1.9;margin:10px 0" });
    const runBtn = this.el("button", { class: "btn small" }, ["开始迁入"]);
    runBtn.disabled = true;
    // Bumped on every source-path edit; scan/import responses tagged with an
    // older value are discarded instead of painting the current path's UI.
    let pathVer = 0;

    const line = (text, strong) =>
      this.el("div", {}, strong ? [this.el("b", {}, [text])] : [text]);
    const renderInfo = (d) => {
      const p = d.preview || {};
      infoBox.replaceChildren(
        line(`源目录：${d.source_path}${p.exists ? "" : "（不存在）"}`, true),
        line(p.exists
          ? `可迁入：${p.sessions} 个会话 / ${p.chunks} 个对话批次（${p.messages} 条消息）、${p.toml_files} 条事实·洞察（TOML）、${p.profiles} 份画像别名${p.archive_files ? `；另有 ${p.archive_files} 个已归档文件（按 KiraAI 语义不迁入）` : ""}`
          : "目录不存在或不可访问，请检查路径。"),
        line("迁入方式：会话批次以「待提炼原文」入库，由后台合并管线逐步 LLM 提炼为摘要/事实/关系边（耗时取决于 LLM 预算，属正常现象）。", false),
        line("安全边界：KiraAI 原始数据只读不改不删；全部写入走幂等键，重复执行自动去重、不产生重复记忆。", false),
      );
      if (d.last_run) {
        const r = d.last_run;
        infoBox.appendChild(line(
          `上次迁入：${UI.fmtTime(r.finished_at)} @ ${r.source_path} — 实插摘要 ${r.summary_rows} / 事实 ${r.fact_rows} / 别名 ${r.aliases_upserted}`,
          true,
        ));
      }
    };
    // No request on render: scanning and importing are explicit one-shot actions.
    const renderIdle = () => {
      infoBox.replaceChildren(
        line("点击「扫描」读取源目录（只读，一次性）；留空则使用默认源目录。"),
        line("迁入方式：会话批次以「待提炼原文」入库，由后台合并管线逐步 LLM 提炼为摘要/事实/关系边（耗时取决于 LLM 预算，属正常现象）。", false),
        line("安全边界：KiraAI 原始数据只读不改不删；全部写入走幂等键，重复执行自动去重、不产生重复记忆。", false),
      );
    };

    const refresh = async (pathOverride) => {
      // Path version: editing the input invalidates in-flight scans/imports so
      // a late response can neither paint a preview for a path the input no
      // longer shows nor re-enable「开始迁入」.
      const ver = pathVer;
      runBtn.disabled = true;
      scanBtn.disabled = true;
      infoBox.replaceChildren(line("扫描中…"));
      try {
        const q = pathOverride ? "?source_path=" + encodeURIComponent(pathOverride) : "";
        const d = await UI.api("/kira_memory/import" + q);
        if (ver !== pathVer) return; // source path changed while scanning
        if (!pathInput.value) pathInput.value = d.source_path || d.default_path || "";
        renderInfo(d);
        runBtn.disabled = !(d.preview && d.preview.exists);
      } catch (e) {
        if (ver !== pathVer) return; // stale failure: keep the new path's UI
        infoBox.replaceChildren(line(`扫描失败：${e.message}`));
      } finally {
        // The scan button recovers whether or not the version still matches.
        scanBtn.disabled = false;
      }
    };

    pathInput.addEventListener("input", () => {
      // New source path: the rendered preview belongs to the old one, and the
      // import button must wait for a scan of the new path.
      pathVer++;
      runBtn.disabled = true;
      renderIdle();
    });
    scanBtn.addEventListener("click", () => refresh(pathInput.value.trim()));
    runBtn.addEventListener("click", () => {
      const p = pathInput.value.trim();
      const ver = pathVer;
      UI.confirm("确认迁入 KiraAI 数据",
        `将从 ${p} 只读迁入记忆数据到本插件。KiraAI 原始数据不会被修改或删除；重复执行按幂等键自动去重。导入的会话批次将由后台管线逐步提炼生效。确认执行？`,
        async () => {
          runBtn.disabled = true;
          scanBtn.disabled = true;
          try {
            const r = await UI.api("/kira_memory/import", {
              method: "POST",
              body: JSON.stringify({ source_path: p }),
            });
            const s = r.stats || {};
            UI.toast(`迁入完成：摘要 ${s.summary_rows || 0} 条、事实 ${s.fact_rows || 0} 条、别名 ${s.aliases_upserted || 0} 条（跳过重复：摘要 ${s.summary_skipped || 0}、事实 ${s.facts_skipped || 0}）`, "ok");
            // Update in place from this run's stats: no full re-scan after import.
            // A path edited mid-import owns the info box now, so leave it alone.
            if (ver === pathVer) {
              infoBox.replaceChildren(
                line(`本次迁入完成：摘要 ${s.summary_rows || 0} 条、事实 ${s.fact_rows || 0} 条、别名 ${s.aliases_upserted || 0} 条（跳过重复：摘要 ${s.summary_skipped || 0}、事实 ${s.facts_skipped || 0}）。`, true),
                line("可再次点击「扫描」查看源目录当前可迁入计数。"),
              );
            }
          } catch (e) {
            UI.toast(`迁入失败：${e.message}`, "err");
          } finally {
            // Only re-enable for the path this run actually imported: a newer
            // path has not been scanned yet and must stay blocked.
            if (ver === pathVer) runBtn.disabled = false;
            scanBtn.disabled = false;
          }
        }, "确认迁入");
    });

    const card = this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "Kira 记忆数据迁入"]),
      this.el("p", { class: "muted", style: "font-size:12px" }, [
        "把 KiraAI 的会话历史（chat_memory.json）、事实/洞察（TOML）与画像别名一次性迁入本插件；源数据只读，写入幂等可重复执行。",
      ]),
      this.el("div", { style: "display:flex;gap:8px;align-items:center;flex-wrap:wrap" }, [pathInput, scanBtn]),
      infoBox,
      runBtn,
    ]);
    renderIdle();
    return card;
  },

  // ---------------- Host DB import (KiraAI v3 keeps sessions in the database) ----------------
  hostImportCard() {
    const infoBox = this.el("div", { class: "muted", style: "font-size:12px;line-height:1.9;margin:10px 0" });
    const scanBtn = this.el("button", { class: "btn small ghost" }, ["扫描宿主库"]);
    const runBtn = this.el("button", { class: "btn small" }, ["从宿主数据库迁入"]);

    const line = (text, strong) =>
      this.el("div", {}, strong ? [this.el("b", {}, [text])] : [text]);
    const renderInfo = (d) => {
      const p = d.preview || {};
      infoBox.replaceChildren(
        line("数据源：宿主数据库（KiraAI 会话表）", true),
        line(`可迁入：${p.sessions} 个会话 / ${p.chunks} 个对话批次（${p.messages} 条消息）`),
        line("适配 v3：KiraAI v3 起把会话上下文写入数据库、不再更新 chat_memory.json，此入口直接读取宿主当前会话，覆盖升级后新产生的记忆。仅迁入会话摘要（v3 无 entities/TOML 事实源）。", false),
        line("安全边界：宿主数据只读；写入走幂等键，重复执行自动去重。", false),
      );
      if (d.last_run && d.last_run.source === "host_db") {
        const r = d.last_run;
        infoBox.appendChild(line(
          `上次宿主库迁入：${UI.fmtTime(r.finished_at)} — 实插摘要 ${r.summary_rows}`,
          true,
        ));
      }
    };

    // No request on render: scanning reads all host sessions (full table + full
    // bodies), so it only runs on explicit user action.
    const renderIdle = () => {
      infoBox.replaceChildren(
        line("数据源：宿主数据库（KiraAI 会话表）", true),
        line("点击「扫描宿主库」读取宿主当前会话（只读，一次性）；迁入同样按需触发。"),
        line("适配 v3：KiraAI v3 起把会话上下文写入数据库、不再更新 chat_memory.json，此入口直接读取宿主当前会话，覆盖升级后新产生的记忆。仅迁入会话摘要（v3 无 entities/TOML 事实源）。", false),
        line("安全边界：宿主数据只读；写入走幂等键，重复执行自动去重。", false),
      );
    };

    const scan = async () => {
      scanBtn.disabled = true;
      runBtn.disabled = true;
      infoBox.replaceChildren(line("扫描中…"));
      try {
        const d = await UI.api("/kira_memory/import_host/preview");
        renderInfo(d);
      } catch (e) {
        infoBox.replaceChildren(line(`扫描失败：${e.message}`));
      } finally {
        scanBtn.disabled = false;
        runBtn.disabled = false;
      }
    };

    scanBtn.addEventListener("click", scan);
    runBtn.addEventListener("click", () => {
      UI.confirm("确认从宿主数据库迁入",
        "将从宿主数据库（KiraAI 会话表）只读迁入当前会话摘要到本插件。宿主数据不会被修改或删除；重复执行按幂等键自动去重。确认执行？",
        async () => {
          runBtn.disabled = true;
          scanBtn.disabled = true;
          try {
            const r = await UI.api("/kira_memory/import_host", { method: "POST" });
            const s = r.stats || {};
            UI.toast(`迁入完成：摘要 ${s.summary_rows || 0} 条（跳过重复：${s.summary_skipped || 0}）`, "ok");
            // Update in place from this run's stats: no full re-scan after import.
            infoBox.replaceChildren(
              line(`本次迁入完成：实插摘要 ${s.summary_rows || 0} 条（跳过重复 ${s.summary_skipped || 0}）。`, true),
              line("可再次点击「扫描宿主库」查看宿主当前可迁入计数。"),
            );
          } catch (e) {
            UI.toast(`迁入失败：${e.message}`, "err");
          } finally {
            runBtn.disabled = false;
            scanBtn.disabled = false;
          }
        }, "确认迁入");
    });

    const card = this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "宿主数据库迁入"]),
      this.el("p", { class: "muted", style: "font-size:12px" }, [
        "读取 KiraAI v3 数据库中的当前会话（升级后新产生的记忆不在 chat_memory.json 中），只读迁入会话摘要；写入幂等可重复执行。",
      ]),
      this.el("div", { style: "display:flex;gap:8px;align-items:center;flex-wrap:wrap" }, [scanBtn]),
      infoBox,
      runBtn,
    ]);
    renderIdle();
    return card;
  },

  // ---------------- Settings (plugin config) ----------------
  async renderSettings(el) {
    const d = await UI.api("/config");
    const schema = d.schema;
    const controls = {}; // field -> input node
    // Initial (masked) value per field: the "user touched it" baseline used to
    // omit untouched secrets from the save payload.
    const initialValues = {};

    const buildControl = (name) => {
      const f = schema[name], v = d.values[name];
      if (f.type === "bool") {
        const c = this.el("input", { type: "checkbox" });
        c.checked = !!v;
        return c;
      }
      if (f.type === "int" || f.type === "float") {
        const attrs = { type: "number", step: f.type === "float" ? "any" : "1" };
        // Backend-derived bounds keep the input itself from offering
        // invalid values (e.g. score_cap=0 would pin scores and stall
        // promotion) -- restored from the old page.
        if (f.min !== undefined && f.min !== null) attrs.min = String(f.min);
        if (f.max !== undefined && f.max !== null) attrs.max = String(f.max);
        const c = this.el("input", attrs);
        if (v !== null && v !== undefined) c.value = String(v);
        return c;
      }
      if (f.type === "list") {
        const c = this.el("textarea", { rows: "3", class: "list-input", placeholder: "每行一项" });
        c.value = (v || []).join("\n");
        return c;
      }
      const c = this.el("input", { type: f.sensitive ? "password" : "text" });
      c.value = v === null || v === undefined ? "" : String(v);
      return c;
    };

    const fieldRow = (name) => {
      const f = schema[name];
      const ctl = buildControl(name);
      controls[name] = ctl;
      initialValues[name] = ctl.value;
      return this.el("div", { class: "cfg-field" + (f.type === "list" || f.type === "str" ? " wide" : "") }, [
        this.el("label", {}, [
          this.el("span", { class: "mono" }, [name]),
          f.restart ? this.el("span", { class: "badge warn", title: "组件装配期取值：保存已写入配置，需重启进程才改变行为" }, ["重启生效"]) : null,
        ]),
        ctl,
        this.el("div", { class: "desc", title: f.description }, [f.description]),
      ]);
    };

    el.appendChild(this.el("div", { class: "card" }, [
      this.el("h3", {}, [UI.paw(), "插件配置"]),
      this.el("p", { class: "muted", style: "font-size:12px" }, [
        "保存写入宿主插件配置存储并即时生效（kernel / 合并 agent / 编码器等运行时直接读取）；标「重启生效」的字段需重启进程。",
      ]),
      this.el("p", { class: "muted", style: "font-size:12px" }, [
        "插件总开关（enabled）请在宿主控制台「插件管理」页操作。",
      ]),
    ]));
    el.appendChild(this.kiraImportCard());
    el.appendChild(this.hostImportCard());
    for (const g of d.groups) {
      el.appendChild(this.el("div", { class: "card" }, [
        this.el("h3", {}, [g.label]),
        this.el("div", { class: "cfg-grid" }, g.fields.map(fieldRow)),
      ]));
    }

    const saveBtn = this.el("button", { class: "btn" }, [UI.svg("DocumentChecked"), "保存配置"]);
    const resetBtn = this.el("button", { class: "btn small ghost" }, ["表单还原默认值"]);
    el.appendChild(this.el("div", { class: "settings-actions" }, [saveBtn, resetBtn]));

    const collect = () => {
      const out = {};
      for (const [name, ctl] of Object.entries(controls)) {
        const t = schema[name].type;
        if (t === "bool") out[name] = ctl.checked;
        else if (t === "int" || t === "float") {
          // Empty/non-numeric input = "leave unchanged" (omit the key), NOT a
          // silent 0 — score_cap=0 would stall promotion, embedding_dims=0
          // would kill the plugin on restart. Exception: null-default floats
          // are derived fields and submit null (see below).
          const raw = ctl.value.trim();
          if (raw === "") {
            // A float whose schema default is null is a derived/optional
            // field (naming_promote_threshold): submitting null tells the
            // backend to drop the override and restore derivation, while
            // omitting the key would keep the stored value.
            if (t === "float" && schema[name].default === null) out[name] = null;
            continue;
          }
          if (t === "int") {
            // Reject silently truncating input ("1e3" / "1.5" used to become 1
            // via parseInt and apply live with no warning); collect() throws,
            // the save handler catches it, toasts and aborts the request.
            const n = Number(raw);
            if (!Number.isFinite(n) || !Number.isInteger(n)) {
              throw new Error(`字段 ${name} 需要整数，当前输入为「${raw}」`);
            }
            out[name] = n;
            continue;
          }
          const n = parseFloat(raw);
          if (isNaN(n)) continue;
          out[name] = n;
        }
        else if (t === "list") out[name] = ctl.value.split("\n").map((s) => s.trim()).filter(Boolean);
        else if (schema[name].sensitive && ctl.value === initialValues[name]) {
          // Untouched secret (still showing the mask sentinel) is not sent
          // back; the backend then keeps the stored value, so the mask can
          // never overwrite the real secret.
          continue;
        }
        else out[name] = ctl.value;
      }
      return out;
    };

    saveBtn.addEventListener("click", async () => {
      try {
        saveBtn.disabled = true;
        try {
          const r = await UI.api("/config", { method: "PUT", body: JSON.stringify({ values: collect() }) });
          if (!r.saved) { UI.toast("配置没有变化", ""); return; }
          UI.toast(`已保存 ${r.changes} 项配置`, "ok");
          if (r.persistence_error) {
            // Applied live but failed to persist: reverts on restart, surface it
            UI.toast(`已热生效但落盘失败，重启后将回退：${r.persistence_error}`, "err");
          }
          if (r.restart_required && r.restart_required.length) {
            UI.modal("部分配置需重启进程生效", [
              this.el("p", {}, ["以下字段在组件装配期取值，已写入配置文件，需重启进程才改变行为："]),
              this.el("div", { class: "pre-box" }, [r.restart_required.join("\n")]),
            ]);
          }
        } finally { saveBtn.disabled = false; }
      } catch (e) { UI.toast("保存失败：" + e.message, "err"); }
    });

    resetBtn.addEventListener("click", () => {
      UI.confirm("还原默认值", "把表单恢复为出厂默认值？（不保存，还原后仍需点「保存配置」才会写入）", () => {
        for (const [name, ctl] of Object.entries(controls)) {
          const f = schema[name], dv = f.default;
          if (f.type === "bool") ctl.checked = !!dv;
          else if (f.type === "list") ctl.value = (dv || []).join("\n");
          else ctl.value = dv === null || dv === undefined ? "" : String(dv);
        }
      }, "还原");
    });
  },
};

Page.boot();
