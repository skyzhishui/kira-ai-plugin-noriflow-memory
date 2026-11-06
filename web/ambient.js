/* ============================================================
   Memory star map - ambient layer (pure decoration, no logic/API)
   - Stardust canvas: drifting memory motes + occasional constellation lines,
     nudged by the pointer
   - Footer quip carousel: text-only decoration
   - Performance and politeness: DPR <= 1.5, particles capped by area,
     ~30fps throttle, paused while the tab is hidden, a single still frame
     under prefers-reduced-motion, start/stop in sync on both-way mobile
     breakpoint switches (no rAF while the canvas is hidden)
   ============================================================ */
(function () {
  "use strict";
  var canvas = document.getElementById("fx-canvas");
  if (!canvas) return;
  // Mobile falls back to the CSS static starfield (the stability rules at
  // the end of memory.css set display:none): a live canvas is resized and
  // cleared as the address bar expands/collapses or the page scrolls.
  // No early return here: the onMobile listener below drives both directions.
  var mobileFx = window.matchMedia(
    "(max-width: 900px), (hover: none) and (pointer: coarse)"
  );
  if (mobileFx.matches) canvas.style.display = "none";
  var ctx = canvas.getContext("2d");
  if (!ctx) return;

  var reducedMq = window.matchMedia("(prefers-reduced-motion: reduce)");
  var DPR_CAP = 1.5;
  var FRAME_GAP = 1000 / 30; // 30fps is plenty for an ambient effect
  var W = 0, H = 0, dpr = 1;
  var motes = [];
  var rafId = 0;
  var running = false;
  var lastFrame = 0;
  var pointer = { x: -9999, y: -9999 };
  var pal = { dot: [150, 230, 255], line: [130, 170, 255], strength: 0.8 };

  function parseTriplet(raw, fallback) {
    var m = String(raw || "").match(/\d+\s*,\s*\d+\s*,\s*\d+/);
    if (!m) return fallback;
    var parts = m[0].split(",").map(function (n) { return parseInt(n, 10) || 0; });
    return parts.length === 3 ? parts : fallback;
  }

  // Colors follow theme vars (index.html flips data-theme on host switch)
  function readPalette() {
    try {
      var cs = getComputedStyle(document.documentElement);
      pal.dot = parseTriplet(cs.getPropertyValue("--fx-dot"), pal.dot);
      pal.line = parseTriplet(cs.getPropertyValue("--fx-line"), pal.line);
      var s = parseFloat(cs.getPropertyValue("--fx-strength"));
      pal.strength = isNaN(s) ? pal.strength : Math.max(0, Math.min(1.5, s));
    } catch (e) { /* keep default colors when unreadable */ }
  }

  function rgba(c, a) { return "rgba(" + c[0] + "," + c[1] + "," + c[2] + "," + a + ")"; }

  function spawn(x, y) {
    var depth = 0.35 + Math.random() * 0.65; // depth: speed/brightness/jitter
    return {
      x: x, y: y,
      r: (0.5 + Math.random() * 1.3) * (0.6 + depth * 0.7),
      vx: (Math.random() - 0.5) * 0.16 * depth,
      vy: (Math.random() - 0.5) * 0.12 * depth - 0.02 * depth,
      depth: depth,
      phase: Math.random() * Math.PI * 2,
      tw: 0.4 + Math.random() * 1.2,
    };
  }

  function targetCount() {
    return Math.max(34, Math.min(120, Math.round((W * H) / 17000)));
  }

  function seed() {
    var want = targetCount();
    while (motes.length < want) motes.push(spawn(Math.random() * W, Math.random() * H));
    if (motes.length > want) motes.length = want;
  }

  function resize() {
    W = canvas.clientWidth || window.innerWidth;
    H = canvas.clientHeight || window.innerHeight;
    dpr = Math.min(DPR_CAP, window.devicePixelRatio || 1);
    canvas.width = Math.round(W * dpr);
    canvas.height = Math.round(H * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    seed();
    if (reducedMq.matches) drawFrame(0, true);
  }

  function stepMotion(k) {
    for (var i = 0; i < motes.length; i++) {
      var m = motes[i];
      // Gentle pointer nudge: nearby motes get pushed softly away (far layers
      // barely move)
      var dx = m.x - pointer.x, dy = m.y - pointer.y;
      var d2 = dx * dx + dy * dy;
      if (d2 < 19600 && d2 > 0.01) { // 140px radius
        var d = Math.sqrt(d2);
        var push = (1 - d / 140) * 0.045 * m.depth * k;
        m.x += (dx / d) * push;
        m.y += (dy / d) * push;
      }
      m.x += m.vx * k;
      m.y += m.vy * k;
      if (m.x < -12) m.x = W + 12; else if (m.x > W + 12) m.x = -12;
      if (m.y < -12) m.y = H + 12; else if (m.y > H + 12) m.y = -12;
    }
  }

  function drawFrame(t, still) {
    ctx.clearRect(0, 0, W, H);
    var ns = motes;
    var linkD = 110, linkD2 = linkD * linkD;
    // Occasional constellation links: close pairs only, very low alpha
    for (var i = 0; i < ns.length; i++) {
      var a = ns[i];
      for (var j = i + 1; j < ns.length; j++) {
        var b = ns[j];
        var dx = a.x - b.x; if (dx > linkD || dx < -linkD) continue;
        var dy = a.y - b.y; if (dy > linkD || dy < -linkD) continue;
        var d2 = dx * dx + dy * dy;
        if (d2 > linkD2) continue;
        var alpha = (1 - Math.sqrt(d2) / linkD) * 0.09 * pal.strength * Math.min(a.depth, b.depth);
        if (alpha < 0.008) continue;
        ctx.strokeStyle = rgba(pal.line, alpha);
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
    }
    for (var n = 0; n < ns.length; n++) {
      var m = ns[n];
      var twinkle = still ? 0.75 : 0.62 + 0.38 * Math.sin(t * 0.001 * m.tw + m.phase);
      var al = (0.16 + 0.5 * m.depth) * twinkle * pal.strength;
      ctx.fillStyle = rgba(pal.dot, Math.min(0.85, al));
      ctx.beginPath();
      ctx.arc(m.x, m.y, m.r, 0, 6.2832);
      ctx.fill();
      // A few bright stars carry a faint halo
      if (m.r > 1.35) {
        ctx.fillStyle = rgba(pal.dot, Math.min(0.12, al * 0.18));
        ctx.beginPath();
        ctx.arc(m.x, m.y, m.r * 3.2, 0, 6.2832);
        ctx.fill();
      }
    }
  }

  function loop(t) {
    rafId = 0;
    if (!running) return;
    if (t - lastFrame >= FRAME_GAP) {
      lastFrame = t;
      stepMotion(FRAME_GAP / 16.7);
      drawFrame(t, false);
    }
    rafId = requestAnimationFrame(loop);
  }

  function start() {
    if (running || mobileFx.matches || reducedMq.matches || document.hidden) return;
    running = true;
    lastFrame = 0;
    rafId = requestAnimationFrame(loop);
  }

  function stop() {
    running = false;
    if (rafId) { cancelAnimationFrame(rafId); rafId = 0; }
  }

  addEventListener("resize", function () {
    clearTimeout(resize._t);
    resize._t = setTimeout(resize, 120);
  });
  addEventListener("pointermove", function (e) {
    pointer.x = e.clientX; pointer.y = e.clientY;
  }, { passive: true });
  document.addEventListener("pointerout", function (e) {
    if (!e.relatedTarget) { pointer.x = -9999; pointer.y = -9999; }
  });
  document.addEventListener("visibilitychange", function () {
    if (document.hidden) stop(); else start();
  });

  // Reduced motion: on -> stop and draw a still frame; off -> restart
  // (start has three guards)
  var onReduced = function () {
    if (reducedMq.matches) { stop(); drawFrame(0, true); }
    else start();
  };
  if (reducedMq.addEventListener) reducedMq.addEventListener("change", onReduced);
  else if (reducedMq.addListener) reducedMq.addListener(onReduced);

  // Start/stop in sync across breakpoints/device shapes: no rAF while CSS
  // hides the canvas; back on desktop, drop the inline hide, rebuild at the
  // current size and restart
  var onMobile = function () {
    if (mobileFx.matches) {
      canvas.style.display = "none";
      stop();
    } else {
      canvas.style.display = "";
      resize();
      start();
    }
  };
  if (mobileFx.addEventListener) mobileFx.addEventListener("change", onMobile);
  else if (mobileFx.addListener) mobileFx.addListener(onMobile);

  // Re-read colors on theme switch (also redraws a frame in still mode)
  new MutationObserver(function () {
    readPalette();
    if (reducedMq.matches) drawFrame(0, true);
  }).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });

  readPalette();
  resize();
  if (mobileFx.matches) return; // mobile: CSS starfield takes over, no canvas
  if (reducedMq.matches) drawFrame(0, true);
  else start();
})();

/* ---------------- Footer quip carousel (pure decoration) ---------------- */
(function () {
  "use strict";
  var el = document.getElementById("foot-quip");
  if (!el) return;
  var quips = [
    "今天的星图又亮了一点——都是聊出来的。",
    "记忆打捞完毕，观测台随时在岗。",
    "温馨提示：翻旧账之前，请先预约。",
    "不是特意记住的，只是顺手而已。",
    "星星不说谎，星图也不会。",
    "观测报告：今天也在好好记录。",
  ];
  var i = Math.floor(Math.random() * quips.length);
  el.textContent = quips[i];
  var reducedMq = window.matchMedia("(prefers-reduced-motion: reduce)");
  var tickTimer = 0;
  var swapTimer = 0;

  // Stop the rotation and clear any pending swap so no timer survives a
  // switch to reduced motion (a half-applied swap must not leave the class on).
  function stop() {
    if (tickTimer) { clearTimeout(tickTimer); tickTimer = 0; }
    if (swapTimer) { clearTimeout(swapTimer); swapTimer = 0; el.classList.remove("quip-swap"); }
  }

  function schedule() {
    if (!tickTimer) tickTimer = setTimeout(tick, 9000);
  }

  function tick() {
    tickTimer = 0;
    // Re-read the preference on every tick: it can flip at runtime (OS
    // setting or devtools) without a page reload.
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    i = (i + 1) % quips.length;
    el.classList.add("quip-swap");
    swapTimer = setTimeout(function () {
      swapTimer = 0;
      el.textContent = quips[i];
      el.classList.remove("quip-swap");
    }, 280);
    schedule();
  }

  function onReducedChange() {
    if (reducedMq.matches) stop();
    else schedule();
  }

  // Initial semantic is unchanged: under reduced motion the quip is shown
  // once and never rotates until the preference is turned off dynamically.
  if (!reducedMq.matches) schedule();
  if (reducedMq.addEventListener) reducedMq.addEventListener("change", onReducedChange);
  else if (reducedMq.addListener) reducedMq.addListener(onReducedChange);
})();
