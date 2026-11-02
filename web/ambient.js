/* ============================================================
   记忆星图 · 环境氛围层（纯装饰，不参与任何业务逻辑/API）
   - 星尘画布：漂移的记忆微尘 + 偶发星座连线，指针轻微扰动
   - 页脚碎念轮播：纯文案装饰
   - 性能与礼貌：DPR ≤ 1.5、粒子按面积封顶、~30fps 节流、
     页签隐藏时暂停、prefers-reduced-motion 时只画一帧静物、
     移动断点双向切换时同步启停（画布隐藏期间不跑 rAF）
   ============================================================ */
(function () {
  "use strict";
  var canvas = document.getElementById("fx-canvas");
  if (!canvas) return;
  // 移动端改用 CSS 静态星场（memory.css 末尾的稳定性规则会 display:none
  // 隐藏画布）：实时 canvas 在地址栏收放/滚动时会重设尺寸并清屏。
  // 不在此提前 return：断点双向切换由下方 onMobile 监听同步启停。
  var mobileFx = window.matchMedia(
    "(max-width: 900px), (hover: none) and (pointer: coarse)"
  );
  if (mobileFx.matches) canvas.style.display = "none";
  var ctx = canvas.getContext("2d");
  if (!ctx) return;

  var reducedMq = window.matchMedia("(prefers-reduced-motion: reduce)");
  var DPR_CAP = 1.5;
  var FRAME_GAP = 1000 / 30; // 氛围效果 30fps 足够
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

  // 取色跟随主题变量（index.html 在宿主切换主题时改 data-theme）
  function readPalette() {
    try {
      var cs = getComputedStyle(document.documentElement);
      pal.dot = parseTriplet(cs.getPropertyValue("--fx-dot"), pal.dot);
      pal.line = parseTriplet(cs.getPropertyValue("--fx-line"), pal.line);
      var s = parseFloat(cs.getPropertyValue("--fx-strength"));
      pal.strength = isNaN(s) ? pal.strength : Math.max(0, Math.min(1.5, s));
    } catch (e) { /* 变量不可读时沿用默认色 */ }
  }

  function rgba(c, a) { return "rgba(" + c[0] + "," + c[1] + "," + c[2] + "," + a + ")"; }

  function spawn(x, y) {
    var depth = 0.35 + Math.random() * 0.65; // 远近层次：影响速度/亮度/扰动幅度
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
      // 指针轻微扰动：靠近的微尘被温柔推开（远层几乎不动）
      var dx = m.x - pointer.x, dy = m.y - pointer.y;
      var d2 = dx * dx + dy * dy;
      if (d2 < 19600 && d2 > 0.01) { // 140px 半径
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
    // 偶发星座连线：仅近距离、极低透明度
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
      // 少数亮星带一点光晕
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

  // 减少动效：开 → 停并画一帧静物；关 → 重新拉起（start 内有三重守卫）
  var onReduced = function () {
    if (reducedMq.matches) { stop(); drawFrame(0, true); }
    else start();
  };
  if (reducedMq.addEventListener) reducedMq.addEventListener("change", onReduced);
  else if (reducedMq.addListener) reducedMq.addListener(onReduced);

  // 跨断点/设备形态变化时同步启停：画布被 CSS 隐藏期间不跑 rAF，
  // 回到桌面形态时撤销内联隐藏、按当前尺寸重建并重新拉起
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

  // 主题切换时换一次取色（静物模式下顺带重绘一帧）
  new MutationObserver(function () {
    readPalette();
    if (reducedMq.matches) drawFrame(0, true);
  }).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });

  readPalette();
  resize();
  if (mobileFx.matches) return; // 移动端：CSS 静态星场接管，不启动实时画布
  if (reducedMq.matches) drawFrame(0, true);
  else start();
})();

/* ---------------- 页脚碎念轮播（纯装饰） ---------------- */
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
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  setInterval(function () {
    i = (i + 1) % quips.length;
    el.classList.add("quip-swap");
    setTimeout(function () {
      el.textContent = quips[i];
      el.classList.remove("quip-swap");
    }, 280);
  }, 9000);
})();
