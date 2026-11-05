// 津梁控制台的前端脚本：手写，无构建链，无依赖（htmx 另有一个 vendored 文件）。
// 只做三件事：CSRF 头、SSE 事件流渲染、表单提交防重复点击。

(function () {
  "use strict";

  var csrf = (document.body && document.body.dataset.csrf) || "";

  // ── htmx：给每个非 GET 请求带上双提交 token ─────────────────
  document.body.addEventListener("htmx:configRequest", function (evt) {
    if (evt.detail.verb && evt.detail.verb !== "get") {
      evt.detail.headers["X-CSRF-Token"] = csrf;
    }
  });

  // htmx 自己发出的错误响应也要看得见（否则用户只看到"没反应"）
  document.body.addEventListener("htmx:responseError", function (evt) {
    var target = evt.detail.target || document.getElementById("action-result");
    if (target) {
      target.innerHTML =
        '<div class="err"><h3>这次操作没有成功</h3><p>' +
        (evt.detail.xhr.status + " " + evt.detail.xhr.statusText) +
        "</p><p class=\"hint\">刷新页面重试；如果一直失败，去「诊断」页看体检结果。</p></div>";
    }
  });

  // ── SSE：任务详情页的实时阶段 ───────────────────────────────
  // 错误码表：error 事件只有 message + code + next，why/risk 要按 code 查表才有。
  // 一次拉全量（几十条），失败就退回「只显示 message + next」。
  var codeTable = {};
  var codesLoading = null;
  function loadCodes() {
    if (codesLoading) return codesLoading;
    codesLoading = fetch("/api/codes", { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) { if (d && d.codes) codeTable = d.codes; })
      .catch(function () { /* 查不到就少两段，不影响主流程 */ });
    return codesLoading;
  }

  var log = document.getElementById("eventlog");
  var live = document.getElementById("live-phase");
  var hint = document.getElementById("live-hint");
  var dot = document.getElementById("live-dot");
  var stream = document.getElementById("sse-source");

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function appendLine(ev) {
    if (!log) return;
    var time = new Date().toLocaleTimeString();
    var row = document.createElement("div");

    if (ev.type === "error") {
      // 完整的四段式：发生了什么 / 为什么 / 你能做什么 / 风险。
      // 事件只有 message + code + next；why 与 risk 按 code 查码表（core.Catalogue），
      // 查不到就只渲染前两段——不编造原因，也不假装没有风险。
      var spec = (ev.code && codeTable[ev.code]) || null;
      var why = spec && spec.why ? spec.why : "";
      var risk = spec && spec.risk ? spec.risk : "";
      var next = (ev.next || (spec && spec.next) || []).map(function (n) {
        return "<li>" + esc(n) + "</li>";
      }).join("");
      row.className = "err";
      row.innerHTML =
        '<h3>' + esc(ev.message || (spec && spec.message) || "出错了") +
        (ev.code ? ' <span class="badge bad">' + esc(ev.code) + "</span>" : "") + "</h3>" +
        "<dl>" +
        (why ? "<dt>为什么</dt><dd>" + esc(why) + "</dd>" : "") +
        (next ? "<dt>你现在能做什么</dt><dd><ol>" + next + "</ol></dd>" : "") +
        (risk ? "<dt>风险</dt><dd>" + esc(risk) + "</dd>" : "") +
        "</dl>" +
        '<p class="hint">' + esc(time) + "</p>";
      log.appendChild(row);
      log.scrollTop = log.scrollHeight;
      return;
    }

    row.className = ev.type === "done" ? "ok" : "";
    row.innerHTML =
      '<span class="t">' + esc(time) + "</span> " +
      (ev.phase_label ? "[" + esc(ev.phase_label) + "] " : "") +
      esc(ev.message || ev.type || "");
    log.appendChild(row);
    log.scrollTop = log.scrollHeight;
    while (log.childElementCount > 200) {
      log.removeChild(log.firstChild);
    }
  }

  // rows 刷新：progress 事件没有结构化字段（B5 才有），只能读控制库的 run 行；
  // 但 snapshot 每块一条 progress，10M 会上百条 —— 必须节流，最多 1 秒一次。
  var rowsEl = document.getElementById("live-rows");
  var lastRowsAt = 0;
  var rowsInflight = false;
  function refreshRows(jobID) {
    if (!rowsEl || !jobID || rowsInflight) return;
    var now = Date.now();
    if (now - lastRowsAt < 1000) return; // 1 秒内只拉一次
    lastRowsAt = now;
    rowsInflight = true;
    fetch("/api/jobs/" + encodeURIComponent(jobID), { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        var run = d && d.job && d.job.last_run;
        if (run) {
          rowsEl.textContent =
            "已处理 " + (run.rows_done || 0) + " 行（每 1 秒刷新一次；数字来自控制库的 run 行）";
        }
      })
      .catch(function () { /* 拉不到就保留上一次的数字 */ })
      .then(function () { rowsInflight = false; });
  }

  function startStream(url) {
    if (!window.EventSource || !url) return;
    var es = new EventSource(url);
    es.onopen = function () {
      if (dot) dot.classList.add("on");
    };
    es.onerror = function () {
      if (dot) dot.classList.remove("on");
      // EventSource 会自己重连；这里只把灯灭掉，别吓人
    };
    function onEvent(msg) {
      var ev;
      try {
        ev = JSON.parse(msg.data);
      } catch (e) {
        return;
      }
      appendLine(ev);
      if (ev.phase_label && live) live.textContent = ev.phase_label;
      if (ev.phase_hint && hint) hint.textContent = ev.phase_hint;
      if (ev.type === "progress" || ev.type === "phase") refreshRows(ev.job_id);
      if (ev.type === "done" && dot) dot.classList.remove("on");
    }
    // 服务端给每条事件都带 event: <type>，所以必须按名字订阅；
    // onmessage 只收「没有 event 字段」的消息（保留它是为了兼容旧服务端）。
    ["phase", "progress", "note", "error", "done"].forEach(function (name) {
      es.addEventListener(name, onEvent);
    });
    es.addEventListener("reset", function (msg) {
      // 控制面进程重启过：旧序号失效，服务端会从头重发。
      // 必须先清空本地列表——否则上一进程的事件会与新的事件混在一起。
      var reason = "控制面重启过，事件流已重置（下面从这一轮开头继续）";
      try {
        var p = JSON.parse(msg.data || "{}");
        if (p.reason) reason = p.reason;
      } catch (e) { /* 用默认文案 */ }
      if (log) {
        log.innerHTML = "";
        var row = document.createElement("div");
        row.innerHTML = '<span class="t">' + new Date().toLocaleTimeString() + "</span> " + esc(reason);
        log.appendChild(row);
      }
    });
    es.onmessage = onEvent;
  }

  if (stream) {
    loadCodes();
    startStream(stream.dataset.url);
  }

  // ── 提交按钮防重复点击（任务 run 之类的写操作） ──────────────
  document.addEventListener("submit", function (evt) {
    var form = evt.target;
    if (!form.matches("form[data-once]")) return;
    var btn = form.querySelector("button[type=submit], button:not([type])");
    if (btn) {
      btn.disabled = true;
      setTimeout(function () { btn.disabled = false; }, 4000);
    }
  });
})();
