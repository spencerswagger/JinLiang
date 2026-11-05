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
  var log = document.getElementById("eventlog");
  var live = document.getElementById("live-phase");
  var hint = document.getElementById("live-hint");
  var dot = document.getElementById("live-dot");
  var stream = document.getElementById("sse-source");

  function appendLine(ev) {
    if (!log) return;
    var row = document.createElement("div");
    var cls = ev.type === "error" ? "err" : ev.type === "done" ? "ok" : "";
    var time = new Date().toLocaleTimeString();
    row.className = cls;
    row.innerHTML =
      '<span class="t">' + time + "</span> " +
      (ev.phase_label ? "[" + ev.phase_label + "] " : "") +
      (ev.message || ev.type || "");
    log.appendChild(row);
    log.scrollTop = log.scrollHeight;
    while (log.childElementCount > 200) {
      log.removeChild(log.firstChild);
    }
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
      if (ev.type === "done" && dot) dot.classList.remove("on");
    }
    // 服务端给每条事件都带 event: <type>，所以必须按名字订阅；
    // onmessage 只收「没有 event 字段」的消息（保留它是为了兼容旧服务端）。
    ["phase", "progress", "note", "error", "done"].forEach(function (name) {
      es.addEventListener(name, onEvent);
    });
    es.addEventListener("reset", function () {
      // 控制面进程重启过：之前的序号失效，日志里明说一句，然后从新流继续。
      if (log) {
        var row = document.createElement("div");
        row.className = "ev";
        row.innerHTML = '<span class="t">' + new Date().toLocaleTimeString() + "</span> " +
          "控制面重启过，事件流已重置（下面从这一轮开头继续）";
        log.appendChild(row);
      }
    });
    es.onmessage = onEvent;
  }

  if (stream) {
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
