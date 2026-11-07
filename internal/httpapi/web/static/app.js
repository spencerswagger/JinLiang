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

  // rows 显示：**优先**用 progress 事件里的结构化字段（B5 起引擎会填 rows_done）；
  // 字段缺失时才退回「拉一次 /api/jobs/{id} 的 last_run」——两种来源都要能工作，
  // 不能只写死一种（引擎与前端不是同一次发版的）。
  // 轮询仍然节流：snapshot 每块一条 progress，10M 会上百条，最多 1 秒一次、单飞。
  var rowsEl = document.getElementById("live-rows");
  var barEl = document.getElementById("live-progress");
  var lastRowsAt = 0;
  var rowsInflight = false;
  var lastRows = -1;      // 进度条不回退：只接受单调不减的数字
  var lastTotal = null;   // 总数只在「估得出」时才有；不要用 0 冒充未知

  function setRows(n) {
    var num = typeof n === "number" ? n : parseInt(n, 10);
    if (isNaN(num)) return;
    if (num < lastRows) return; // 回退的一律丢弃（在途轮询/乱序事件）
    lastRows = num;
    if (rowsEl) {
      var txt = "已处理 " + num + " 行";
      if (lastTotal && lastTotal > 0) {
        var pct = Math.min(100, Math.floor((num * 100) / lastTotal));
        txt += " / 约 " + lastTotal + " 行（" + pct + "%）";
      }
      rowsEl.textContent = txt;
    }
    if (barEl && lastTotal && lastTotal > 0) {
      barEl.max = lastTotal;
      barEl.value = Math.min(num, lastTotal); // 超了也别画出 110%
      barEl.hidden = false;
    }
  }

  // 事件里带了 rows_done 就直接用（0 也是合法值，所以用 typeof 判存在）
  var rowsFromEventSeen = false; // 事件已经给过数字之后，就不许轮询把旧值写回来
  function rowsFromEvent(ev) {
    // rows_total 只在估得出时才有；缺省（不是 0）表示「未知」，此时不画进度条
    if (typeof ev.rows_total === "number" && ev.rows_total > 0) {
      lastTotal = ev.rows_total;
    }
    if (typeof ev.rows_done === "number") {
      rowsFromEventSeen = true;
      setRows(ev.rows_done);
      if (typeof ev.tables_done === "number" && typeof ev.tables_total === "number" && rowsEl) {
        rowsEl.textContent += " · 表 " + ev.tables_done + "/" + ev.tables_total;
      }
      return true;
    }
    return false;
  }

  function refreshRows(jobID) {
    if (!rowsEl || !jobID || rowsInflight) return;
    var now = Date.now();
    if (now - lastRowsAt < 1000) return; // 1 秒内只拉一次
    lastRowsAt = now;
    rowsInflight = true;
    fetch("/api/jobs/" + encodeURIComponent(jobID), { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        // 轮询是在途请求：事件可能已经先一步给出更新的数字。此时丢弃轮询结果，
        // 否则进度会往回跳（最多回退 1 秒，但用户看到的就是「倒退」）。
        if (rowsFromEventSeen) return;
        var run = d && d.job && d.job.last_run;
        if (run) {
          setRows(run.rows_done || 0);
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
      // run_id 为空 = 还没开跑就失败（Core 的收紧规则）：这种帧不推进阶段、
      // 也不当作某一轮的历史记录，只把消息与四段式提示给出来。
      if (ev.type !== "error" || ev.run_id) {
        if (!rowsFromEvent(ev) && (ev.type === "progress" || ev.type === "phase")) {
          refreshRows(ev.job_id);
        }
      }
      if (ev.type === "done") {
        if (dot) dot.classList.remove("on");
        // finish_reason 是「跑完了」还是「被停止」的唯一权威（#18 的徽章同一口径）
        var fr = ev.finish_reason;
        if (fr && live) {
          live.textContent =
            fr === "completed" ? "已完成" :
            fr === "stopped" ? "已停止（不是失败）" :
            fr === "interrupted" ? "已中断（被别的实例接管）" :
            fr === "failed" ? "失败" : fr;
        }
        if (fr && fr !== "completed" && dot) dot.classList.add("warn");
      }
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
