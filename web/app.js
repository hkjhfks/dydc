const statusText = document.getElementById("statusText");
const form = document.getElementById("authorForm");
const submitBtn = document.getElementById("submitBtn");
const refreshBtn = document.getElementById("refreshBtn");
const runNowBtn = document.getElementById("runNowBtn");
const listEl = document.getElementById("authorList");
const emptyHint = document.getElementById("emptyHint");
const itemTpl = document.getElementById("authorItemTemplate");
const monitorMain = document.getElementById("monitorMain");
const monitorSub = document.getElementById("monitorSub");

function setStatus(text, type = "info") {
  statusText.textContent = text;
  if (type === "error") {
    statusText.style.background = "rgba(220, 38, 38, 0.12)";
    statusText.style.color = "#b91c1c";
    return;
  }
  if (type === "success") {
    statusText.style.background = "rgba(5, 150, 105, 0.14)";
    statusText.style.color = "#047857";
    return;
  }
  statusText.style.background = "rgba(37, 99, 235, 0.12)";
  statusText.style.color = "#1d4ed8";
}

async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  let payload = {};
  try {
    payload = await resp.json();
  } catch (_) {
    payload = {};
  }

  if (!resp.ok) {
    throw new Error(payload.error || `请求失败（HTTP ${resp.status}）`);
  }
  return payload;
}

function formatTime(iso) {
  if (!iso) {
    return "-";
  }
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) {
    return iso;
  }
  return d.toLocaleString();
}

function cutText(text, maxLen = 200) {
  if (!text) {
    return "";
  }
  if (text.length <= maxLen) {
    return text;
  }
  return `${text.slice(0, maxLen)}...`;
}

function renderMonitor(monitor) {
  if (!monitor) {
    monitorMain.textContent = "监控状态未知";
    monitorSub.textContent = "无法获取后台状态";
    return;
  }

  if (monitor.running) {
    monitorMain.textContent = `运行中：${monitor.current_reason || "任务执行"}`;
  } else if (monitor.queued) {
    monitorMain.textContent = `排队中：${monitor.queued_reason || "等待执行"}`;
  } else {
    monitorMain.textContent = "监控中（空闲）";
  }

  const lines = [
    `间隔：${monitor.min_interval_seconds || "-"} ~ ${monitor.max_interval_seconds || "-"} 秒（随机）`,
    `下次预计：${formatTime(monitor.next_due_at)}`,
    `最近开始：${formatTime(monitor.last_started_at)}`,
    `最近结束：${formatTime(monitor.last_finished_at)}`,
  ];

  if (monitor.last_return_code !== null && monitor.last_return_code !== undefined) {
    lines.push(monitor.last_ok ? "最近一轮：成功" : `最近一轮：失败（rc=${monitor.last_return_code}）`);
  }
  if (monitor.last_reason) {
    lines.push(`触发原因：${monitor.last_reason}`);
  }
  if (monitor.last_stderr) {
    lines.push(`错误摘要：${cutText(monitor.last_stderr.replace(/\s+/g, " "), 180)}`);
  }

  monitorSub.textContent = lines.join("\n");
}

function renderAuthors(items) {
  listEl.innerHTML = "";
  emptyHint.classList.toggle("hidden", items.length > 0);

  items.forEach((item) => {
    const node = itemTpl.content.firstElementChild.cloneNode(true);
    node.dataset.key = item.key;

    node.querySelector(".name").textContent = `${item.name}`;
    node.querySelector(".meta").textContent = `抖音号：${item.douyin_id}`;

    const profile = node.querySelector(".profile");
    profile.href = item.profile_url;
    profile.textContent = item.profile_url;

    const delBtn = node.querySelector(".danger");
    delBtn.addEventListener("click", async () => {
      const ok = window.confirm(`确认删除作者「${item.douyin_id}-${item.name}」吗？`);
      if (!ok) {
        return;
      }

      try {
        delBtn.disabled = true;
        setStatus("正在删除作者...");
        const encoded = encodeURIComponent(item.key);
        const result = await api(`/api/authors/${encoded}`, { method: "DELETE" });
        renderAuthors(result.items || []);
        setStatus("删除成功，后续任务不会再执行该作者。", "success");
      } catch (err) {
        setStatus(err.message, "error");
      } finally {
        delBtn.disabled = false;
      }
    });

    listEl.appendChild(node);
  });
}

async function loadAuthors() {
  try {
    refreshBtn.disabled = true;
    setStatus("正在加载作者列表...");
    const data = await api("/api/authors");
    renderAuthors(data.items || []);
    setStatus(`已加载 ${data.items?.length || 0} 位作者。`, "success");
  } catch (err) {
    setStatus(err.message, "error");
  } finally {
    refreshBtn.disabled = false;
  }
}

async function loadMonitor({ silent = false } = {}) {
  try {
    const data = await api("/api/monitor");
    renderMonitor(data.monitor || null);
  } catch (err) {
    if (!silent) {
      setStatus(err.message, "error");
    }
    renderMonitor(null);
  }
}

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const formData = new FormData(form);

  const payload = {
    profile_url: String(formData.get("profile_url") || "").trim(),
    douyin_id: String(formData.get("douyin_id") || "").trim(),
    name: String(formData.get("name") || "").trim(),
  };

  try {
    submitBtn.disabled = true;
    setStatus("正在保存作者并触发同步...");
    const data = await api("/api/authors", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    renderAuthors(data.items || []);
    renderMonitor(data.monitor || null);
    form.reset();
    setStatus(
      data.message === "created"
        ? "作者添加成功，已触发立即同步。"
        : "作者信息已更新，已触发立即同步。",
      "success"
    );
  } catch (err) {
    setStatus(err.message, "error");
  } finally {
    submitBtn.disabled = false;
  }
});

refreshBtn.addEventListener("click", () => {
  loadAuthors();
});

runNowBtn.addEventListener("click", async () => {
  try {
    runNowBtn.disabled = true;
    setStatus("已提交立即执行请求...");
    const data = await api("/api/monitor/run-now", { method: "POST" });
    renderMonitor(data.monitor || null);
    setStatus("后台任务已加入执行队列。", "success");
  } catch (err) {
    setStatus(err.message, "error");
  } finally {
    runNowBtn.disabled = false;
  }
});

loadAuthors();
loadMonitor();
window.setInterval(() => {
  loadMonitor({ silent: true });
}, 5000);
