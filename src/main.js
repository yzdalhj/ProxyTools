import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

const target = document.querySelector("#target");
const host = document.querySelector("#host");
const port = document.querySelector("#port");
const toggle = document.querySelector("#toggle");
const status = document.querySelector("#status");
const logList = document.querySelector("#log-list");
const logCount = document.querySelector("#log-count");

let logs = [];

let running = false;

function appendLog(entry) {
  logs = [...logs.slice(-199), { time: new Date(), ...entry }];
  logCount.textContent = logs.length || "";
  logList.replaceChildren(...logs.map(({ time, level, message }) => {
    const item = document.createElement("div");
    item.className = `log-entry ${level}`;
    const timeNode = document.createElement("span");
    timeNode.className = "log-time";
    timeNode.textContent = time.toLocaleTimeString("zh-CN", { hour12: false });
    const messageNode = document.createElement("span");
    messageNode.textContent = message;
    item.append(timeNode, messageNode);
    return item;
  }));
  logList.scrollTop = logList.scrollHeight;
}

invoke("get_local_ip").then((ip) => {
  host.value = ip;
}).catch((error) => {
  status.textContent = `未获取到本机 IP：${error}`;
  appendLog({ level: "error", message: `获取本机 IP 失败：${error}` });
});

listen("proxy-log", ({ payload }) => appendLog(payload));

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    const activeView = tab.dataset.view;
    document.querySelectorAll(".tab").forEach((item) => item.classList.toggle("active", item === tab));
    document.querySelectorAll(".view").forEach((view) => {
      view.hidden = view.id !== activeView;
    });
  });
});

document.querySelector("#clear-logs").addEventListener("click", () => {
  logs = [];
  logCount.textContent = "";
  logList.replaceChildren(Object.assign(document.createElement("p"), { className: "empty", textContent: "暂无运行记录" }));
});

window.addEventListener("beforeunload", () => {
  logs = [];
  localStorage.clear();
  sessionStorage.clear();
});

function setRunning(value, message) {
  running = value;
  document.body.dataset.running = value ? "true" : "false";
  target.disabled = value;
  host.disabled = value;
  port.disabled = value;
  toggle.textContent = value ? "停止转发" : "启动转发";
  status.textContent = message;
}

toggle.addEventListener("click", async () => {
  try {
    if (running) {
      await invoke("stop_proxy");
      setRunning(false, "已停止");
      return;
    }
    const result = await invoke("start_proxy", {
      target: target.value.trim(),
      host: host.value.trim(),
      port: Number(port.value),
    });
    setRunning(true, `运行中：${result}`);
  } catch (error) {
    status.textContent = `启动失败：${error}`;
    appendLog({ level: "error", message: `启动失败：${error}` });
  }
});
