import { useEffect, useState } from "react";
import { confirm as dialogConfirm, message as dialogMessage } from "@tauri-apps/plugin-dialog";
import pkg from "../../package.json";
import { invokeCommand } from "../lib/tauri";
import appLogo from "../../src-tauri/icons/128x128.png";

const actionLinks = [
  {
    buttonText: "作者主页",
    url: "https://space.bilibili.com/82679456?spm_id_from=333.788.0.0",
  },
  {
    buttonText: "GitHub仓库",
    url: "https://github.com/UknowNull/biliClipFlow",
  },
  {
    buttonText: "视频教程",
    url: "https://www.bilibili.com/video/BV1P4r9BfESr/?spm_id_from=333.1387.homepage.video_card.click",
  },
  {
    buttonText: "反馈地址",
    url: "https://github.com/UknowNull/biliClipFlow/issues",
  },
];


const ExternalLinkIcon = () => (
  <svg
    viewBox="0 0 24 24"
    width="14"
    height="14"
    aria-hidden="true"
    focusable="false"
  >
    <path
      d="M14 3h7v7h-2V6.41l-9.29 9.3-1.42-1.42 9.3-9.29H14V3z"
      fill="currentColor"
    />
    <path
      d="M5 5h6v2H7v10h10v-4h2v6H5V5z"
      fill="currentColor"
    />
  </svg>
);

const openExternal = async (url) => {
  if (!url) {
    return;
  }
  try {
    const { openUrl } = await import("@tauri-apps/plugin-opener");
    await openUrl(url);
  } catch (_) {}
};

export default function AboutSection() {
  const [version, setVersion] = useState(pkg?.version || "unknown");
  const [checkingUpdate, setCheckingUpdate] = useState(false);

  useEffect(() => {
    let active = true;
    const loadVersion = async () => {
      try {
        const data = await invokeCommand("app_version");
        const nextVersion = String(data?.version || "").trim();
        if (active && nextVersion) {
          setVersion(nextVersion);
        }
      } catch (_) {}
    };
    loadVersion();
    return () => {
      active = false;
    };
  }, []);

  const handleCheckUpdate = async () => {
    if (checkingUpdate) {
      return;
    }
    setCheckingUpdate(true);
    try {
      const data = await invokeCommand("app_update_check");
      if (!data?.hasUpdate) {
        await dialogMessage("已是最新版本。", { title: "检查更新" });
        return;
      }
      const latestVersion = String(data?.latestVersion || "").trim();
      const currentVersion = String(data?.currentVersion || "").trim();
      const confirmMessage = `发现新版本 v${latestVersion}（当前 v${currentVersion}），是否前往更新页面？`;
      const confirmed = await dialogConfirm(confirmMessage, {
        title: "发现新版本",
      });
      if (!confirmed) {
        return;
      }
      const updateUrl = String(data?.downloadUrl || data?.releaseUrl || "").trim();
      if (updateUrl) {
        await openExternal(updateUrl);
      }
    } catch (error) {
      await dialogMessage(`检查更新失败：${error?.message || String(error)}`, {
        title: "检查更新",
      });
    } finally {
      setCheckingUpdate(false);
    }
  };

  return (
    <div className="space-y-3">
      <div className="panel p-4 space-y-2">
        <div className="space-y-1">
          <div className="flex items-center gap-3">
            <img
              src={appLogo}
              alt="biliClipFlow Logo"
              className="h-32 w-32 rounded-xl border border-black/10 bg-white object-cover"
            />
            <div className="text-lg font-semibold text-[var(--content-color)]">biliClipFlow</div>
          </div>
          <div className="desc leading-loose">
            介绍：支持直播间订阅与手动/自动录制（含弹幕录制配置），支持视频下载（分P选择、多视频、下载+投稿），提供投稿任务的剪辑/合并/分段/更新与重试管理，并内置 FLV 转 MP4 转封装工具。
          </div>
        </div>
        <div className="space-y-1 text-sm leading-loose text-[var(--content-color)]">
          <div className="flex flex-wrap items-center gap-2">
            <span>版本：{version}</span>
            <button
              type="button"
              className="inline-flex items-center gap-2 rounded-full border border-[var(--primary-color)] bg-[var(--primary-color)] px-3 py-1 text-xs text-white transition hover:brightness-95 disabled:opacity-60"
              onClick={handleCheckUpdate}
              disabled={checkingUpdate}
            >
              {checkingUpdate ? "检查中..." : "检查更新"}
            </button>
          </div>
          <div className="flex flex-wrap gap-2">
            {actionLinks.map((item) => (
              <button
                key={item.buttonText}
                className="inline-flex items-center gap-2 rounded-full border border-[var(--split-color)] bg-[var(--solid-button-color)] px-3 py-1 text-xs text-[var(--content-color)] transition hover:border-[var(--primary-color)]"
                onClick={() => openExternal(item.url)}
                type="button"
              >
                <ExternalLinkIcon />
                {item.buttonText}
              </button>
            ))}
          </div>
          <div>本软件为开源软件，许可证：GPLv3</div>
          <div className="text-[var(--desc-color)]">
            此软件为公益免费项目。如果你付费购买了此软件，你可能被骗了。
          </div>
          <div className="text-[var(--desc-color)]">
            觉得好用的话，在 GitHub 给这个项目点个 Star 吧！
          </div>
        </div>
      </div>

    </div>
  );
}
