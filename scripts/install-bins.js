import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import http from "node:http";
import https from "node:https";

import extract from "extract-zip";

const platformName =
  process.platform === "darwin" ? "macos" : process.platform === "win32" ? "windows" : "linux";
const BIN_DIR = path.resolve("src-tauri/bin", platformName);
const baseNames = ["ffmpeg", "ffprobe", "aria2c", "BaiduPCS-Go"];
const targetNames = process.platform === "win32" ? baseNames.map((name) => `${name}.exe`) : baseNames;
const defaultSourceDir = path.resolve(process.cwd(), "bin", platformName);
const sourceDir = process.env.BIN_SOURCE_DIR || defaultSourceDir;
const downloadEnabled = isTruthy(process.env.BIN_DOWNLOAD);
const arch = process.arch;

const DOWNLOAD_PACKAGES = {
  windows: {
    x64: [
      {
        id: "ffmpeg",
        envKey: "BIN_URL_FFMPEG",
        url: "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip",
        provides: ["ffmpeg.exe", "ffprobe.exe"],
      },
      {
        id: "aria2c",
        envKey: "BIN_URL_ARIA2C",
        url: "https://github.com/aria2/aria2/releases/download/release-1.37.0/aria2-1.37.0-win-64bit-build1.zip",
        provides: ["aria2c.exe"],
      },
      {
        id: "baidupcs",
        envKey: "BIN_URL_BAIDUPCS",
        url: "https://github.com/qjfoidnh/BaiduPCS-Go/releases/download/v4.0.0/BaiduPCS-Go-v4.0.0-windows-amd64.zip",
        provides: ["BaiduPCS-Go.exe"],
      },
    ],
  },
};

function ensureDir(pathname) {
  if (!fs.existsSync(pathname)) {
    fs.mkdirSync(pathname, { recursive: true });
  }
}

function copyFile(sourcePath, targetPath) {
  fs.copyFileSync(sourcePath, targetPath);
  if (process.platform !== "win32") {
    fs.chmodSync(targetPath, 0o755);
  }
}

function isTruthy(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());
}

function resolveDownloadPackages() {
  return DOWNLOAD_PACKAGES?.[platformName]?.[arch] || null;
}

function findFile(rootDir, filename) {
  const entries = fs.readdirSync(rootDir, { withFileTypes: true });
  for (const entry of entries) {
    const entryPath = path.join(rootDir, entry.name);
    if (entry.isFile() && entry.name === filename) {
      return entryPath;
    }
    if (entry.isDirectory()) {
      const found = findFile(entryPath, filename);
      if (found) {
        return found;
      }
    }
  }
  return null;
}

function downloadFile(url, targetPath) {
  return new Promise((resolve, reject) => {
    const request = (nextUrl) => {
      const client = nextUrl.startsWith("https:") ? https : http;
      const req = client.get(
        nextUrl,
        { headers: { "User-Agent": "reaction-cut-rust/install-bins" } },
        (res) => {
          if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
            const redirectUrl = new URL(res.headers.location, nextUrl).toString();
            request(redirectUrl);
            return;
          }
          if (res.statusCode !== 200) {
            reject(new Error(`下载失败: ${nextUrl} status=${res.statusCode}`));
            return;
          }
          const file = fs.createWriteStream(targetPath);
          res.pipe(file);
          file.on("finish", () => file.close(resolve));
        },
      );
      req.on("error", reject);
    };
    request(url);
  });
}

async function downloadPackage(pkg, targetDir) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "reaction-cut-bins-"));
  try {
    const url = process.env[pkg.envKey] || pkg.url;
    const zipPath = path.join(tempDir, `${pkg.id}.zip`);
    await downloadFile(url, zipPath);
    await extract(zipPath, { dir: tempDir });
    for (const name of pkg.provides) {
      const sourcePath = findFile(tempDir, name);
      if (!sourcePath) {
        throw new Error(`解压后未找到 ${name}`);
      }
      copyFile(sourcePath, path.join(targetDir, name));
    }
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

async function main() {
  ensureDir(BIN_DIR);

  const missing = [];
  for (const name of targetNames) {
    const targetPath = path.join(BIN_DIR, name);
    if (fs.existsSync(targetPath)) {
      continue;
    }
    const sourcePath = path.join(sourceDir, name);
    if (fs.existsSync(sourcePath)) {
      copyFile(sourcePath, targetPath);
      continue;
    }
    missing.push(name);
  }

  if (missing.length > 0 && !downloadEnabled) {
    throw new Error(
      `缺少二进制文件: ${missing.join(
        ", ",
      )}。可设置 BIN_SOURCE_DIR 指向已下载目录，或设置 BIN_DOWNLOAD=1 自动下载（仅支持 Windows x64）。`,
    );
  }

  if (missing.length > 0 && downloadEnabled) {
    const packages = resolveDownloadPackages();
    if (!packages) {
      throw new Error(
        `当前平台/架构不支持自动下载: ${platformName}/${arch}，请使用 BIN_SOURCE_DIR 手动提供二进制文件。`,
      );
    }
    const missingSet = new Set(missing);
    for (const pkg of packages) {
      if (!pkg.provides.some((name) => missingSet.has(name))) {
        continue;
      }
      await downloadPackage(pkg, BIN_DIR);
      for (const name of pkg.provides) {
        missingSet.delete(name);
      }
    }
    if (missingSet.size > 0) {
      throw new Error(`自动下载后仍缺少二进制文件: ${Array.from(missingSet).join(", ")}`);
    }
  }

  console.log(`二进制已写入: ${BIN_DIR}`);
}

try {
  await main();
} catch (error) {
  console.error(error);
  process.exit(1);
}
