import { useState } from "react";

const COVER_ASPECT_RATIO = 16 / 10;
const COVER_MIN_WIDTH = 960;
const COVER_MIN_HEIGHT = 600;

function normalizeCoverUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  if (raw.startsWith("data:image/")) {
    return raw;
  }
  if (raw.startsWith("//")) {
    return `https:${raw}`;
  }
  if (raw.startsWith("http://")) {
    return `https://${raw.slice("http://".length)}`;
  }
  return raw;
}

export default function useSubmissionCover({
  openDialog,
  convertFileSrc,
  invokeCommand,
  setTaskForm,
  setMessage,
}) {
  const [coverCropOpen, setCoverCropOpen] = useState(false);
  const [coverCropSourceUrl, setCoverCropSourceUrl] = useState("");
  const [coverCropSourcePath, setCoverCropSourcePath] = useState("");
  const [coverUploading, setCoverUploading] = useState(false);
  const [coverPreviewUrl, setCoverPreviewUrl] = useState("");

  const loadLocalCoverPreview = async (filePath) => {
    const dataUrl = await invokeCommand("submission_cover_local_preview", {
      request: {
        filePath,
      },
    });
    return String(dataUrl || "").trim();
  };

  const resetCoverState = () => {
    setCoverCropOpen(false);
    setCoverCropSourceUrl("");
    setCoverCropSourcePath("");
    setCoverUploading(false);
    setCoverPreviewUrl("");
  };

  const handleSelectCoverFile = async () => {
    setMessage("");
    try {
      const selected = await openDialog({
        multiple: false,
        directory: false,
        filters: [
          {
            name: "图片文件",
            extensions: ["jpg", "jpeg", "png", "webp"],
          },
        ],
      });
      if (typeof selected !== "string") {
        return;
      }
      const selectedPath = selected.trim();
      if (!selectedPath) {
        setMessage("封面文件路径无效，请重新选择");
        return;
      }
      let previewSource = "";
      try {
        previewSource = await loadLocalCoverPreview(selectedPath);
      } catch (error) {
        previewSource = String(convertFileSrc(selectedPath) || "");
        if (!previewSource) {
          throw error;
        }
        setMessage(
          `本地预览转换失败，已尝试直接加载文件：${error?.message || "未知错误"}`,
        );
      }
      if (!previewSource) {
        setMessage("封面预览地址生成失败，请重新选择");
        return;
      }
      setCoverCropSourcePath(selectedPath);
      setCoverCropSourceUrl(previewSource);
      setCoverCropOpen(true);
    } catch (error) {
      setMessage(error.message || "选择封面失败");
    }
  };

  const handleCloseCoverCrop = () => {
    if (coverUploading) {
      return;
    }
    setCoverCropOpen(false);
    setCoverCropSourceUrl("");
    setCoverCropSourcePath("");
  };

  const handleCoverCropImageError = async () => {
    if (!coverCropSourcePath) {
      setMessage("封面预览加载失败，请重新选择图片");
      return;
    }
    if (coverCropSourceUrl.startsWith("data:image/")) {
      setMessage("封面预览加载失败，请重新选择图片");
      return;
    }
    try {
      const fallbackDataUrl = await loadLocalCoverPreview(coverCropSourcePath);
      if (!fallbackDataUrl) {
        throw new Error("封面预览地址为空");
      }
      setCoverCropSourceUrl(fallbackDataUrl);
      setMessage("封面预览已自动切换为兼容模式");
    } catch (error) {
      setMessage(error?.message || "封面预览加载失败，请重新选择图片");
    }
  };

  const handleConfirmCoverCrop = async ({ dataUrl, width, height }) => {
    if (coverUploading) {
      return;
    }
    setCoverUploading(true);
    setMessage("");
    try {
      const coverUrl = await invokeCommand("submission_upload_cover", {
        request: {
          imageDataUrl: dataUrl,
        },
      });
      const normalizedCoverUrl = normalizeCoverUrl(coverUrl);
      setTaskForm((prev) => ({
        ...prev,
        coverUrl: normalizedCoverUrl,
        coverDataUrl: dataUrl,
      }));
      // Use the local cropped image for instant preview in UI.
      setCoverPreviewUrl(normalizeCoverUrl(dataUrl));
      setCoverCropOpen(false);
      setCoverCropSourceUrl("");
      setCoverCropSourcePath("");
      setMessage(`封面上传成功（${width}x${height}）`);
    } catch (error) {
      setMessage(error?.message || "封面上传失败");
    } finally {
      setCoverUploading(false);
    }
  };

  const handleClearCover = () => {
    setTaskForm((prev) => ({
      ...prev,
      coverUrl: "",
      coverLocalPath: "",
      coverDataUrl: "",
    }));
    setCoverPreviewUrl("");
    setMessage("已清空封面");
  };

  return {
    coverAspectRatio: COVER_ASPECT_RATIO,
    coverMinWidth: COVER_MIN_WIDTH,
    coverMinHeight: COVER_MIN_HEIGHT,
    coverCropOpen,
    coverCropSourceUrl,
    coverUploading,
    coverPreviewUrl,
    resetCoverState,
    handleSelectCoverFile,
    handleCloseCoverCrop,
    handleCoverCropImageError,
    handleConfirmCoverCrop,
    handleClearCover,
  };
}
