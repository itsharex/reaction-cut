import { useEffect, useMemo, useRef, useState } from "react";

function clamp(value, min, max) {
  if (!Number.isFinite(value)) {
    return min;
  }
  return Math.min(Math.max(value, min), max);
}

function resolveFrameSpec(width, height, aspectRatio, minWidth, minHeight) {
  const imageWidth = Number(width) || 0;
  const imageHeight = Number(height) || 0;
  if (imageWidth <= 0 || imageHeight <= 0) {
    return { ready: false };
  }
  const minAllowedWidth = Math.ceil(Math.max(minWidth, minHeight * aspectRatio));
  const maxWidth = Math.floor(Math.min(imageWidth, imageHeight * aspectRatio));
  if (maxWidth <= 0) {
    return { ready: false };
  }
  const tooSmall = maxWidth < minAllowedWidth;
  // Initialize with the largest 16:10 frame that fits the uploaded image.
  const defaultWidth = maxWidth;
  const defaultHeight = Math.floor(defaultWidth / aspectRatio);
  return {
    ready: true,
    tooSmall,
    width: defaultWidth,
    height: defaultHeight,
    maxX: Math.max(0, imageWidth - defaultWidth),
    maxY: Math.max(0, imageHeight - defaultHeight),
    minAllowedWidth,
    minAllowedHeight: Math.floor(minAllowedWidth / aspectRatio),
  };
}

export default function CoverCropModal({
  open,
  imageSrc,
  aspectRatio = 16 / 10,
  minWidth = 960,
  minHeight = 600,
  submitting = false,
  onClose,
  onConfirm,
  onImageLoadError,
}) {
  const imageRef = useRef(null);
  const previewCanvasRef = useRef(null);
  const dragRef = useRef({
    active: false,
    pointerId: null,
    startClientX: 0,
    startClientY: 0,
    startX: 0,
    startY: 0,
    scaleX: 1,
    scaleY: 1,
  });

  const [naturalSize, setNaturalSize] = useState({ width: 0, height: 0 });
  const [framePos, setFramePos] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(false);
  const [localError, setLocalError] = useState("");

  const frameSpec = useMemo(() => {
    return resolveFrameSpec(
      naturalSize.width,
      naturalSize.height,
      aspectRatio,
      minWidth,
      minHeight,
    );
  }, [aspectRatio, minHeight, minWidth, naturalSize.height, naturalSize.width]);

  useEffect(() => {
    if (!open) {
      return;
    }
    dragRef.current.active = false;
    setDragging(false);
    setLocalError("");
    setNaturalSize({ width: 0, height: 0 });
    setFramePos({ x: 0, y: 0 });
  }, [open, imageSrc]);

  useEffect(() => {
    if (!frameSpec.ready || frameSpec.tooSmall) {
      return;
    }
    setFramePos((prev) => {
      const nextX = clamp(prev.x, 0, frameSpec.maxX);
      const nextY = clamp(prev.y, 0, frameSpec.maxY);
      if (nextX === prev.x && nextY === prev.y) {
        return prev;
      }
      return { x: nextX, y: nextY };
    });
  }, [frameSpec]);

  useEffect(() => {
    if (!open || !frameSpec.ready || frameSpec.tooSmall) {
      return;
    }
    const canvas = previewCanvasRef.current;
    const image = imageRef.current;
    if (!canvas || !image) {
      return;
    }

    canvas.width = 320;
    canvas.height = Math.round(320 / aspectRatio);
    const ctx = canvas.getContext("2d");
    if (!ctx) {
      return;
    }

    const cropX = Math.round(framePos.x);
    const cropY = Math.round(framePos.y);
    const cropWidth = frameSpec.width;
    const cropHeight = frameSpec.height;

    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(
      image,
      cropX,
      cropY,
      cropWidth,
      cropHeight,
      0,
      0,
      canvas.width,
      canvas.height,
    );
  }, [aspectRatio, framePos.x, framePos.y, frameSpec, open]);

  if (!open) {
    return null;
  }

  const handleImageLoaded = (event) => {
    const image = event.currentTarget;
    const nextWidth = Number(image.naturalWidth) || 0;
    const nextHeight = Number(image.naturalHeight) || 0;
    setNaturalSize({ width: nextWidth, height: nextHeight });
    const nextSpec = resolveFrameSpec(
      nextWidth,
      nextHeight,
      aspectRatio,
      minWidth,
      minHeight,
    );
    if (nextSpec.ready && !nextSpec.tooSmall) {
      setFramePos({
        x: Math.floor((nextWidth - nextSpec.width) / 2),
        y: Math.floor((nextHeight - nextSpec.height) / 2),
      });
    } else {
      setFramePos({ x: 0, y: 0 });
    }
    setLocalError("");
  };

  const handleImageLoadError = () => {
    dragRef.current.active = false;
    setDragging(false);
    setNaturalSize({ width: 0, height: 0 });
    setFramePos({ x: 0, y: 0 });
    setLocalError("封面图片加载失败，请重新选择");
    if (typeof onImageLoadError === "function") {
      onImageLoadError();
    }
  };

  const handleFramePointerDown = (event) => {
    if (!frameSpec.ready || frameSpec.tooSmall || submitting) {
      return;
    }
    const image = imageRef.current;
    if (!image) {
      return;
    }
    const rect = image.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) {
      return;
    }

    dragRef.current = {
      active: true,
      pointerId: event.pointerId,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startX: framePos.x,
      startY: framePos.y,
      scaleX: naturalSize.width / rect.width,
      scaleY: naturalSize.height / rect.height,
    };
    setDragging(true);
    event.currentTarget.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  };

  const handleFramePointerMove = (event) => {
    const drag = dragRef.current;
    if (!drag.active || drag.pointerId !== event.pointerId) {
      return;
    }
    const deltaX = (event.clientX - drag.startClientX) * drag.scaleX;
    const deltaY = (event.clientY - drag.startClientY) * drag.scaleY;
    const nextX = clamp(drag.startX + deltaX, 0, frameSpec.maxX);
    const nextY = clamp(drag.startY + deltaY, 0, frameSpec.maxY);
    setFramePos((prev) => {
      if (prev.x === nextX && prev.y === nextY) {
        return prev;
      }
      return { x: nextX, y: nextY };
    });
  };

  const handleFramePointerUp = (event) => {
    const drag = dragRef.current;
    if (!drag.active || drag.pointerId !== event.pointerId) {
      return;
    }
    dragRef.current.active = false;
    setDragging(false);
    event.currentTarget.releasePointerCapture?.(event.pointerId);
  };

  const handleConfirm = async () => {
    if (!frameSpec.ready || frameSpec.tooSmall) {
      return;
    }
    const image = imageRef.current;
    if (!image) {
      setLocalError("未加载到封面图片，请重新选择");
      return;
    }

    const width = frameSpec.width;
    const height = frameSpec.height;
    if (width < minWidth || height < minHeight) {
      setLocalError(`裁剪尺寸过小，至少 ${minWidth}x${minHeight}`);
      return;
    }

    const exportCanvas = document.createElement("canvas");
    exportCanvas.width = width;
    exportCanvas.height = height;
    const ctx = exportCanvas.getContext("2d");
    if (!ctx) {
      setLocalError("封面裁剪失败，请重试");
      return;
    }

    const cropX = Math.round(framePos.x);
    const cropY = Math.round(framePos.y);
    ctx.drawImage(
      image,
      cropX,
      cropY,
      width,
      height,
      0,
      0,
      exportCanvas.width,
      exportCanvas.height,
    );

    const dataUrl = exportCanvas.toDataURL("image/jpeg", 0.92);
    await onConfirm({
      dataUrl,
      width,
      height,
    });
  };

  const frameStyle =
    naturalSize.width > 0 && naturalSize.height > 0 && frameSpec.ready && !frameSpec.tooSmall
      ? {
          left: `${(framePos.x / naturalSize.width) * 100}%`,
          top: `${(framePos.y / naturalSize.height) * 100}%`,
          width: `${(frameSpec.width / naturalSize.width) * 100}%`,
          height: `${(frameSpec.height / naturalSize.height) * 100}%`,
        }
      : {};

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/35 px-4">
      <div className="w-full max-w-5xl rounded-2xl bg-white p-5 shadow-lg">
        <div className="flex items-center justify-between gap-3">
          <div className="text-sm font-semibold text-[var(--ink)]">封面裁剪（16:10）</div>
          <button
            className="rounded-full border border-black/10 bg-white px-3 py-1 text-xs font-semibold text-[var(--ink)]"
            onClick={onClose}
            disabled={submitting}
          >
            关闭
          </button>
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-[1fr_340px]">
          <div className="rounded-xl border border-black/10 bg-black/5 p-3">
            <div className="relative mx-auto w-fit max-w-full overflow-hidden rounded-lg">
              <img
                ref={imageRef}
                src={imageSrc}
                alt="封面原图"
                className="block max-h-[420px] max-w-full select-none"
                onLoad={handleImageLoaded}
                onError={handleImageLoadError}
                draggable={false}
              />
              {frameSpec.ready && !frameSpec.tooSmall ? (
                <div
                  className={`absolute border-2 border-white shadow-[0_0_0_9999px_rgba(0,0,0,0.35)] ${
                    submitting ? "cursor-not-allowed" : dragging ? "cursor-grabbing" : "cursor-grab"
                  }`}
                  style={frameStyle}
                  onPointerDown={handleFramePointerDown}
                  onPointerMove={handleFramePointerMove}
                  onPointerUp={handleFramePointerUp}
                  onPointerCancel={handleFramePointerUp}
                >
                  <div className="pointer-events-none absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 rounded bg-black/40 px-2 py-0.5 text-[10px] text-white">
                    拖动裁剪框
                  </div>
                </div>
              ) : null}
            </div>
          </div>

          <div className="space-y-3 rounded-xl border border-black/10 bg-white p-3">
            <div className="text-xs text-[var(--muted)]">
              原图尺寸：{naturalSize.width || "-"} x {naturalSize.height || "-"}
            </div>
            <div className="text-xs text-[var(--muted)]">
              裁剪框尺寸：{frameSpec.ready ? `${frameSpec.width} x ${frameSpec.height}` : "-"}
            </div>
            <div className="text-xs text-[var(--muted)]">
              最小要求：{minWidth} x {minHeight}（16:10）
            </div>
            {frameSpec.ready && !frameSpec.tooSmall ? (
              <div className="text-xs text-[var(--muted)]">
                裁剪位置：x={Math.round(framePos.x)}，y={Math.round(framePos.y)}
              </div>
            ) : null}
            {frameSpec.ready && frameSpec.tooSmall ? (
              <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                图片尺寸不足，至少需要 {minWidth}x{minHeight}
              </div>
            ) : (
              <div className="text-xs text-[var(--muted)]">请在左侧直接拖动裁剪框选择封面区域。</div>
            )}
            <canvas
              ref={previewCanvasRef}
              className="h-auto w-full rounded-lg border border-black/10 bg-black/5"
            />
            {localError ? (
              <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                {localError}
              </div>
            ) : null}
            <div className="flex justify-end gap-2">
              <button
                className="rounded-full border border-black/10 bg-white px-3 py-1 text-xs font-semibold text-[var(--ink)]"
                onClick={onClose}
                disabled={submitting}
              >
                取消
              </button>
              <button
                className="rounded-full bg-[var(--accent)] px-3 py-1 text-xs font-semibold text-white disabled:cursor-not-allowed disabled:opacity-60"
                onClick={handleConfirm}
                disabled={!frameSpec.ready || frameSpec.tooSmall || submitting}
              >
                {submitting ? "上传中" : "确认并上传"}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
