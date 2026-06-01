export function findMappedPreviewTarget(root, caretLine) {
  const blocks = Array.from(root.querySelectorAll('.preview-map-block[data-src-start][data-src-end]'));
  if (!blocks.length) return null;
  let best = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const element of blocks) {
    const startLine = Number(element.getAttribute('data-src-start'));
    const endLine = Number(element.getAttribute('data-src-end'));
    if (!Number.isFinite(startLine) || !Number.isFinite(endLine)) continue;
    if (caretLine >= startLine && caretLine <= endLine) {
      const lineCount = Math.max(1, endLine - startLine + 1);
      const localRatio = Math.min(1, Math.max(0, (caretLine - startLine + 0.5) / lineCount));
      return { element, localRatio };
    }
    const distance = Math.min(Math.abs(caretLine - startLine), Math.abs(caretLine - endLine));
    if (distance < bestDistance) {
      bestDistance = distance;
      best = { element, localRatio: caretLine < startLine ? 0 : 1 };
    }
  }
  return best;
}

export function centeredRatioFromLine(caretLine, totalLines, viewportHeight, scrollHeight) {
  const normalizedTotal = Math.max(1, totalLines - 1);
  const ratio = normalizedTotal > 0 ? caretLine / normalizedTotal : 0;
  const viewportFraction = scrollHeight > 0 ? viewportHeight / scrollHeight : 0;
  return Math.max(0, Math.min(1, ratio - (viewportFraction / 2)));
}

export function centerMappedTarget(scroller, target) {
  if (!scroller || !target) return false;
  const scrollerRect = scroller.getBoundingClientRect();
  const targetRect = target.element.getBoundingClientRect();
  const anchorY = targetRect.top + targetRect.height * target.localRatio;
  const next = scroller.scrollTop + (anchorY - scrollerRect.top) - (scroller.clientHeight / 2);
  const max = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
  scroller.scrollTop = Math.max(0, Math.min(max, next));
  return true;
}

export function syncPlainScroller(scroller, caretLine, totalLines) {
  if (!scroller) return;
  const ratio = centeredRatioFromLine(caretLine, totalLines, scroller.clientHeight, scroller.scrollHeight);
  const max = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
  scroller.scrollTop = ratio * max;
}
