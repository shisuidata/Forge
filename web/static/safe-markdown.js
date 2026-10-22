// Render untrusted Markdown (model replies) into inert DOM. marked does not sanitize, so its HTML is parsed
// in a detached document and only Markdown formatting elements are copied; attributes, raw HTML and script
// URLs never reach the page.
(function () {
  const ALLOWED = new Set([
    "P", "BR", "HR", "H1", "H2", "H3", "H4", "H5", "H6", "UL", "OL", "LI", "STRONG", "EM", "DEL",
    "CODE", "PRE", "BLOCKQUOTE", "TABLE", "THEAD", "TBODY", "TR", "TH", "TD", "A",
  ]);
  const DROPPED = new Set(["SCRIPT", "STYLE", "TEMPLATE", "IFRAME", "OBJECT", "EMBED", "SVG", "MATH", "NOSCRIPT"]);
  const SAFE_HREF = /^(https?:|mailto:|\/(?!\/)|#)/i;

  function copySafe(source, target) {
    for (const node of source.childNodes) {
      if (node.nodeType === Node.TEXT_NODE) {
        target.append(node.data);
        continue;
      }
      if (node.nodeType !== Node.ELEMENT_NODE || DROPPED.has(node.tagName.toUpperCase())) continue;
      if (!ALLOWED.has(node.tagName)) {
        copySafe(node, target);
        continue;
      }
      const copy = document.createElement(node.tagName.toLowerCase());
      if (node.tagName === "A") {
        const href = (node.getAttribute("href") || "").trim();
        if (SAFE_HREF.test(href)) {
          copy.setAttribute("href", href);
          copy.setAttribute("rel", "noopener noreferrer");
          copy.setAttribute("target", "_blank");
        }
      }
      const align = node.getAttribute("align");
      if ((node.tagName === "TH" || node.tagName === "TD") && /^(left|right|center)$/.test(align || "")) {
        copy.setAttribute("align", align);
      }
      copySafe(node, copy);
      target.append(copy);
    }
  }

  window.renderSafeMarkdown = function renderSafeMarkdown(markdown) {
    const fragment = document.createDocumentFragment();
    const text = String(markdown ?? "");
    if (typeof marked === "undefined") {
      fragment.append(text);
      return fragment;
    }
    const parsed = new DOMParser().parseFromString(marked.parse(text), "text/html");
    copySafe(parsed.body, fragment);
    return fragment;
  };
})();
