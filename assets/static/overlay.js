(() => {
  const configScript = document.getElementById("overlay-config");
  const config = JSON.parse(configScript?.textContent || "{}");
  const eventsUrl = config.eventsUrl || "/api/current-print/events";
  const selectedDeviceId = String(config.selectedDeviceId || "").trim();
  const state = {
    title: document.getElementById("title"),
    fileName: document.getElementById("fileName"),
    timeEstimate: document.getElementById("timeEstimate"),
    printWeight: document.getElementById("printWeight"),
    progressPercent: document.getElementById("progressPercent"),
    layerInfo: document.getElementById("layerInfo"),
    remainingTime: document.getElementById("remainingTime"),
    progress: document.getElementById("progress"),
    toolheadTemp: document.getElementById("toolheadTemp"),
    bedTemp: document.getElementById("bedTemp"),
    fanSpeed: document.getElementById("fanSpeed"),
    printSpeed: document.getElementById("printSpeed"),
    spoolList: document.getElementById("spoolList"),
    thumbSlot: document.getElementById("thumbSlot"),
    thumbUrl: null,
    thumbKey: null,
    thumbPendingUrl: null,
    thumbPendingKey: null,
    thumbFailedUrl: null,
    thumbFailedKey: null,
    thumbRetryTimer: null,
    thumbObjectUrl: null,
    thumbRequest: 0,
    connectionBubble: document.getElementById("connectionBubble"),
    events: null,
    spoolIconId: 0,
  };

  function pickDevice(devices) {
    if (selectedDeviceId) {
      return devices.find((device) => device.id === selectedDeviceId) || devices[0] || null;
    }
    return devices[0] || null;
  }

  function setText(node, value) {
    node.textContent = value == null || value === "" ? "" : String(value);
  }

  function setOptionalText(node, value) {
    const text = value == null || value === "" ? "" : String(value);
    node.textContent = text;
    node.hidden = text === "";
  }

  function fallback(value, empty = "--") {
    return value == null || value === "" ? empty : String(value);
  }

  function layerText(device) {
    if (device.layerCurrent == null && device.layerTotal == null) {
      return "Layer -- / --";
    }
    return `Layer ${fallback(device.layerCurrent)} / ${fallback(device.layerTotal)}`;
  }

  function progressText(progress) {
    return progress == null ? "--%" : `${Math.round(progress)}%`;
  }

  function spoolSvg() {
    const spoolIconId = ++state.spoolIconId;
    const filamentBodyId = `spool-filament-body-${spoolIconId}`;
    const filamentClipId = `spool-filament-clip-${spoolIconId}`;
    const cutoutId = `spool-cutout-${spoolIconId}`;

    return `
      <svg viewBox="0 0 178 200" aria-hidden="true">
        <defs>
          <path id="${filamentBodyId}" d="M58 24 A66 12 0 0 1 124 24 A44.91 76 0 0 1 124 176 A66 12 0 0 1 58 176 A44.91 76 0 0 0 58 24 Z"/>
          <clipPath id="${filamentClipId}">
            <use href="#${filamentBodyId}"/>
          </clipPath>
          <path id="${cutoutId}" d="M-18 -35 A26 36 0 0 1 -29 -70 A31 18 0 0 1 0 -84 A31 18 0 0 1 29 -70 A26 36 0 0 1 18 -35 A40 40 0 0 0 -18 -35 Z"/>
        </defs>
        <ellipse cx="126" cy="100" rx="52" ry="88" fill="#4f5a70" stroke="#000" stroke-width="1" vector-effect="non-scaling-stroke"/>
        <use href="#${filamentBodyId}" fill="currentColor"/>
        <g clip-path="url(#${filamentClipId})" fill="none" stroke="#111827" stroke-width="1.6" stroke-linecap="round" opacity=".24">
          <path d="M74.5 24 A44.91 76 0 0 1 74.5 176"/>
          <path d="M91 24 A44.91 76 0 0 1 91 176"/>
          <path d="M107.5 24 A44.91 76 0 0 1 107.5 176"/>
        </g>
        <g transform="translate(58 100) scale(.6 1)">
          <circle r="88" fill="#4f5a70" stroke="#000" stroke-width="1" vector-effect="non-scaling-stroke"/>
          <g fill="currentColor" stroke="#252b3d" stroke-width="2" stroke-linejoin="round" vector-effect="non-scaling-stroke">
            <use href="#${cutoutId}" transform="translate(0 6)"/>
            <use href="#${cutoutId}" transform="rotate(120) translate(0 6)"/>
            <use href="#${cutoutId}" transform="rotate(240) translate(0 6)"/>
          </g>
          <circle r="26" fill="#111827"/>
        </g>
      </svg>
    `;
  }

  function spoolElement(material) {
    const el = document.createElement("div");
    el.className = "spool";
    el.classList.toggle("is-active", material.active === true);

    const roll = document.createElement("div");
    roll.className = "spool-roll";
    roll.style.setProperty("--spool-color", material.color || "#9ca3af");
    roll.innerHTML = spoolSvg();

    const tag = document.createElement("span");
    tag.className = "spool-tag";
    tag.textContent = material.label || "?";

    const kind = document.createElement("span");
    kind.className = "spool-material";
    kind.textContent = material.kind || "Filament";

    roll.append(tag);
    el.append(roll, kind);
    return el;
  }

  function renderMaterials(node, materials, emptyText) {
    const items = Array.isArray(materials) ? materials.filter(Boolean) : [];
    if (items.length === 0) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = emptyText;
      node.replaceChildren(empty);
      return;
    }

    node.replaceChildren(...items.map(spoolElement));
  }

  function clearThumbRetry() {
    if (state.thumbRetryTimer != null) {
      window.clearTimeout(state.thumbRetryTimer);
      state.thumbRetryTimer = null;
    }
  }

  function revokeThumbObjectUrl() {
    if (state.thumbObjectUrl) {
      URL.revokeObjectURL(state.thumbObjectUrl);
      state.thumbObjectUrl = null;
    }
  }

  function setThumbLoading() {
    revokeThumbObjectUrl();
    state.thumbSlot.replaceChildren();
    state.thumbSlot.className = "thumb";
    state.thumbSlot.classList.add("is-loading");
    state.thumbSlot.removeAttribute("aria-label");
    state.thumbSlot.setAttribute("aria-busy", "true");
  }

  function setThumbEmpty() {
    revokeThumbObjectUrl();
    state.thumbSlot.replaceChildren();
    state.thumbSlot.className = "thumb";
    state.thumbSlot.classList.add("is-empty");
    state.thumbSlot.textContent = "3D";
    state.thumbSlot.removeAttribute("aria-busy");
    state.thumbSlot.setAttribute("aria-label", "No print thumbnail available");
  }

  function retryDelay(response) {
    const seconds = Number(response.headers.get("retry-after"));
    if (Number.isFinite(seconds) && seconds > 0) {
      return Math.min(seconds * 1000, 10000);
    }
    return 2000;
  }

  function renderThumb(url, key = url, force = false) {
    if (!url) {
      if (!force && state.thumbUrl == null && state.thumbPendingUrl == null) {
        return;
      }
      clearThumbRetry();
      ++state.thumbRequest;
      state.thumbUrl = null;
      state.thumbKey = null;
      state.thumbPendingUrl = null;
      state.thumbPendingKey = null;
      state.thumbFailedUrl = null;
      state.thumbFailedKey = null;
      setThumbEmpty();
      return;
    }

    if (!force && url === state.thumbFailedUrl && key === state.thumbFailedKey) {
      return;
    }

    const isRendered = url === state.thumbUrl && key === state.thumbKey;
    const isPending = url === state.thumbPendingUrl && key === state.thumbPendingKey;
    if (!force && (isRendered || isPending)) {
      return;
    }

    clearThumbRetry();
    const requestId = ++state.thumbRequest;
    state.thumbPendingUrl = url;
    state.thumbPendingKey = key;

    setThumbLoading();
    fetch(url, { cache: "no-store" })
      .then((response) => {
        if (
          requestId !== state.thumbRequest ||
          state.thumbPendingUrl !== url ||
          state.thumbPendingKey !== key
        ) {
          return null;
        }
        if (response.status === 202) {
          state.thumbRetryTimer = window.setTimeout(
            () => renderThumb(url, key, true),
            retryDelay(response),
          );
          return null;
        }
        if (!response.ok) {
          throw new Error(`thumbnail request returned HTTP ${response.status}`);
        }
        return response.blob();
      })
      .then((blob) => {
        if (
          !blob ||
          requestId !== state.thumbRequest ||
          state.thumbPendingUrl !== url ||
          state.thumbPendingKey !== key
        ) {
          return;
        }
        renderThumbBlob(url, key, blob, requestId);
      })
      .catch(() => {
        if (requestId !== state.thumbRequest) {
          return;
        }
        markThumbFailed(url, key);
      });
  }

  function markThumbFailed(url, key) {
    clearThumbRetry();
    state.thumbPendingUrl = null;
    state.thumbPendingKey = null;
    state.thumbFailedUrl = url;
    state.thumbFailedKey = key;
    if (!state.thumbUrl) {
      state.thumbUrl = null;
      state.thumbKey = null;
      revokeThumbObjectUrl();
      setThumbEmpty();
    }
  }

  function renderThumbBlob(url, key, blob, requestId) {
    const objectUrl = URL.createObjectURL(blob);
    const nextImage = new Image();
    nextImage.alt = "";
    nextImage.decoding = "async";
    nextImage.referrerPolicy = "no-referrer";
    nextImage.onload = () => {
      if (
        requestId !== state.thumbRequest ||
        state.thumbPendingUrl !== url ||
        state.thumbPendingKey !== key
      ) {
        URL.revokeObjectURL(objectUrl);
        return;
      }

      const oldImages = Array.from(state.thumbSlot.querySelectorAll("img"));
      const oldObjectUrl = state.thumbObjectUrl;
      state.thumbSlot.className = "thumb";
      if (oldImages.length === 0) {
        state.thumbSlot.replaceChildren(nextImage);
      } else {
        state.thumbSlot.append(nextImage);
      }

      requestAnimationFrame(() => nextImage.classList.add("is-visible"));
      window.setTimeout(() => oldImages.forEach((image) => image.remove()), 220);
      state.thumbUrl = url;
      state.thumbKey = key;
      state.thumbObjectUrl = objectUrl;
      state.thumbPendingUrl = null;
      state.thumbPendingKey = null;
      state.thumbFailedUrl = null;
      state.thumbFailedKey = null;
      state.thumbSlot.removeAttribute("aria-busy");
      state.thumbSlot.removeAttribute("aria-label");
      if (oldObjectUrl) {
        window.setTimeout(() => URL.revokeObjectURL(oldObjectUrl), 250);
      }
    };
    nextImage.onerror = () => {
      URL.revokeObjectURL(objectUrl);
      if (requestId !== state.thumbRequest) {
        return;
      }
      markThumbFailed(url, key);
    };
    nextImage.src = objectUrl;
  }

  function renderError(message) {
    renderConnectionBubble(null);
    setText(state.title, message || "Could not load print status");
    setText(state.fileName, "--");
    setOptionalText(state.timeEstimate, "");
    setOptionalText(state.printWeight, "");
    setText(state.progressPercent, "--%");
    setOptionalText(state.layerInfo, "Layer -- / --");
    setOptionalText(state.remainingTime, "--");
    state.progress.style.width = "0%";
    setText(state.toolheadTemp, "--");
    setText(state.bedTemp, "--");
    setText(state.fanSpeed, "--");
    setText(state.printSpeed, "--");
    renderMaterials(state.spoolList, [], "No material data");
    renderThumb(null);
  }

  function renderConnectionBubble(device) {
    if (!state.connectionBubble) {
      return;
    }
    const status = device?.serviceStatus || (device?.serviceConnected === false ? "disconnected" : "connected");
    const unavailable = status !== "connected";
    state.connectionBubble.hidden = !unavailable;
    state.connectionBubble.textContent = status === "connecting" ? "Printer connecting" : "Printer disconnected";
    if (unavailable && device.serviceError) {
      state.connectionBubble.title = device.serviceError;
    } else {
      state.connectionBubble.removeAttribute("title");
    }
  }

  function render(data) {
    if (!data.ok) {
      renderError(data.error);
      return;
    }

    const device = pickDevice(data.devices || []);
    if (!device) {
      renderError("No printers returned");
      return;
    }

    const progress = Number.isFinite(device.progress) ? Math.max(0, Math.min(100, device.progress)) : null;
    const title = device.isPrinting ? fallback(device.title, "Unknown print") : fallback(device.title, "No active print");

    renderConnectionBubble(device);
    setText(state.title, title);
    setText(state.fileName, fallback(device.filename));
    setOptionalText(state.timeEstimate, device.totalPrintTime);
    setOptionalText(state.printWeight, device.weight);
    setText(state.progressPercent, progressText(progress));
    setOptionalText(state.layerInfo, layerText(device));
    setOptionalText(state.remainingTime, device.timeRemaining || "--");
    setText(state.toolheadTemp, fallback(device.toolheadTemp));
    setText(state.bedTemp, fallback(device.bedTemp));
    setText(state.fanSpeed, fallback(device.fanSpeed));
    setText(state.printSpeed, fallback(device.printSpeed));
    renderMaterials(state.spoolList, device.materials || [], "No material data");

    state.progress.style.width = progress == null ? "0%" : `${progress}%`;
    renderThumb(device.thumbnail, device.thumbnailTask || device.thumbnail);
  }

  function handlePrintEvent(event) {
    try {
      render(JSON.parse(event.data));
    } catch (error) {
      renderError(error.message);
    }
  }

  function connectEvents() {
    if (!window.EventSource) {
      renderError("Server-sent events are not supported");
      return;
    }

    state.events = new EventSource(eventsUrl);
    state.events.addEventListener("current-print", handlePrintEvent);
    state.events.onerror = () => {
      if (state.events?.readyState === EventSource.CLOSED) {
        renderError("Print status stream closed");
      }
    };
  }

  if (window.__bambuOverlayEnableTestHooks) {
    window.__bambuOverlayTest = {
      state,
      render,
      renderConnectionBubble,
      renderThumb,
    };
  }

  connectEvents();
})();
