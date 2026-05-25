import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";

const overlayScript = fs.readFileSync(new URL("../assets/static/overlay.js", import.meta.url), "utf8");

class FakeClassList {
  constructor(element) {
    this.element = element;
  }

  add(...names) {
    const classes = new Set(this.element.className.split(/\s+/).filter(Boolean));
    for (const name of names) {
      classes.add(name);
    }
    this.element.className = [...classes].join(" ");
  }

  remove(...names) {
    const remove = new Set(names);
    this.element.className = this.element.className
      .split(/\s+/)
      .filter((name) => name && !remove.has(name))
      .join(" ");
  }

  toggle(name, force) {
    const enabled = force == null ? !this.element.className.split(/\s+/).includes(name) : Boolean(force);
    if (enabled) {
      this.add(name);
    } else {
      this.remove(name);
    }
    return enabled;
  }
}

class FakeElement {
  constructor(tagName = "div") {
    this.tagName = tagName.toUpperCase();
    this.children = [];
    this.className = "";
    this.hidden = false;
    this.textContent = "";
    this.innerHTML = "";
    this.style = { setProperty: (name, value) => (this.style[name] = value) };
    this.attributes = new Map();
    this.classList = new FakeClassList(this);
  }

  append(...children) {
    this.children.push(...children);
  }

  replaceChildren(...children) {
    this.children = children;
    if (children.length > 0) {
      this.textContent = "";
    }
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
    this[name] = String(value);
  }

  removeAttribute(name) {
    this.attributes.delete(name);
    delete this[name];
  }

  querySelectorAll(selector) {
    if (selector !== "img") {
      return [];
    }
    return this.children.filter((child) => child?.tagName === "IMG");
  }

  remove() {
    this.removed = true;
  }
}

function loadOverlay({
  fetch = () => Promise.reject(new Error("unexpected fetch")),
  config = {},
} = {}) {
  const elements = new Map();
  const document = {
    createElement: (tagName) => new FakeElement(tagName),
    getElementById: (id) => {
      if (id === "overlay-config") {
        return { textContent: JSON.stringify(config) };
      }
      if (!elements.has(id)) {
        elements.set(id, new FakeElement());
      }
      return elements.get(id);
    },
  };

  class FakeEventSource {
    static CLOSED = 2;

    constructor(url) {
      this.url = url;
      this.readyState = 0;
      this.listeners = new Map();
    }

    addEventListener(name, listener) {
      this.listeners.set(name, listener);
    }
  }

  const revokedUrls = [];
  const context = {
    EventSource: FakeEventSource,
    Image: class FakeImage extends FakeElement {
      constructor() {
        super("img");
      }
    },
    URL: {
      createObjectURL: () => "blob:test",
      revokeObjectURL: (url) => revokedUrls.push(url),
    },
    document,
    fetch,
    requestAnimationFrame: (callback) => callback(),
    setTimeout,
    window: {
      __machin3dOverlayEnableTestHooks: true,
      clearTimeout,
      EventSource: FakeEventSource,
      location: { search: "" },
      setTimeout,
    },
  };
  context.window.URL = context.URL;
  context.window.document = document;

  vm.runInNewContext(overlayScript, context);
  return { ...context.window.__machin3dOverlayTest, revokedUrls };
}

test("renderThumb clears an existing thumbnail when the next status is missing", () => {
  const { state, renderThumb, revokedUrls } = loadOverlay();
  state.thumbUrl = "/devices/printer-a/thumbnail";
  state.thumbPendingUrl = null;
  state.thumbObjectUrl = "blob:old";
  state.thumbSlot.replaceChildren(new FakeElement("img"));

  renderThumb(null);

  assert.equal(state.thumbUrl, null);
  assert.equal(state.thumbPendingUrl, null);
  assert.equal(state.thumbObjectUrl, null);
  assert.equal(state.thumbSlot.textContent, "3D");
  assert.match(state.thumbSlot.className, /(^|\s)is-empty(\s|$)/);
  assert.deepEqual(revokedUrls, ["blob:old"]);
});

test("render uses the configured device id from the route", () => {
  const { state, render } = loadOverlay({
    config: { selectedDeviceId: "printer-b" },
  });

  render({
    ok: true,
    devices: [
      { id: "printer-a", isPrinting: true, title: "Printer A" },
      { id: "printer-b", isPrinting: true, title: "Printer B" },
    ],
  });

  assert.equal(state.title.textContent, "Printer B");
});

test("renderThumb refetches when the thumbnail task changes without a URL change", () => {
  const fetches = [];
  const { state, renderThumb } = loadOverlay({
    fetch: (url, options) => {
      fetches.push({ url, options });
      return new Promise(() => {});
    },
  });
  state.thumbUrl = "/devices/printer-a/thumbnail";
  state.thumbKey = "old-task";

  renderThumb("/devices/printer-a/thumbnail", "new-task");
  renderThumb("/devices/printer-a/thumbnail", "new-task");

  assert.equal(fetches.length, 1);
  assert.equal(fetches[0].url, "/devices/printer-a/thumbnail");
  assert.equal(fetches[0].options.cache, "no-store");
  assert.equal(state.thumbPendingKey, "new-task");
});

test("renderThumb stops re-fetching when the server reports the thumbnail as unavailable", async () => {
  const fetches = [];
  const { state, renderThumb } = loadOverlay({
    fetch: (url, options) => {
      fetches.push({ url, options });
      return Promise.resolve({
        ok: false,
        status: 404,
        headers: { get: () => null },
        blob: () => Promise.reject(new Error("not called")),
      });
    },
  });

  renderThumb("/devices/printer-a/thumbnail", "task-1");
  await new Promise((resolve) => setTimeout(resolve, 0));
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(fetches.length, 1);
  assert.equal(state.thumbFailedUrl, "/devices/printer-a/thumbnail");
  assert.equal(state.thumbFailedKey, "task-1");

  renderThumb("/devices/printer-a/thumbnail", "task-1");
  renderThumb("/devices/printer-a/thumbnail", "task-1");

  assert.equal(fetches.length, 1, "repeated calls with the same key should not re-fetch");

  renderThumb("/devices/printer-a/thumbnail", "task-2");
  assert.equal(fetches.length, 2, "a new key should retry");
});

test("renderConnectionBubble exposes printer connection freshness", () => {
  const { state, renderConnectionBubble } = loadOverlay();
  const bubble = state.connectionBubble;

  renderConnectionBubble({ serviceStatus: "connecting" });
  assert.equal(bubble.hidden, false);
  assert.equal(bubble.textContent, "Printer connecting");
  assert.equal(bubble.title, undefined);

  renderConnectionBubble({ serviceStatus: "disconnected", serviceError: "No route to host" });
  assert.equal(bubble.hidden, false);
  assert.equal(bubble.textContent, "Printer disconnected");
  assert.equal(bubble.title, "No route to host");

  renderConnectionBubble({ serviceStatus: "connected" });
  assert.equal(bubble.hidden, true);
  assert.equal(bubble.title, undefined);
});
