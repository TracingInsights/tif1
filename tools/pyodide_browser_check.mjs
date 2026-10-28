#!/usr/bin/env node
/**
 * tif1 inside Pyodide in real browsers, driven by Playwright.
 *
 * Serves the built wheel and a test page from localhost, loads Pyodide from the
 * jsDelivr CDN, installs the wheel with micropip and runs
 * tools/pyodide_checks.py on the main thread and in a Web Worker:
 *   - the async pipeline offline and, with TIF1_SMOKE_NETWORK=1, against the CDNs;
 *   - the synchronous API through JSPI (Chrome) or AsyncRequiredError without it;
 *   - urllib3's synchronous XMLHttpRequest path from a synchronous entry point;
 *   - IndexedDB persistence of the SQLite cache across a page reload
 *     (the reloaded page must load the session without one data request).
 *
 * Browsers (TIF1_BROWSERS, comma separated; default "chrome"):
 *   chrome        the installed Google Chrome (Playwright "chrome" channel)
 *   chromium      Playwright's bundled Chromium (`npx playwright install chromium`)
 *   firefox       Playwright's bundled Firefox (`npx playwright install firefox`), no JSPI
 *   firefox-jspi  the same with the JSPI preference enabled
 *
 * Usage:
 *   npm install --no-save playwright && npx playwright install firefox
 *   TIF1_BROWSERS=chrome,firefox TIF1_SMOKE_NETWORK=1 node tools/pyodide_browser_check.mjs dist/tif1-<version>-py3-none-any.whl
 */
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { basename, resolve } from "node:path";
import { chromium, firefox } from "playwright";

const here = new URL(".", import.meta.url);
const target = JSON.parse(readFileSync(new URL("pyodide_target.json", here), "utf8"));
const checks = readFileSync(new URL("pyodide_checks.py", here), "utf8");
const wheelPath = process.argv[2];
if (!wheelPath) {
  console.error("usage: node tools/pyodide_browser_check.mjs <tif1 wheel>");
  process.exit(2);
}
const wheelName = basename(wheelPath);
const wheelBytes = readFileSync(resolve(wheelPath));
const network = ["1", "true", "yes"].includes((process.env.TIF1_SMOKE_NETWORK ?? "").toLowerCase());
const browsers = (process.env.TIF1_BROWSERS ?? "chrome")
  .split(",")
  .map((name) => name.trim())
  .filter(Boolean);
const indexURL = `https://cdn.jsdelivr.net/pyodide/v${target.pyodide}/full/`;

// Runs in the page: install the wheel, then call the checks in the order the
// stage needs. "first" is a fresh origin; "second" runs after a reload and
// must be served from the IndexedDB-backed cache.
const PAGE_SCRIPT = String.raw`
async function installTif1(pyodide, opts) {
  const bytes = new Uint8Array(await (await fetch("/wheel/" + opts.wheelName)).arrayBuffer());
  pyodide.FS.writeFile("/tmp/" + opts.wheelName, bytes);
  await pyodide.loadPackage("micropip");
  const micropip = pyodide.pyimport("micropip");
  await micropip.install("emfs:/tmp/" + opts.wheelName);
  pyodide.runPython(opts.checks);
}

window.tif1Check = async function (opts) {
  const pyodide = await loadPyodide({ indexURL: opts.indexURL });
  await installTif1(pyodide, opts);
  const report = { pyodide: pyodide.version };
  const asyncRun = async (expr) => Object.assign(report, JSON.parse(await pyodide.runPythonAsync(expr)));
  const syncRun = (expr) => Object.assign(report, JSON.parse(pyodide.runPython(expr)));
  if (opts.stage === "first") {
    await asyncRun(
      "merge(runtime_checks(), await offline_load_async(), offline_sync_weather(), sync_api_check(), journal_mode_check(), ergast_check())",
    );
    report.sync_entry = JSON.parse(pyodide.runPython("merge(sync_api_check())")).sync_api;
    if (opts.network) {
      await asyncRun("merge(await persistence_enable())");
      await asyncRun('merge(await network_load_async("R"))');
      // A synchronous entry point: urllib3 takes the XMLHttpRequest path.
      syncRun('merge(network_sync_weather("Q"))');
      if (report.jspi) {
        await asyncRun('merge(network_sync_load("Q"))');
      }
      await asyncRun("merge(await persistence_persist())");
    }
  } else {
    await asyncRun("merge(runtime_checks(), await persistence_enable(), persistence_rows())");
    await asyncRun('merge(await network_load_async("R"))');
  }
  return report;
};

window.tif1WorkerCheck = function (opts) {
  return new Promise((resolvePromise, rejectPromise) => {
    // Pyodide 314 ships pyodide.js as an ES module: the worker must be a module worker.
    const worker = new Worker("/worker.js", { type: "module" });
    worker.onmessage = (event) => {
      worker.terminate();
      if (event.data.ok) resolvePromise(event.data.report);
      else rejectPromise(new Error(event.data.error));
    };
    worker.onerror = (event) => {
      worker.terminate();
      rejectPromise(new Error(event.message || "worker error"));
    };
    worker.postMessage(opts);
  });
};
`;

const WORKER_SCRIPT = String.raw`
self.onmessage = async (event) => {
  const opts = event.data;
  try {
    const { loadPyodide } = await import(opts.indexURL + "pyodide.mjs");
    const pyodide = await loadPyodide({ indexURL: opts.indexURL });
    const bytes = new Uint8Array(await (await fetch("/wheel/" + opts.wheelName)).arrayBuffer());
    pyodide.FS.writeFile("/tmp/" + opts.wheelName, bytes);
    await pyodide.loadPackage("micropip");
    const micropip = pyodide.pyimport("micropip");
    await micropip.install("emfs:/tmp/" + opts.wheelName);
    pyodide.runPython(opts.checks);
    const report = JSON.parse(
      await pyodide.runPythonAsync(
        "merge(runtime_checks(), await offline_load_async(), offline_sync_weather(), sync_api_check(), journal_mode_check(), ergast_check())",
      ),
    );
    report.sync_entry = JSON.parse(pyodide.runPython("merge(sync_api_check())")).sync_api;
    if (opts.network) {
      Object.assign(report, JSON.parse(await pyodide.runPythonAsync('merge(await network_load_async("R"))')));
      Object.assign(report, JSON.parse(pyodide.runPython('merge(network_sync_weather("Q"))')));
    }
    self.postMessage({ ok: true, report });
  } catch (error) {
    self.postMessage({ ok: false, error: String((error && error.stack) || error) });
  }
};
`;

const PAGE = `<!doctype html><html><head><meta charset="utf-8"><title>tif1 Pyodide check</title></head>
<body><script src="${indexURL}pyodide.js"></script><script>${PAGE_SCRIPT}</script></body></html>`;

const server = createServer((request, response) => {
  const url = new URL(request.url, "http://localhost");
  response.setHeader("Access-Control-Allow-Origin", "*");
  if (url.pathname === "/") {
    response.setHeader("Content-Type", "text/html; charset=utf-8");
    response.end(PAGE);
  } else if (url.pathname === "/worker.js") {
    response.setHeader("Content-Type", "text/javascript; charset=utf-8");
    response.end(WORKER_SCRIPT);
  } else if (url.pathname === `/wheel/${wheelName}`) {
    response.setHeader("Content-Type", "application/octet-stream");
    response.end(wheelBytes);
  } else {
    response.statusCode = 404;
    response.end();
  }
});
await new Promise((done) => server.listen(0, "127.0.0.1", done));
const origin = `http://127.0.0.1:${server.address().port}`;

function launchOptions(name) {
  switch (name) {
    case "chrome":
      return [chromium, { channel: "chrome" }];
    case "chromium":
      return [chromium, {}];
    case "firefox":
      return [firefox, {}];
    case "firefox-jspi":
      return [firefox, { firefoxUserPrefs: { "javascript.options.wasm_js_promise_integration": true } }];
    default:
      throw new Error(`unknown browser "${name}" (chrome, chromium, firefox, firefox-jspi)`);
  }
}

function expectSyncEntryRaises(report, label) {
  if (!String(report.sync_entry).startsWith("AsyncRequiredError")) {
    throw new Error(`${label}: synchronous entry point did not raise AsyncRequiredError: ${report.sync_entry}`);
  }
}

const results = {};
let failed = false;
for (const name of browsers) {
  const [launcher, options] = launchOptions(name);
  const browser = await launcher.launch({ headless: true, ...options });
  const context = await browser.newContext();
  const page = await context.newPage();
  page.setDefaultTimeout(120_000);
  let dataRequests = 0;
  page.on("request", (request) => {
    if (/tracinginsights/i.test(request.url())) dataRequests += 1;
  });
  const consoleErrors = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(String(error)));
  const common = { network, wheelName, checks, indexURL };
  const result = {};
  try {
    await page.goto(`${origin}/`);
    result.first = await page.evaluate((opts) => window.tif1Check(opts), { ...common, stage: "first" });
    expectSyncEntryRaises(result.first, `${name} main thread`);
    result.firstDataRequests = dataRequests;
    if (network) {
      dataRequests = 0;
      await page.reload();
      result.second = await page.evaluate((opts) => window.tif1Check(opts), { ...common, stage: "second" });
      result.secondDataRequests = dataRequests;
      if (dataRequests !== 0) {
        throw new Error(`${name}: the reloaded page fetched ${dataRequests} data payloads; IndexedDB persistence failed`);
      }
    }
    result.worker = await page.evaluate((opts) => window.tif1WorkerCheck(opts), common);
    expectSyncEntryRaises(result.worker, `${name} worker`);
    result.consoleErrors = consoleErrors;
    results[name] = result;
    console.log(`${name}: ok`);
  } catch (error) {
    failed = true;
    results[name] = { ...result, error: String((error && error.stack) || error), consoleErrors };
    console.error(`${name}: FAILED`);
    console.error(error);
  } finally {
    await browser.close();
  }
}
server.close();
console.log(JSON.stringify(results, null, 2));
if (failed) {
  process.exit(1);
}
console.log("pyodide browser check passed");
