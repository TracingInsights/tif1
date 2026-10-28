#!/usr/bin/env node
/**
 * tif1 inside Pyodide (Python on WebAssembly), driven from Node.
 *
 * Installs the built wheel with micropip, imports tif1 and runs
 * tools/pyodide_checks.py: a synthetic session through the async pipeline
 * offline and, with TIF1_SMOKE_NETWORK=1, a real past-season race through the
 * CDNs. Started with `node --experimental-wasm-jspi`, it exercises the
 * synchronous API as well (JavaScript Promise Integration); from a synchronous
 * entry point the synchronous API must raise AsyncRequiredError either way.
 *
 * Usage:
 *   npm install --no-save pyodide@$(python -c "import json; print(json.load(open('tools/pyodide_target.json'))['pyodide'])")
 *   uv build --wheel
 *   node --experimental-wasm-jspi tools/pyodide_smoke.mjs dist/tif1-<version>-py3-none-any.whl
 */
import { readFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { loadPyodide } from "pyodide";

const here = new URL(".", import.meta.url);
const target = JSON.parse(readFileSync(new URL("pyodide_target.json", here), "utf8"));
const checks = readFileSync(new URL("pyodide_checks.py", here), "utf8");
const wheelPath = process.argv[2];
if (!wheelPath) {
  console.error("usage: node tools/pyodide_smoke.mjs <tif1 wheel>");
  process.exit(2);
}
const wheelName = basename(wheelPath);
const network = ["1", "true", "yes"].includes((process.env.TIF1_SMOKE_NETWORK ?? "").toLowerCase());

try {
  const pyodide = await loadPyodide();
  if (pyodide.version !== target.pyodide) {
    console.warn(`warning: pyodide ${pyodide.version} loaded; tools/pyodide_target.json expects ${target.pyodide}`);
  }
  pyodide.FS.writeFile(`/tmp/${wheelName}`, readFileSync(resolve(wheelPath)));
  await pyodide.loadPackage("micropip");
  const micropip = pyodide.pyimport("micropip");
  await micropip.install(`emfs:/tmp/${wheelName}`);
  pyodide.runPython(checks);

  const report = JSON.parse(
    await pyodide.runPythonAsync(
      "merge(runtime_checks(), await offline_load_async(), offline_sync_weather(), sync_api_check(), journal_mode_check(), ergast_check())",
    ),
  );
  // A synchronous entry point never has JSPI: the synchronous API must raise.
  const syncEntry = JSON.parse(pyodide.runPython("merge(sync_api_check(), offline_sync_weather())"));
  if (!String(syncEntry.sync_api).startsWith("AsyncRequiredError")) {
    throw new Error(`sync entry point did not raise AsyncRequiredError: ${syncEntry.sync_api}`);
  }
  report.sync_entry = syncEntry.sync_api;
  if (network) {
    Object.assign(report, JSON.parse(await pyodide.runPythonAsync('merge(await network_load_async("R"))')));
    if (report.jspi) {
      // Node has no XMLHttpRequest: the synchronous fetch path needs JSPI here.
      Object.assign(
        report,
        JSON.parse(await pyodide.runPythonAsync('merge(network_sync_weather("Q"), network_sync_load("Q"))')),
      );
    }
  }
  console.log(JSON.stringify(report, null, 2));
  console.log("pyodide smoke test passed");
} catch (error) {
  console.error(error);
  process.exit(1);
}
