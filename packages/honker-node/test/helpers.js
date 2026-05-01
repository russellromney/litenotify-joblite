'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const deferredCleanupDirs = new Set();
let deferredCleanupInstalled = false;

function sleepSync(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function cleanupDir(dir) {
  global.gc?.();
  sleepSync(100);
  let lastErr;
  for (let i = 0; i < 60; i++) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
      return;
    } catch (err) {
      if (!['EBUSY', 'EPERM', 'ENOTEMPTY'].includes(err.code)) throw err;
      lastErr = err;
      global.gc?.();
      sleepSync(50 * (i + 1));
    }
  }
  throw lastErr;
}

function trackCloseable(tracked, resource) {
  if (resource && typeof resource.close === 'function') tracked.push(resource);
  return resource;
}

function wrapMethod(target, name, wrapResult) {
  if (!target || typeof target[name] !== 'function') return;
  const real = target[name].bind(target);
  target[name] = (...args) => wrapResult(real(...args));
}

function wrapDatabase(db, tracked) {
  if (db.__honkerTracked) return db;

  Object.defineProperty(db, '__honkerTracked', {
    value: true,
    enumerable: false,
    configurable: false,
    writable: false,
  });

  trackCloseable(tracked, db);

  wrapMethod(db, 'updateEvents', (resource) => trackCloseable(tracked, resource));
  wrapMethod(db, 'listen', (resource) => trackCloseable(tracked, resource));
  wrapMethod(db, 'stream', (stream) => {
    wrapMethod(stream, 'subscribe', (resource) => trackCloseable(tracked, resource));
    return stream;
  });
  wrapMethod(db, 'queue', (queue) => {
    wrapMethod(queue, 'claimWaker', (resource) => trackCloseable(tracked, resource));
    return queue;
  });

  return db;
}

function closeTracked(tracked) {
  const seen = new Set();
  while (tracked.length) {
    const resource = tracked.pop();
    if (!resource || seen.has(resource)) continue;
    seen.add(resource);
    try {
      resource.close?.();
    } catch {}
  }
  global.gc?.();
  sleepSync(100);
}

function installDeferredCleanup() {
  if (deferredCleanupInstalled) return;
  deferredCleanupInstalled = true;
  process.on('exit', () => {
    for (const dir of deferredCleanupDirs) {
      try {
        cleanupDir(dir);
      } catch {}
    }
    deferredCleanupDirs.clear();
  });
}

function createTempDb(prefix, openFn) {
  installDeferredCleanup();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  const tracked = [];
  return {
    path: path.join(dir, 't.db'),
    dir,
    open: (...args) => wrapDatabase(openFn(...args), tracked),
    cleanup: () => {
      closeTracked(tracked);
      try {
        cleanupDir(dir);
        deferredCleanupDirs.delete(dir);
        return true;
      } catch {
        deferredCleanupDirs.add(dir);
        return false;
      }
    },
  };
}

module.exports = {
  cleanupDir,
  createTempDb,
  sleepSync,
};
