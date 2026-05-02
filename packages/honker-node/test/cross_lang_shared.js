'use strict';

const { spawn } = require('node:child_process');
const path = require('node:path');

const REPO = path.resolve(__dirname, '..', '..', '..');
const PACKAGES = path.resolve(__dirname, '..', '..');
const PYTHON = path.join(
  REPO,
  '.venv',
  process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python',
);

function waitForExit(proc) {
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) {
    return Promise.resolve(proc?.exitCode);
  }
  return new Promise((resolve) => proc.once('exit', resolve));
}

async function stopChild(proc) {
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
  proc.kill();
  await waitForExit(proc);
}

function spawnPython(script, stdio = ['ignore', 'pipe', 'inherit']) {
  return spawn(PYTHON, ['-c', script], { stdio });
}

function createLineReader(stream) {
  const lines = [];
  const waiters = [];
  let buf = '';

  stream.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    let nl;
    while ((nl = buf.indexOf('\n')) >= 0) {
      let line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      // Python's print() on Windows runs through text-mode stdout
      // and writes "\r\n" terminators. Strip the trailing CR before
      // delivering the line so consumers can use strict equality
      // (e.g. `line === 'READY'`) and stay portable.
      if (line.endsWith('\r')) line = line.slice(0, -1);
      lines.push(line);
      const waiter = waiters.shift();
      if (waiter) waiter(line);
    }
  });

  return function nextLineMatching(predicate, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('timeout waiting for line')),
        timeoutMs,
      );
      const check = () => {
        for (let i = 0; i < lines.length; i++) {
          if (predicate(lines[i])) {
            const line = lines.splice(i, 1)[0];
            clearTimeout(timer);
            return resolve(line);
          }
        }
        waiters.push((line) => {
          if (predicate(line)) {
            clearTimeout(timer);
            resolve(line);
          } else {
            check();
          }
        });
      };
      check();
    });
  };
}

module.exports = {
  PACKAGES,
  PYTHON,
  createLineReader,
  spawnPython,
  stopChild,
  waitForExit,
};
