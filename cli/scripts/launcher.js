#!/usr/bin/env node
const { spawnSync } = require('node:child_process');

const BIN_NAME = 'spanly';
const os = process.platform;
const arch = process.arch;
const ext = os === 'win32' ? '.exe' : '';
const pkg = `@spanly/spanly-${os}-${arch}`;

let binaryPath;
try {
  binaryPath = require.resolve(`${pkg}/bin/${BIN_NAME}${ext}`);
} catch (err) {
  if (process.env.SPANLY_DEBUG) console.error('[spanly] resolve error:', err);
  console.error(
    `[spanly] no prebuilt binary for ${os}-${arch}.\n` +
      `Supported: darwin-arm64, darwin-x64, linux-arm64, linux-x64, win32-x64.\n` +
      `File an issue at https://github.com/spanlyhq/spanly/issues`,
  );
  process.exit(1);
}

const result = spawnSync(binaryPath, process.argv.slice(2), {
  stdio: 'inherit',
});
if (result.error) {
  console.error(`[spanly] failed to spawn binary: ${result.error.message}`);
  process.exit(1);
}
process.exit(result.status ?? 1);
