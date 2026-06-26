#!/usr/bin/env node
// Consumes GoReleaser's dist/artifacts.json and publishes to npm:
//   - N per-platform packages (@spanly/spanly-{os}-{arch}) each shipping a binary
//   - A wrapper @spanly/spanly package whose bin dispatches to the right platform
//
// Re-runs are idempotent: already-published versions are skipped.
// Pass --dry-run to stage the packages under dist-npm/ without publishing.

import { execFileSync } from 'node:child_process';
import {
  copyFileSync,
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CLI_DIR = path.resolve(__dirname, '..');
const DIST_DIR = path.join(CLI_DIR, 'dist');
const OUT_DIR = path.join(CLI_DIR, 'dist-npm');
const WRAPPER_NAME = '@spanly/spanly';
const BIN_NAME = 'spanly';
// Public repo URL: npm provenance requires the repository field to match the
// repo the publish workflow runs in (the public mirror).
const REPOSITORY = 'https://github.com/spanlyhq/spanly';
const HOMEPAGE = 'https://spanly.com';

const VERSION = process.env.VERSION;
if (!VERSION) {
  console.error('VERSION env var is required');
  process.exit(1);
}

const DRY_RUN = process.argv.includes('--dry-run');

const GOOS_TO_NODE = { linux: 'linux', darwin: 'darwin', windows: 'win32' };
const GOARCH_TO_NODE = { amd64: 'x64', arm64: 'arm64' };

function run(cmd, args, opts = {}) {
  console.log(
    `$ ${cmd} ${args.join(' ')}${opts.cwd ? `  (cwd=${opts.cwd})` : ''}`,
  );
  return execFileSync(cmd, args, { stdio: 'inherit', ...opts });
}

function versionExists(pkg) {
  try {
    execFileSync('npm', ['view', `${pkg}@${VERSION}`, 'version'], {
      stdio: 'pipe',
    });
    return true;
  } catch {
    return false;
  }
}

function loadArtifacts() {
  const artifactsPath = path.join(DIST_DIR, 'artifacts.json');
  if (!existsSync(artifactsPath)) {
    console.error(
      `No artifacts.json at ${artifactsPath} — run goreleaser first.`,
    );
    process.exit(1);
  }
  const artifacts = JSON.parse(readFileSync(artifactsPath, 'utf8'));
  return artifacts.filter(
    (a) =>
      a.type === 'Binary' &&
      a.goos in GOOS_TO_NODE &&
      a.goarch in GOARCH_TO_NODE,
  );
}

function buildPlatformPackage(artifact) {
  const nodeOs = GOOS_TO_NODE[artifact.goos];
  const nodeArch = GOARCH_TO_NODE[artifact.goarch];
  const name = `${WRAPPER_NAME}-${nodeOs}-${nodeArch}`;
  const binFile = nodeOs === 'win32' ? `${BIN_NAME}.exe` : BIN_NAME;
  const pkgDir = path.join(OUT_DIR, `${BIN_NAME}-${nodeOs}-${nodeArch}`);
  const binDir = path.join(pkgDir, 'bin');

  rmSync(pkgDir, { recursive: true, force: true });
  mkdirSync(binDir, { recursive: true });

  copyFileSync(artifact.path, path.join(binDir, binFile));
  if (nodeOs !== 'win32') chmodSync(path.join(binDir, binFile), 0o755);

  // No `bin` field here on purpose: the wrapper package is the only one that
  // declares the `spanly` bin. If a platform package also declared it, npm would
  // see two packages claiming the same bin name and link neither — breaking
  // `npx @spanly/spanly` / PATH. The platform package only ships the raw binary;
  // the wrapper's launcher resolves it via require.resolve.
  writeFileSync(
    path.join(pkgDir, 'package.json'),
    JSON.stringify(
      {
        name,
        version: VERSION,
        description: `Spanly CLI binary for ${nodeOs}-${nodeArch}`,
        repository: REPOSITORY,
        homepage: HOMEPAGE,
        license: 'Apache-2.0',
        os: [nodeOs],
        cpu: [nodeArch],
      },
      null,
      2,
    ) + '\n',
  );

  return { name, pkgDir };
}

function buildWrapperPackage(platformPackages) {
  const pkgDir = path.join(OUT_DIR, BIN_NAME);
  const binDir = path.join(pkgDir, 'bin');

  rmSync(pkgDir, { recursive: true, force: true });
  mkdirSync(binDir, { recursive: true });

  copyFileSync(
    path.join(__dirname, 'launcher.js'),
    path.join(binDir, `${BIN_NAME}.js`),
  );
  chmodSync(path.join(binDir, `${BIN_NAME}.js`), 0o755);

  // The CLI README doubles as the npm page for the wrapper package. LICENSE
  // sits at the public repo root (one level above cli/); absent in other layouts.
  copyFileSync(path.join(CLI_DIR, 'README.md'), path.join(pkgDir, 'README.md'));
  const license = path.join(CLI_DIR, '..', 'LICENSE');
  if (existsSync(license)) copyFileSync(license, path.join(pkgDir, 'LICENSE'));

  const optionalDependencies = Object.fromEntries(
    platformPackages.map((p) => [p.name, VERSION]),
  );

  writeFileSync(
    path.join(pkgDir, 'package.json'),
    JSON.stringify(
      {
        name: WRAPPER_NAME,
        version: VERSION,
        description:
          'Spanly CLI: observability for MCP servers (run + proxy modes)',
        keywords: [
          'mcp',
          'model-context-protocol',
          'observability',
          'monitoring',
          'telemetry',
          'cli',
          'spanly',
        ],
        repository: REPOSITORY,
        homepage: HOMEPAGE,
        bugs: `${REPOSITORY}/issues`,
        license: 'Apache-2.0',
        bin: { [BIN_NAME]: `bin/${BIN_NAME}.js` },
        optionalDependencies,
      },
      null,
      2,
    ) + '\n',
  );

  return { name: WRAPPER_NAME, pkgDir };
}

function publishPackage(pkg) {
  if (DRY_RUN) {
    console.log(`[dry-run] would publish ${pkg.name}@${VERSION}`);
    return;
  }
  if (versionExists(pkg.name)) {
    console.log(`skip: ${pkg.name}@${VERSION} already published`);
    return;
  }
  run('npm', ['publish', '--access', 'public'], { cwd: pkg.pkgDir });
}

rmSync(OUT_DIR, { recursive: true, force: true });
mkdirSync(OUT_DIR, { recursive: true });

const artifacts = loadArtifacts();
if (artifacts.length === 0) {
  console.error('No matching Binary artifacts found in dist/artifacts.json');
  process.exit(1);
}

console.log(`Found ${artifacts.length} platform binaries`);
const platformPackages = artifacts.map(buildPlatformPackage);
const wrapper = buildWrapperPackage(platformPackages);

for (const pkg of platformPackages) publishPackage(pkg);
publishPackage(wrapper);

console.log('Done.');
