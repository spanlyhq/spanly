# Contributing

Thanks for your interest in improving Spanly's open-source SDKs and CLI!

## How this repository works

This repository is a **read-only mirror** of a directory subset of Spanly's
private monorepo, which is the single source of truth. A one-way sync
([Copybara](https://github.com/google/copybara)) continuously projects the
internal `js/`, `python/`, and `cli/` sources here, with internal-only files
and references scrubbed out.

Practically, this means:

- **Direct pushes to `main` are not accepted** — `main` is overwritten by the
  sync on every internal change.
- **Pull requests are welcome.** A maintainer reviews your PR here, then
  re-applies the change inside the internal monorepo with attribution to you.
  The next sync publishes it back to this repo.
- Because of the above, your PR branch will typically be **closed rather than
  merged** once the change lands internally — the change still ships, it just
  arrives via the next mirror push. This is expected and not a rejection.
- For anything non-trivial, **open an issue first** so we can confirm the
  approach before you invest time.

## Local development

Each artifact builds independently with standard tooling — no monorepo or Nx
required.

### `js/` — TypeScript SDK (`@spanly/sdk`)

```bash
cd js
npm install
npm run build      # tsc --build tsconfig.lib.json
npm test           # jest
npm run lint       # eslint
```

### `python/` — Python SDK (`spanly`)

```bash
cd python
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest tests/ -v
```

### `cli/` — Spanly CLI (Go)

```bash
cd cli
go build ./...
go vet ./...
go test ./...
```

## Pull request checklist

- [ ] The relevant `build`/`test`/`lint` commands above pass for the artifact
      you changed.
- [ ] Changes are scoped to a single artifact (`js/`, `python/`, or `cli/`)
      unless they are inherently cross-cutting.
- [ ] No internal infrastructure details, hostnames, or credentials are
      introduced (the sync's scrub guard will reject these).
- [ ] Public behavior changes are described in the PR so they can be reflected
      in release notes.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
