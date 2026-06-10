# Spanly

Open-source SDKs and CLI for [Spanly](https://spanly.com), observability for
MCP (Model Context Protocol) servers and AI agents.

This repository contains three independently versioned and published artifacts:

| Path        | Package                                  | Install                                      |
| ----------- | ---------------------------------------- | -------------------------------------------- |
| [`js/`](js)         | [`@spanly/sdk`](https://www.npmjs.com/package/@spanly/sdk) (npm)        | `npm install @spanly/sdk`                    |
| [`python/`](python) | [`spanly`](https://pypi.org/project/spanly/) (PyPI)                    | `pip install spanly`                         |
| [`cli/`](cli)       | [`@spanly/spanly`](https://www.npmjs.com/package/@spanly/spanly) / Homebrew | `npx -y @spanly/spanly run -- <mcp-command>` |

## Quick start

### TypeScript / JavaScript SDK

```bash
npm install @spanly/sdk
```

### Python SDK

```bash
pip install spanly
```

### CLI

```bash
# via npm (recommended for MCP client configs)
npx -y @spanly/spanly run -- <your-mcp-command>

# or via Homebrew
brew install spanlyhq/tap/spanly
```

See each subdirectory's README for usage details, or the full
documentation at [spanly.com/docs](https://spanly.com/docs/).

## Contributing

Issues and pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
first. This repository is a mirror of an internal monorepo; the contribution
flow is slightly different from a typical repo, and CONTRIBUTING explains it.

## License

[Apache License 2.0](LICENSE).
