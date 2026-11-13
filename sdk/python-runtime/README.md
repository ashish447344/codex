# circle-codex-cli-bin

Platform-specific Codex CLI runtime consumed by the `circle-codex` Python SDK.
Install `circle-codex`; this package is pulled in automatically.

This is an unofficial build of the Codex CLI from
<https://github.com/ashish447344/codex>, a fork of
<https://github.com/openai/codex> that adds a built-in xAI model provider. It is
not affiliated with, endorsed by, or supported by OpenAI or xAI.

This package is staged during release so the SDK can pin an exact Codex CLI
version without checking platform binaries into the repo.

`circle-codex-cli-bin` is intentionally wheel-only. Do not build or publish an
sdist for this package.
