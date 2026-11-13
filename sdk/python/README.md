# circle-codex

Unofficial fork of the OpenAI Codex Python SDK. It bundles a Codex CLI build
that adds a built-in xAI (Grok) model provider.

This project is not affiliated with, endorsed by, or supported by OpenAI or
xAI. It is built from <https://github.com/ashish447344/codex>, a fork of
<https://github.com/openai/codex>.

Build Python applications that start Codex threads, run turns, stream progress,
and control workspace access.

## Install

Install the SDK:

```bash
pip install circle-codex
```

Prebuilt runtime wheels (`circle-codex-cli-bin`) are published for macOS
(Apple silicon and Intel) and Linux (x86_64 and aarch64, glibc and musl).
Windows is not supported.

The import package keeps the upstream name `openai_codex`, so do not install
this package into the same environment as the official `openai-codex`.

## Quickstart with xAI

Set `XAI_API_KEY` in your environment, then select the xAI provider:

```python
from openai_codex import Codex, CodexConfig, Sandbox

config = CodexConfig(
    config_overrides=('model_provider="xai"', 'model="grok-4.7"'),
)

with Codex(config) as codex:
    thread = codex.thread_start(sandbox=Sandbox.workspace_write)
    result = thread.run("Explain this repository in three bullets.")
    print(result.final_response)
```

## Quickstart

The SDK reuses your existing Codex authentication when one is already
available:

```python
from openai_codex import Codex

with Codex() as codex:
    thread = codex.thread_start()
    result = thread.run("Explain this repository in three bullets.")
    print(result.final_response)
```

`thread.run(...)` returns a `TurnResult` containing the final response,
collected items, and token usage.

## Authentication

Existing Codex authentication is reused automatically. To start ChatGPT
browser login explicitly:

```python
from openai_codex import Codex

with Codex() as codex:
    login = codex.login_chatgpt()
    print(login.auth_url)
    print(login.wait().success)
```

For device-code login:

```python
with Codex() as codex:
    login = codex.login_chatgpt_device_code()
    print(login.verification_url, login.user_code)
    login.wait()
```

For API-key login:

```python
with Codex() as codex:
    codex.login_api_key("sk-...")
```

## Built-In Help

Use Python's standard `help(openai_codex)`, `help(Codex)`, or
`python -m pydoc openai_codex` documentation tools.

## Documentation

The SDK API is the same as upstream, so the upstream guides apply:

- [Getting started](https://github.com/openai/codex/blob/main/sdk/python/docs/getting-started.md)
- [API reference](https://github.com/openai/codex/blob/main/sdk/python/docs/api-reference.md)
- [Untrusted external messages](https://github.com/openai/codex/blob/main/sdk/python/docs/api-reference.md#externalmessage)
- [FAQ](https://github.com/openai/codex/blob/main/sdk/python/docs/faq.md)
- [Examples](https://github.com/openai/codex/blob/main/sdk/python/examples/README.md)

Where those guides say `pip install openai-codex`, use `pip install circle-codex`.

The package is licensed under the
[Apache License 2.0](https://github.com/ashish447344/codex/blob/main/LICENSE),
the same license as upstream. See the repository
[NOTICE](https://github.com/ashish447344/codex/blob/main/NOTICE) for attribution.
