# Python SDK releases

## This fork: circle-codex

This fork publishes two PyPI packages from one workflow,
`.github/workflows/circle-python-release.yml`:

- `circle-codex-cli-bin`: platform wheels with the fork's Codex CLI
  (macOS arm64 and x86_64; Linux x86_64 and aarch64, manylinux and musllinux).
- `circle-codex`: the pure-Python SDK, pinned to that exact runtime version.

To release, open the Actions tab, pick `circle-python-release`, choose
"Run workflow", and enter a version such as `0.154.0` (0.151.0 or newer). Leave
"publish" unchecked for a build-only dry run; check it to upload to PyPI. The
version is stamped into the Rust workspace during the build, so the checked-in
`0.0.0` stays in place for local development. PyPI versions cannot be replaced,
so every fix needs a new version.

One-time setup: create a GitHub environment named `pypi`, and on PyPI add a
trusted (pending) publisher for both package names that points at this
repository, workflow `circle-python-release.yml`, environment `pypi`.

The sections below describe the upstream OpenAI release process and its
workflows, which do not run in this fork.

## Upstream

Stable CLI releases trigger `python-sdk-cli-release.yml` after `rust-release`
finishes. Publication requires a successful `release` job and all required runtime
assets on the published stable release; unrelated publisher failures, such as
winget, do not block it. The downstream workflow checks that the release tag still
points to that run's commit, builds the SDK and runtime from that revision, then
publishes and verifies all runtime wheels before publishing and verifying the SDK.
For CLI version X, the SDK version and its exact runtime dependency are both X.
CLI prereleases do not trigger Python publication. Failures in this downstream
workflow do not block CLI completion or `latest-alpha-cli`.

The downstream workflow must exist on the default branch before GitHub can trigger
it. The CLI release revision must contain the Python build scripts and generated
contracts. It never substitutes newer SDK sources from the default branch.

To retry independently, rerun the failed downstream jobs, or dispatch
`python-sdk-cli-release.yml` with `cli_run_id` set to the Rust release's GitHub
Actions run ID. The resolver checks the effective release job across run attempts
and verifies the same tag, commit, and required assets again. Existing
PyPI uploads are accepted, and verification still requires the complete release.
Use a new version if already-published package contents need to change.

Before enabling publication, configure both `openai-codex` and
`openai-codex-cli-bin` on PyPI to trust owner `openai`, repository `codex`, workflow
`python-sdk-cli-release.yml`, environment `pypi`. The GitHub environment must permit
this workflow on the default branch, which is the ref used by `workflow_run` and
manual dispatch even though the build checks out the release commit. These PyPI
and GitHub environment settings are managed outside the repository.

Independent SDK releases remain available through `python-v*` tags and
`python-sdk-release.yml`. Stable SDK versions must match the checked-in runtime
pin; beta SDK versions such as `python-v0.1.0b1` can use an independently versioned
runtime. Retain the existing trusted-publisher entries for this manual workflow
and `python-runtime-release.yml`.

See GitHub's [workflow_run documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run)
and PyPI's [trusted-publisher setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).
