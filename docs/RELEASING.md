# Release integrity

Samsarix Chat Engine releases are versioned GitHub prereleases while the project is below 1.0. A release is a source tag plus a wheel, source distribution, checksum manifest, and GitHub/Sigstore build-provenance attestations. PyPI and npm publication are separate owner-controlled decisions; the release workflow has no credentials or steps for either registry.

## Prepare a release

1. Start from a clean, current `main` whose full CI run passed.
2. Update every Python version surface together: `pyproject.toml`, `samsarix_chat_engine/__init__.py`, the service/OpenAPI version in `samsarix_chat_engine/app.py`, README status, and any deployment examples.
3. Move every entry under `Unreleased` in `CHANGELOG.md` into a dated `## MAJOR.MINOR.PATCH — YYYY-MM-DD` section. An absent or empty `Unreleased` section is required at the tagged commit.
4. Keep TypeScript client versioning independent. A Python tag does not publish the npm package.
5. Re-run the documented development and package checks, merge through CI, then create and push an annotated tag such as `v0.13.0`. Never move or reuse a published version tag.

The tag workflow rejects non-canonical tags, a version mismatch, a missing dated changelog section, nonempty unreleased notes, or a commit that is not an ancestor of the repository's default branch. It uses read-only repository access while building. Only the final GitHub-release job receives `contents: write`; the attestation job receives only `contents: read`, `id-token: write`, and `attestations: write`.

## What the workflow proves

The workflow installs explicit versions of its Python build tools, builds the wheel and source distribution once without a second isolated dependency resolution, validates both with Twine, installs the wheel into a fresh environment, runs the installed-product smoke journey, creates and verifies `SHA256SUMS`, and asks GitHub's first-party attestation action to bind every artifact digest to the tag workflow. The publish job downloads those exact workflow artifacts, verifies the checksums again, and creates a GitHub prerelease without rebuilding.

This provenance establishes which repository, workflow, commit, and event produced a file. It does not audit the source, guarantee dependency behavior, sign a container image, publish to a package registry, or turn a preview topology into a supported release.

## Consumer verification

Download a release into a new directory, verify the checksum manifest, and verify each artifact's GitHub attestation:

```bash
gh release download v0.13.0 --repo Deathcharge/samsarix-chat-engine --dir samsarix-v0.13.0
cd samsarix-v0.13.0
sha256sum --check SHA256SUMS
gh attestation verify samsarix_chat_engine-0.13.0-py3-none-any.whl --repo Deathcharge/samsarix-chat-engine
gh attestation verify samsarix_chat_engine-0.13.0.tar.gz --repo Deathcharge/samsarix-chat-engine
```

On Windows, compare `Get-FileHash -Algorithm SHA256` output with `SHA256SUMS` before running the same `gh attestation verify` commands. Verification requires the GitHub CLI and network access to GitHub's attestation service.
