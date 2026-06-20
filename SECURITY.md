# Security Policy

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions, or pull requests.**

Report privately via either channel:

1. **GitHub private vulnerability reporting** (preferred) — use the **"Report a vulnerability"** button on the repository's [Security tab](https://github.com/pulseai-labs/PulseDB/security). This is enabled on this repo.
2. **Email** — **praveensingh2897@gmail.com** with subject `SECURITY: PulseDB`.

Please include, where possible:

- A description of the vulnerability and its impact.
- Steps to reproduce (a minimal proof-of-concept, affected version/commit).
- Any suggested remediation.

## What to expect

- **Acknowledgement** within 5 business days.
- An assessment and, if accepted, a remediation plan with a target timeline.
- Coordinated disclosure: we will agree on a disclosure date and credit you (if you wish) once a fix is released.

Please give us a reasonable window to release a fix before any public disclosure.

## Supported versions

PulseDB is pre-1.0; security fixes target the **latest published `0.x` release** on crates.io. Older versions are not patched — please upgrade.

| Version | Supported |
|---------|-----------|
| latest `0.x` | ✅ |
| older | ❌ (upgrade) |

## Scope

In scope: the `pulsehive-db` crate (storage, vector search, sync, embeddings) and its build/release pipeline. Out of scope: downstream products built on PulseDB, and issues requiring a compromised host or physical access.
