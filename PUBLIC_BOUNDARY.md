# Public Boundary

This repository (`pulseai-labs/PulseDB`) is the **public, open-source** home of the `pulsehive-db` crate. This document states what belongs in public and what must stay internal, so contributors and AI tooling never leak private material into a published artifact.

## What is public (intentionally)

- The PulseDB library source (`src/`), tests, benchmarks, and examples.
- Public API documentation, the README, CHANGELOG, and governance docs (this file, `SECURITY.md`, `LICENSING.md`, `CONTRIBUTING.md`).
- CI/release configuration under `.github/`.

PulseDB is designed as a substrate that other systems build on. The only downstream system named in public docs is **PulseHive** (its documented public consumer via `SubstrateProvider`).

## What must NEVER be committed here

- **Secrets**: API keys, tokens, `CARGO_TOKEN`, `.env` files, private keys (`*.pem`, `*.key`, `id_rsa`), credentials. Secret scanning + push protection are enabled, and `.gitignore` covers common patterns — but the first line of defense is not committing them.
- **Product strategy / roadmaps** for downstream commercial products. PulseDB documents *its own* capabilities only.
- **Customer data**, real datasets, or `*.db` fixtures containing anything non-synthetic.
- **AI-workspace material**: `CLAUDE.md`, `AGENTS.md`, memory-bank, sprint specs, handoffs, and scaffold tooling live in the **private** AI workspace, not here (already `.gitignore`d).

## Internal vs. public repos

| Concern | Lives in |
|---------|----------|
| Library code, public docs, releases | **this repo (public)** |
| Project planning, specs, agent scaffolding, memory bank | private AI workspace |
| Downstream product code & strategy | their own (private) repos |

## If something leaked

Treat any secret that reached git history as **compromised**: rotate it immediately (do not rely on a force-push to erase it). For an accidental disclosure of private material, open a private report per [SECURITY.md](SECURITY.md).
