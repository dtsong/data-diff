# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-03-01

Community revival release. This marks the beginning of active community maintenance after Datafold sunset the project in May 2024.

### Changed

- **Community fork**: Adopted and revived the project under community maintenance.
- **Build system**: Migrated from Poetry to uv for dependency management and packaging.
- **Test framework**: Migrated from unittest to pytest.
- **CI/CD**: Modernized GitHub Actions workflows with updated dependencies and security hardening.
- **Linting**: Adopted ruff for linting and formatting, replacing black/flake8.

### Removed

- Removed Datafold Cloud integration and proprietary tracking code.
- Removed telemetry and analytics collection.

### Security

- Pinned all CI/CD action versions to commit SHAs.
- Audited and updated dependencies for known vulnerabilities.
