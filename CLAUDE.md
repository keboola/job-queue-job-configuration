# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands must be run via Docker from the **monorepo root** (`../../` from this directory). PHP is not available locally.

The `dev-job-configuration` service has `entrypoint: bash`, so pass commands via `-c`:

```bash
# From monorepo root:
docker compose run --rm dev-job-configuration -c "composer tests"     # Full PHPUnit test suite
docker compose run --rm dev-job-configuration -c "composer phpstan"   # Static analysis (max level)
docker compose run --rm dev-job-configuration -c "composer phpcs"     # Code style checks
docker compose run --rm dev-job-configuration -c "composer phpcbf"    # Auto-fix code style
docker compose run --rm dev-job-configuration -c "composer check"     # phpcs + phpstan + tests
```

Run a specific test suite (defined in `phpunit.xml.dist`):
```bash
docker compose run --rm dev-job-configuration -c "vendor/bin/phpunit --testsuite job-definition"
```

Run a single test file or method:
```bash
docker compose run --rm dev-job-configuration -c "vendor/bin/phpunit tests/JobDefinition/Component/ComponentSpecificationTest.php"
docker compose run --rm dev-job-configuration -c "vendor/bin/phpunit --filter testConfigurationDefaults"
```

Do not use the `ci-job-configuration` service — that is for CI pipelines only.

## Architecture

This library parses, validates, and exposes job configuration for Keboola jobs. It bridges the Storage API response format and the internal job runner domain.

### Two main subsystems

**1. `JobDefinition/` — Configuration models**

The entry point is typically `ComponentSpecification` (wraps the Storage API `components[]` response) and `Configuration` (wraps the job's own configuration data).

- `Component/ComponentSpecification` — Validates and exposes component metadata (image URI, memory, timeout, features, logging, etc.). Uses `ComponentSpecificationDefinition` (Symfony `ConfigurationInterface`) internally.
- `Component/ImageDefinition` — Symfony config node defining `definition` sub-tree (`type`, `uri`, `tag`, `digest`, `name`, `repository`).
- `Configuration/Configuration` — Readonly class holding parsed storage, processors, runtime, and artifact config.
- `Configuration/ConfigurationDefinition` — Symfony processor schema for job configuration arrays.
- `Configuration/Storage`, `Runtime`, `Artifacts`, `Processors` — Sub-sections of job configuration.
- `State/State` — Job state file management.

All configuration classes use Symfony's `Config` component (`TreeBuilder` / `ConfigurationInterface`) for validation and normalization. Invalid configs throw `ComponentInvalidException` or `InvalidConfigurationException`.

**2. `Mapping/` — Data loading and staging**

Handles input/output data movement between Storage and the job's working directory.

- `InputDataLoader/InputDataLoader` — Downloads input tables/files from Storage API with staging support.
- `OutputDataLoader/OutputDataLoader` — Uploads output metadata back to Storage.
- `StagingWorkspace/StagingWorkspaceFacade` — Lifecycle wrapper around a Keboola workspace (Snowflake, BigQuery, etc.).
- `StagingWorkspace/StagingWorkspaceFactory` — Instantiates the correct workspace type.

Staging variants (S3, ABS, BigQuery, GCS, Snowflake) each have their own `InputDataLoader` subclass. Tests for these backends require real credentials and are separated into named PHPUnit test suites (`mapping-s3`, `mapping-abs`, etc.).

### Supporting pieces

- `JobStorageApiClient/` — Factory + options for constructing Storage API clients with job-specific auth.
- `Exception/` — `ComponentInvalidException` (extends `ApplicationExceptionInterface`), `InvalidDataException`, `UserException`.
- `JobDefinition/UnitConverter` — Memory string ↔ bytes conversion (e.g. `256m` → bytes).

### Test suites

| Suite | Scope |
|---|---|
| `job-definition` | Config models only (no network) |
| `mapping-general` | Core mapping (no cloud-specific backends) |
| `mapping-s3` / `mapping-abs` / `mapping-bigquery` / `mapping-gcs` | Backend-specific (require credentials) |
