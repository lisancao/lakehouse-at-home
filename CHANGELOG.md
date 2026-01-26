# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Features
- Apache Airflow 3.1.6 orchestration with DAGs for medallion pipeline and Iceberg maintenance
- Scripts reorganization into clear directory structure (quickstarts/, connectivity/, pipelines/, tools/)
- Test data generation framework
- Comprehensive CI/CD pipeline with Codecov integration
- Multi-version Spark support (4.0 and 4.1)

### Documentation
- Development workflow guide with change checklists
- Airflow orchestration guide
- Architecture documentation with diagrams
- CLI reference

### Security
- Pre-commit hooks for secrets detection
- Security test suite
- Input validation in CLI

---

*This changelog is automatically updated by [release-please](https://github.com/googleapis/release-please).*
