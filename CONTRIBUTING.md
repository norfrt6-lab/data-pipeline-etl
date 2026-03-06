# Contributing

## Getting Started

```bash
# Clone the repo
git clone https://github.com/norfrt6-lab/data-pipeline-etl.git
cd data-pipeline-etl

# Install dev dependencies
pip install -r requirements-dev.txt

# Start infrastructure
docker compose up -d

# Run tests
make test
```

## Development Workflow

1. Create a feature branch from `dev`:
   ```bash
   git checkout dev && git pull
   git checkout -b feat/your-feature
   ```

2. Make changes and verify locally:
   ```bash
   make lint       # ruff check
   make format     # ruff format
   make typecheck  # mypy
   make test       # pytest (unit + coverage)
   ```

3. Open a PR targeting `dev`. CI must pass before merge.

4. After merging to `dev`, a separate PR promotes `dev` → `main`.

## Branch Strategy

| Branch | Purpose |
|--------|---------|
| `main` | Production-ready code |
| `dev` | Integration branch |
| `feat/*` | New features |
| `fix/*` | Bug fixes |
| `docs/*` | Documentation changes |

## Code Standards

- **Python 3.10+** with `from __future__ import annotations`
- **Type hints** on all function signatures — enforced by mypy
- **ruff** for linting and formatting (line length 100)
- **structlog** for all logging (no `print()` in production code)
- **Docstrings** on public functions and classes

## Testing

- Unit tests go in `tests/test_*.py`
- Integration tests go in `tests/integration/` and must be marked with `@pytest.mark.integration`
- Use `unittest.mock` for external dependencies (Kafka, PostgreSQL, CCXT)
- Coverage threshold: 60% minimum (enforced in CI)

## Commit Messages

Format: `<type>: <short description>`

Types: `feat`, `fix`, `refactor`, `docs`, `style`, `test`, `chore`

Examples:
```
feat: add EMA indicator using applyInPandas
fix: handle null values in RSI calculation
test: add integration tests for Kafka producer
docs: update architecture diagram
```
