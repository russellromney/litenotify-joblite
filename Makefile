.PHONY: help test test-rust test-python test-python-slow test-node test-all \
        build build-pyo3 build-ext \
        coverage coverage-rust coverage-python coverage-python-all \
        install-coverage-deps clean

help:
	@echo "litenotify / joblite development targets"
	@echo ""
	@echo "Tests:"
	@echo "  make test           - default test run: rust + python + node (fast)"
	@echo "  make test-rust      - cargo test on litenotify-core"
	@echo "  make test-python    - pytest tests/ (excludes slow/linux_only)"
	@echo "  make test-python-slow - pytest -m slow (soak, real-time cron)"
	@echo "  make test-node      - npm test in packages/litenotify-node"
	@echo "  make test-all       - everything, including slow marks"
	@echo ""
	@echo "Builds:"
	@echo "  make build          - build both PyO3 + loadable extension"
	@echo "  make build-pyo3     - maturin develop --release"
	@echo "  make build-ext      - cargo build -p litenotify-extension --release"
	@echo ""
	@echo "Coverage (run 'make install-coverage-deps' once):"
	@echo "  make coverage       - both rust + python HTML reports into coverage/"
	@echo "  make coverage-rust  - cargo llvm-cov on litenotify-core"
	@echo "  make coverage-python - coverage.py over pytest run"

# ---- tests ----

test: test-rust test-python test-node
	@echo "all fast tests passed"

test-rust:
	cargo test -p litenotify-core --release

test-python:
	.venv/bin/python -m pytest tests/

test-python-slow:
	.venv/bin/python -m pytest tests/ -m slow

test-node:
	cd packages/litenotify-node && npm test

test-all: test test-python-slow
	@echo "all tests passed (including slow marks)"

# ---- builds ----

build: build-pyo3 build-ext

build-pyo3:
	cd packages/litenotify && VIRTUAL_ENV=$(CURDIR)/.venv \
		$(CURDIR)/.venv/bin/python -m maturin develop --release

build-ext:
	cargo build --release -p litenotify-extension

# ---- coverage ----

install-coverage-deps:
	.venv/bin/python -m pip install coverage pytest pytest-asyncio pytest-xdist
	cargo install cargo-llvm-cov

coverage: coverage-rust coverage-python
	@echo ""
	@echo "coverage reports:"
	@echo "  rust:   coverage/rust/html/index.html"
	@echo "  python: coverage/python/index.html"

coverage-rust:
	cargo llvm-cov --release -p litenotify-core --html --output-dir coverage/rust
	cargo llvm-cov --release -p litenotify-core --summary-only

coverage-python:
	.venv/bin/python -m coverage erase
	# Override default addopts (`-n auto`) with an empty string so
	# xdist doesn't fork — coverage sees every statement in the
	# main process. Slower than the parallel run but accurate. We
	# still respect the slow / linux_only marks — `coverage-python-all`
	# adds those back.
	PYTHONPATH=$(CURDIR)/packages .venv/bin/python -m coverage run \
		--source=$(CURDIR)/packages/joblite \
		-m pytest tests/ \
		-o addopts='-m "not slow and not linux_only"'
	.venv/bin/python -m coverage html -d coverage/python
	.venv/bin/python -m coverage report

coverage-python-all:
	.venv/bin/python -m coverage erase
	PYTHONPATH=$(CURDIR)/packages .venv/bin/python -m coverage run \
		--source=$(CURDIR)/packages/joblite \
		-m pytest tests/ -o addopts=""
	.venv/bin/python -m coverage html -d coverage/python
	.venv/bin/python -m coverage report

clean:
	rm -rf coverage/
	rm -rf target/
	rm -rf **/__pycache__
	rm -rf .pytest_cache/
	rm -f .coverage .coverage.*
