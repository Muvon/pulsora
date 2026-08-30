# pulsora development tasks
.PHONY: help coverage

help: ## List available tasks
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

# Coverage report (requires cargo-llvm-cov + llvm-tools-preview).
# Tests run without default features (ONNX Runtime issues); test-only files
# are excluded so percentages describe product code.
coverage: ## Generate test coverage report (cargo-llvm-cov)
	@echo "Generating coverage report..."
	cargo llvm-cov --summary-only --no-default-features --ignore-filename-regex '_test\.rs$$'
