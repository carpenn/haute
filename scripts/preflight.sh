#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────
# preflight.sh — Run all CI checks locally before pushing.
#
# Usage:
#   ./scripts/preflight.sh          # full check (lint + types + tests)
#   ./scripts/preflight.sh --quick  # lint + types only (~30s)
#
# Exit code 0 = safe to push.  Non-zero = fix before pushing.
# ──────────────────────────────────────────────────────────────────
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

QUICK=false
if [[ "${1:-}" == "--quick" ]]; then
  QUICK=true
fi

FAIL=0

step() {
  printf "${YELLOW}▶ %s${NC}\n" "$1"
}

pass() {
  printf "${GREEN}✓ %s${NC}\n" "$1"
}

fail() {
  printf "${RED}✗ %s${NC}\n" "$1"
  FAIL=1
}

# ── Backend checks ────────────────────────────────────────────────

step "Ruff lint (Python)"
if uv run ruff check src/; then
  pass "Ruff lint"
else
  fail "Ruff lint — run 'uv run ruff check src/ --fix' to auto-fix"
fi

step "Ruff format check (Python)"
if uv run ruff format --check src/ tests/; then
  pass "Ruff format"
else
  fail "Ruff format — run 'uv run ruff format src/ tests/' to fix"
fi

step "Mypy type check (Python)"
if uv run mypy src/haute/; then
  pass "Mypy"
else
  fail "Mypy type errors"
fi

# ── Frontend checks ───────────────────────────────────────────────

step "TypeScript type check"
if (cd frontend && npx tsc -b --noEmit); then
  pass "TypeScript"
else
  fail "TypeScript errors"
fi

step "ESLint (frontend)"
if (cd frontend && npx eslint .); then
  pass "ESLint"
else
  fail "ESLint errors — run 'cd frontend && npx eslint . --fix' to auto-fix"
fi

# ── Tests (skip with --quick) ────────────────────────────────────

if [[ "$QUICK" == false ]]; then
  step "Python tests with coverage"
  if uv run pytest tests/ -q --cov=src/haute --cov-branch --cov-fail-under=85; then
    pass "Python tests (coverage ≥85%)"
  else
    fail "Python tests"
  fi

  step "Frontend tests"
  if (cd frontend && npx vitest run); then
    pass "Frontend tests"
  else
    fail "Frontend tests"
  fi
else
  printf "${YELLOW}⏭ Skipping tests (--quick mode)${NC}\n"
fi

# ── Summary ──────────────────────────────────────────────────────

echo ""
if [[ "$FAIL" -eq 0 ]]; then
  printf "${GREEN}══════════════════════════════════════${NC}\n"
  printf "${GREEN}  All checks passed. Safe to push.    ${NC}\n"
  printf "${GREEN}══════════════════════════════════════${NC}\n"
else
  printf "${RED}══════════════════════════════════════${NC}\n"
  printf "${RED}  Some checks failed. Fix before push. ${NC}\n"
  printf "${RED}══════════════════════════════════════${NC}\n"
  exit 1
fi
