# Haute Codebase Review — Summary

**Date:** 2026-03-14
**Method:** 20 parallel Opus agents, each analyzing a distinct subsystem
**Scope:** Full codebase — backend (Python), frontend (TypeScript/React), tests, deploy

## Files

| # | File | Category | Issue Count |
|---|------|----------|-------------|
| 01 | [01_security.md](01_security.md) | Security | 3 |
| 02 | [02_bugs_and_correctness.md](02_bugs_and_correctness.md) | Bugs & Correctness | 24 |
| 03 | [03_dry_violations_backend.md](03_dry_violations_backend.md) | DRY (Backend) | 19 |
| 04 | [04_dry_violations_frontend.md](04_dry_violations_frontend.md) | DRY (Frontend) | 15 |
| 05 | [05_architecture_and_complexity.md](05_architecture_and_complexity.md) | Architecture | 16 |
| 06 | [06_test_infrastructure.md](06_test_infrastructure.md) | Tests | 14 |
| 07 | [07_performance.md](07_performance.md) | Performance | 7 |
| 08 | [08_error_handling.md](08_error_handling.md) | Error Handling | 12 |
| | | **Total** | **110** |

## Severity Breakdown

| Severity | Count | Key Examples |
|----------|-------|-------------|
| **HIGH** | 8 | Path traversal (S1-S3), deploy bundler skips registered models (B1), codegen injection (B2-B3), git routes missing exception handling (E1) |
| **MEDIUM-HIGH** | 6 | DATA_SOURCE missing JSON templates (B4), deploy scorer duplicates executor (D1), executor builder extraction (A1) |
| **MEDIUM** | 45 | Parser logic bugs (B5-B6), selected_columns gap (B7), event loop blocking (B16), DRY violations across backend and frontend |
| **LOW-MEDIUM** | 30 | Cache/performance issues, test DRY, minor frontend bugs |
| **LOW** | 21 | Cosmetic, documentation, minor edge cases |

## Recommended Priority Order

### Immediate (security + correctness)
1. **S1-S3**: Fix 3 path traversal vulnerabilities (30 min)
2. **B1**: Fix deploy bundler for registered models (1 hr)
3. **B2-B3**: Codegen triple-quote and curly-brace injection (30 min)
4. **E1**: Git routes missing exception handling (15 min)
5. **B16**: Change git route handlers from `async def` to `def` (10 min)

### High-value refactors (DRY + robustness)
6. **B4**: Add JSON/JSONL templates to DATA_SOURCE codegen (30 min)
7. **B7 + B8**: Fix TypedDict and config key alignment (1 hr)
8. **D2**: Add `func_name`/`config` properties to `NodeBuildContext` (30 min)
9. **D5**: Add `require_completed_job()` to JobStore (15 min)
10. **B10**: Fix double opacity on trace-dimmed nodes (10 min)

### Structural improvements
11. **A1**: Extract executor node builders to `_builders.py` (2 hrs)
12. **D1**: Refactor deploy scorer to delegate to executor builders (3 hrs)
13. **T1-T4**: Consolidate test fixtures to conftest (1 hr)
14. **T5**: Resolve frontend dual test directory structure (2 hrs)
15. **A4-A5**: Split `OptimiserConfig.tsx` and `_shared.tsx` (3 hrs)

### Polish
16. **F1-F3**: Hover handlers, PanelHeader adoption, ModalShell (2 hrs)
17. **P1-P3**: Git subprocess optimization (2 hrs)
18. **A12**: Add Pydantic field constraints (1 hr)
19. **D10**: Codegen passthrough builder factory (30 min)
20. **E3**: Stop leaking raw exception strings (1 hr)
