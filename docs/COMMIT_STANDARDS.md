# Commit Standards

Every commit merged into `main` must satisfy the checks below. Review this list during code review and before approving a PR.

---

## Design Philosophy

These are the non-negotiable principles that shape every decision in runway. If a commit conflicts with any of these, it needs rethinking.

### Code is the source of truth

The `.py` file is always canonical. The GUI is a live, editable *view* of that code — never the other way around. Layout metadata lives in a sidecar `.runw.json`, not in the Python. A pipeline must always be runnable with `python my_pipeline.py` — no GUI required.

### Same pipeline, every context

The same pipeline code runs for a 1-row live API quote and a 100M-row batch job. No separate "live mode" vs "batch mode" implementations. If a node works in preview, it works in production.

### Single execution engine

CLI, GUI, and programmatic usage all flow through the same parser → executor path. There is one topo sort, one graph executor, one code generator. Parallel implementations drift and break.

### Polars-native, lazy by default

All intermediate results stay as `LazyFrame` until the moment data is needed. This lets Polars push predicates and limits into scans. Never call `.collect()` earlier than necessary. Never convert to pandas.

### The GUI never crashes

Bad code in a `.py` file shows an error banner, not a white screen. Parse errors, execution errors, and malformed data are all caught and surfaced gracefully. The last known good state is always preserved.

### Permissive parsing, strict generation

The parser accepts messy, hand-edited Python — regex fallback, partial decorators, missing type hints. The code generator always emits clean, idiomatic, `ruff`-passing Python. Round-tripping through the GUI improves code quality.

### Thin orchestration, not a platform

Runway is a client library that leans on Databricks, MLflow, and Polars. It does not reimplement model training, data storage, or serving infrastructure. New integrations should wrap existing tools, not replace them.

### Low floor, high ceiling

A pricing analyst with no engineering background can drag nodes and connect them in the GUI. A senior engineer can write raw Polars expressions, custom decorators, and deploy via CI/CD. Both use the same tool. Features should serve the analyst by default and get out of the engineer's way.

### Everything is diffable

Pipeline logic is `.py` files — reviewable in PRs, diffable in git, testable in CI. Node positions are JSON. There are no opaque binary formats. If it can't be diffed, it shouldn't be committed.

### Explain every price

Every output value must be traceable back through the graph to its inputs, showing the intermediate value at each node. This is a regulatory requirement (Solvency II, IFRS 17) and a core product differentiator. Features that break traceability are not acceptable.

---

## Engineering Standards

## 1. No Duplication (DRY)

- No copy-pasted logic. If two places do the same thing, extract a shared function/module.
- Shared utilities live in well-known locations (`graph_utils.py`, `frontend/src/utils/`).
- Frontend and backend implementations of the same logic (e.g. `sanitizeName`) must reference each other in comments and stay in sync.

## 2. Simplicity (KISS)

- Prefer the simplest solution that works. No premature abstractions.
- If a function takes more than 5 parameters, consider a config object or breaking it up.
- Avoid clever one-liners that sacrifice readability.

## 3. Single Responsibility

- Each module, class, and function does one thing.
- API endpoints are thin — validation and response shaping only. Business logic lives in the library layer (`executor.py`, `pipeline.py`, etc.).
- React components: rendering only. Side effects in hooks, logic in utils.

## 4. Type Safety

- **Python**: All function signatures have type annotations. No `Any` unless truly unavoidable.
- **Python API**: Every endpoint uses Pydantic request/response models. No `body: dict`.
- **TypeScript**: No `any`. Use proper interfaces/types for all props, state, and API responses.

## 5. Linter Clean

- `ruff check src/runw/` must pass with zero errors before merge.
- Frontend must have no TypeScript errors (`tsc --noEmit`).
- New ruff rules are not silenced without a comment explaining why.

## 6. No Dead Code

- No unused imports, variables, or functions.
- No commented-out code blocks. Use version control to retrieve old code.
- No empty files. If a file has no content, delete it.

## 7. No Stale Documentation

- If you change behaviour, update the relevant doc in `docs/`.
- Status tables in design docs must reflect reality (no "❌ Missing" for things that exist).
- README examples must actually run.

## 8. Dependency Discipline

- No heavy optional dependencies in core `[project.dependencies]`. Use `[project.optional-dependencies]`.
- Pin minimum versions, not exact versions.
- Every new dependency must be justified in the PR description.

## 9. No Resource Leaks

- File handles: use `with` statements or `subprocess.DEVNULL`. Never `open()` without close.
- Async tasks: always cancellable. Use lifespan context managers, not `on_event`.
- WebSocket connections: always cleaned up in `finally` blocks.

## 10. Single Execution Path

- CLI and GUI must use the same execution engine (`parse → execute_graph`).
- No parallel implementations of the same algorithm (e.g. two topo sorts).
- If a code path is only reachable from one caller, consider inlining it.

## 11. Correct Data Structures

- Use `deque` for FIFO, not `list.pop(0)`.
- Use `set` for membership checks, not list scans.
- Use lazy evaluation (Polars `LazyFrame`) until the moment you need concrete data.

## 12. Error Handling

- Never swallow exceptions silently (`except: pass`).
- API errors return structured JSON with status codes, not bare strings.
- The GUI never crashes due to bad data. Worst case: show last good state + error banner.

## 13. Consistent Naming

- Python: `snake_case` for functions/variables, `PascalCase` for classes.
- TypeScript: `camelCase` for functions/variables, `PascalCase` for components/types.
- API fields that cross the frontend/backend boundary use `camelCase` (frontend convention) with Pydantic model aliases where needed.

## 14. Idiomatic React

- No module-level mutable state. Use `useRef` for instance-scoped counters.
- Side effects only in `useEffect` or event handlers.
- Memoize expensive computations with `useMemo`/`useCallback`.
- Prefer controlled components.

## 15. Security Basics

- No hardcoded secrets or API keys. Use environment variables.
- File access endpoints must validate paths stay within the project root.
- No `eval()` on untrusted input. User code execution is sandboxed to pipeline context.
- Sensitive internal docs (competitive analysis, credentials) must be in `.gitignore`.

## 16. Test Coverage

- New business logic must include at least one test.
- Bug fixes must include a regression test.
- Tests must not depend on external services or network access.
- Never delete or weaken an existing test without explicit justification in the PR.

## 17. Commit Hygiene

- Each commit is atomic: one logical change per commit.
- Commit messages follow conventional format: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`.
- No generated files (`node_modules/`, `__pycache__/`, build output) in commits.

---

## LLM-Generated Code: Watch For These

AI coding assistants produce plausible-looking code that often hides real problems. Every reviewer (human or AI) must check for these patterns specifically.

### Dangerous fallbacks that mask errors

```python
# BAD — silently returns empty data instead of crashing
def get_data(path):
    try:
        return pl.read_parquet(path)
    except Exception:
        return pl.DataFrame()  # caller has no idea it failed
```

- Never return a default value from a `catch` unless the caller explicitly expects it.
- Prefer letting exceptions propagate. If you must catch, log the error and re-raise or return a result type that signals failure.
- Watch for `or {}`, `or []`, `or ""` fallbacks that turn a bug into silent wrong data.

### Broad exception swallowing

- `except Exception: pass` is almost never correct. Catch the specific exception you expect.
- `except Exception as e: return {"error": str(e)}` in API endpoints is fine — but only at the outermost layer. Inner code should not catch broadly.

### Hallucinated APIs and parameters

- LLMs invent function signatures, config keys, and library methods that don't exist. Every API call, import, and parameter must be verified against the actual codebase or library docs.
- Watch for plausible-but-wrong Polars methods (e.g. `df.groupby()` instead of `df.group_by()`).

### Stale patterns from older library versions

- LLMs train on old code. Watch for deprecated patterns:
  - `@app.on_event("startup")` → use `lifespan` context manager
  - `from typing import List, Dict` → use `list`, `dict` (Python 3.11+)
  - `pd.DataFrame` when we use `pl.DataFrame`

### Defensive code that hides bugs

```python
# BAD — if node_map is missing a key, this silently skips it
result = node_map.get(nid, {}).get("data", {}).get("config", {})
```

- Chained `.get()` with default dicts makes `KeyError` impossible to diagnose. If the key should exist, access it directly and let it fail loudly.
- Only use `.get()` with defaults when the key is genuinely optional.

### Duplicated logic disguised as "safety"

- LLMs often generate a "just in case" check that duplicates logic already handled upstream. This creates two code paths that must be kept in sync.
- If a function already validates its input, don't re-validate it in the caller.

### Over-abstraction and premature generalisation

- LLMs love creating `BaseNode`, `NodeFactory`, `AbstractPipelineExecutor` hierarchies for code that has exactly one concrete implementation.
- If there's only one subclass, you don't need the base class.
- Prefer plain functions over class hierarchies until you have three concrete use cases.

### Comments that restate the code

```python
# BAD — the comment adds zero information
x = x + 1  # increment x by 1
```

- LLMs pad output with obvious comments. Comments should explain *why*, not *what*.
- Delete any comment that a competent reader could infer from the code itself.

### Untested edge cases presented as handled

- LLMs generate `if` branches for edge cases but don't test them. An untested branch is worse than no branch — it gives false confidence.
- If you add an edge case handler, add a test for it. If you can't test it, add a `# TODO: untested` comment.

### Import bloat

- LLMs import modules speculatively. If a function isn't used, the import shouldn't be there.
- Watch for `from typing import ...` lines that grow with every edit but never shrink.

---

## Backward Compatibility: None Required

This is a brand new application. Do not add compatibility shims, version checks, or migration code.

- **No "legacy" support** — if an API is poorly designed, change it. Do not keep the old version alongside the new one.
- **No feature flags** — if a feature is ready, ship it. Do not add `ENABLE_NEW_X` environment variables.
- **No versioned endpoints** — `/api/v1/` is unnecessary. Use `/api/` and evolve it as needed.
- **No migration scripts** — if the data model changes, update the code. There is no production data to migrate yet.
- **No deprecation warnings** — if something is wrong, remove it. Do not add `warnings.warn` with a future removal date.

The only exception is the public PyPI package interface (`runw` CLI and core APIs). Prioritize clean, simple code over compatibility gymnastics.

---

## Quick Checklist

Copy into PR descriptions:

```
Design Philosophy
- [ ] Code (.py) is the source of truth — GUI is a view, not the canonical form
- [ ] Same code path works for 1-row and N-row execution
- [ ] Uses the single execution engine (parse → execute_graph), no parallel paths
- [ ] Polars LazyFrame throughout — no premature .collect() or pandas conversion
- [ ] GUI handles errors gracefully (error banner, not crash)
- [ ] Generated code is clean, idiomatic, ruff-passing Python
- [ ] No unnecessary abstraction layers or platform reimplementation
- [ ] All outputs are traceable back through the graph

Engineering Standards
- [ ] No duplicated logic
- [ ] All functions have type annotations
- [ ] API endpoints use Pydantic request/response models
- [ ] `ruff check src/runw/` passes with zero errors
- [ ] No unused imports, variables, or dead code
- [ ] Docs updated if behaviour changed
- [ ] No new heavy dependencies in core
- [ ] No resource leaks (file handles, async tasks, sockets)
- [ ] Error cases return structured responses, not bare strings
- [ ] Consistent naming (snake_case Python, camelCase TypeScript, PascalCase classes/components)
- [ ] React state in hooks/refs, not module-level variables
- [ ] File access endpoints validate paths stay within project root
- [ ] Tests added for new logic; bug fixes include regression tests

LLM Code Review
- [ ] No silent fallbacks that mask errors (return empty data, `or {}`, `or []`)
- [ ] No broad exception swallowing (`except Exception: pass`)
- [ ] All API calls and imports verified against actual codebase/library docs
- [ ] No deprecated patterns (old typing imports, on_event, pandas)
- [ ] No chained .get() on keys that should exist — fail loudly on missing data
- [ ] No redundant validation that duplicates upstream checks
- [ ] No premature abstraction (base classes with one subclass)
- [ ] Comments explain why, not what — no restating the code
- [ ] Edge case branches have tests, or are marked # TODO: untested

Backward Compatibility
- [ ] No compatibility shims, version flags, or migration code
- [ ] Bad APIs are replaced, not versioned alongside
```
