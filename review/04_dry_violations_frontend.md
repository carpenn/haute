# DRY Violations — Frontend

Systemic repetition across the TypeScript/React codebase.

---

## F1: Inline hover handlers — 62 occurrences across 24 files

**Severity:** MEDIUM (previously identified as D2/D7, skipped)
**Agent:** Frontend Toolbar (13)

```tsx
onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--chrome-hover)'; e.currentTarget.style.color = 'var(--text-primary)' }}
onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)' }}
```

**Options:**
- A: CSS-only — define `.hover-chrome` class using `:hover` pseudoselector. Eliminates all JS handlers.
- B: Small helper — `hoverHandlers(bg, color?)` returns `{ onMouseEnter, onMouseLeave }` objects. Reduces 4 lines to 1 spread.
- C: Do nothing — previously assessed as "50+ files, high risk, low value."

**Recommendation:** Option B as an immediate low-risk win. Migrate to Option A in new code.

---

## F2: `PanelHeader` used by 1 of 4+ panels

**Files:** `ImportsPanel.tsx` (uses it), `UtilityPanel.tsx`, `GitPanel.tsx`, `TracePanel.tsx` (all have custom headers)
**Severity:** MEDIUM
**Agent:** Frontend Toolbar (13)

`PanelHeader` already supports `icon`, `subtitle`, and `actions` props. All three panels' custom headers fit this interface. TracePanel was "skipped due to custom header" but the component's props cover its needs.

**Fix:** Mechanical change, ~10 mins per panel, ~30 lines eliminated.

---

## F3: Dialog/modal shell duplicated 3 times

**Files:** `SubmodelDialog.tsx`, `RenameDialog.tsx`, `KeyboardShortcuts.tsx`
**Severity:** MEDIUM
**Agent:** Frontend Toolbar (13)

All repeat the same 5-6 lines of modal shell (fixed overlay, backdrop click, accessibility attributes, styling). A `ModalShell` component would centralize this and fix the SubmodelDialog missing Escape handler automatically.

---

## F4: `labelMap` duplicates `NODE_TYPE_META`

**File:** `frontend/src/hooks/useEdgeHandlers.ts:20-38`
**Severity:** LOW-MEDIUM
**Agent:** React Flow (10)

18-line manual mapping of every node type to a display name. This is an exact duplicate of `NODE_TYPE_META[type].name`.

**Fix:** Replace `labelMap[type] || "Node"` with `NODE_TYPE_META[type as NodeTypeValue]?.name || "Node"`. Delete `labelMap`. 1-line fix, 18 lines removed.

---

## F5: `nodeIdCounter` initialization duplicated in 2 places

**Files:** `frontend/src/hooks/usePipelineAPI.ts:158-161`, `frontend/src/hooks/useWebSocketSync.ts:77-80`
**Severity:** LOW-MEDIUM
**Agents:** React Flow (10), Frontend State (12)

Identical reduce pattern for computing max node ID suffix.

**Fix:** Extract `computeNextNodeId(nodes: Node[]): number` utility.

---

## F6: `normalizeEdges` duplicated in 3 files

**Files:** `useSubmodelNavigation.ts:33-35`, `usePipelineAPI.ts:143`, `useWebSocketSync.ts:60`
**Severity:** LOW
**Agent:** React Flow (10)

`edges.map((e) => ({ ...e, type: "default", animated: false }))` repeated 3 times.

**Fix:** Extract shared utility function.

---

## F7: CSS variable inconsistency — `--input-bg` vs `--bg-input`

**Files:** `ConstantEditor.tsx`, `OptimiserConfig.tsx` (~20 places), modelling sub-panels
**Severity:** MEDIUM
**Agent:** Node Editors (11)

`_shared.tsx` and most editors use `var(--bg-input)`. But `ConstantEditor.tsx` and `OptimiserConfig.tsx` use `var(--input-bg)`. Could cause visual inconsistencies if one isn't aliased in CSS.

**Fix:** Standardize on `INPUT_STYLE` / `SELECT_STYLE` constants from `_shared.tsx` everywhere.

---

## F8: Form components built but never adopted

**Files:** `components/form/ConfigInput.tsx`, `ConfigSelect.tsx`, `ConfigCheckbox.tsx`
**Severity:** LOW-MEDIUM
**Agent:** Node Editors (11)

Well-designed, accessible form components with proper labels and `aria-label`. But every editor hand-codes its own `<input>`, `<select>`, `<label>` with inline styles. The label pattern `text-[11px] font-bold uppercase tracking-[0.08em]` appears in virtually every editor.

**Fix:** Either adopt them throughout, or extract a shared `EditorLabel` component.

---

## F9: `PipelineNodeData`, `SubmodelNodeData`, `SubmodelPortData` overlap with `HauteNodeData`

**Files:** `PipelineNode.tsx:15`, `SubmodelNode.tsx:6`, `SubmodelPortNode.tsx:5`, `types/node.ts`
**Severity:** LOW-MEDIUM
**Agents:** React Flow (10), Schemas (20)

Four separate types define node data shapes with significant overlap. `HauteNodeData` is the canonical type but node components define their own.

**Fix:** Have all node components use `HauteNodeData` from `types/node.ts` directly.

---

## F10: Inline types in `client.ts` duplicate backend schemas

**File:** `frontend/src/api/client.ts`
**Severity:** MEDIUM
**Agent:** Schemas (20)

~15 API response shapes defined as anonymous inline types in function return signatures rather than importing from `api/types.ts`. `GitStatus`, `GitBranch`, `GitHistoryEntry`, `TrainEstimate`, `UtilityFile`, etc., are all defined inline.

**Fix:** Move to `api/types.ts` and import.

---

## F11: `{ name: string; dtype: string }` inline in 5+ places instead of `ColumnInfo`

**Files:** `api/types.ts:29,30,92`, `ColumnsTab.tsx:11,13`, `useNodeResultsStore.ts:149,155,156`
**Severity:** LOW
**Agent:** Schemas (20)

`ColumnInfo` exists in `types/node.ts`. These should import and use it.

---

## F12: Dropdown pattern repeated 4 times

**Files:** Toolbar scenario selector, UtilityPanel file selector, GitPanel branch selector, BreakdownDropdown
**Severity:** LOW
**Agent:** Frontend Toolbar (13)

Each implements its own dropdown from scratch with open/close state, `useClickOutside`, ref, positioning, and ChevronDown rotation.

---

## F13: Column table rendering duplicated in 3 places

**Files:** `OutputEditor.tsx:38-69`, `ColumnsTab.tsx:117-158`, `SchemaPreview` in `_shared.tsx:248-269`
**Severity:** LOW
**Agent:** Node Editors (11)

Nearly identical table with `getDtypeColor`, column name, dtype badge. Extract a shared `ColumnTable` component.

---

## F14: `collectUpstreamColumns` duplicated

**Files:** `NodePanel.tsx:211-219`, `OutputEditor.tsx:21-23`, `useDataInputColumns.ts`
**Severity:** LOW
**Agent:** Node Editors (11)

Each reimplements "find upstream columns" slightly differently.

---

## F15: `buildCartesianEntries` duplicated in test file

**File:** `frontend/src/__tests__/utils/banding.test.ts:39-73`
**Severity:** LOW
**Agent:** Rating (15)

Copied from `ratingTableUtils.ts` into the test file despite being exported. The test tests a local copy, not the actual implementation.

**Fix:** Import from `ratingTableUtils` instead.
