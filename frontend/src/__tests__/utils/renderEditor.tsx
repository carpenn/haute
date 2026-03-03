/**
 * Shared test utility for rendering editor components.
 *
 * Wraps editors in ReactFlowProvider (required by many editors)
 * and provides default props with mocked callbacks.
 */
import { render, type RenderResult } from "@testing-library/react"
import { ReactFlowProvider } from "@xyflow/react"
import { vi } from "vitest"
import type { InputSource, SimpleNode, OnUpdateConfig } from "../../panels/editors/_shared"

export type EditorTestProps = {
  config?: Record<string, unknown>
  onUpdate?: OnUpdateConfig
  inputSources?: InputSource[]
  onDeleteInput?: (edgeId: string) => void
  allNodes?: SimpleNode[]
  upstreamColumns?: { name: string; dtype: string }[]
}

const DEFAULT_INPUT_SOURCES: InputSource[] = []
const DEFAULT_ALL_NODES: SimpleNode[] = []

/**
 * Render an editor component wrapped in ReactFlowProvider with sensible defaults.
 *
 * Returns the RTL render result plus the `onUpdate` mock for assertions.
 */
export function renderEditor(
  EditorComponent: React.ComponentType<Record<string, unknown>>,
  overrides: EditorTestProps = {},
): RenderResult & { onUpdate: ReturnType<typeof vi.fn> } {
  const onUpdate = overrides.onUpdate ?? vi.fn()
  const config = overrides.config ?? {}
  const inputSources = overrides.inputSources ?? DEFAULT_INPUT_SOURCES
  const onDeleteInput = overrides.onDeleteInput ?? vi.fn()
  const allNodes = overrides.allNodes ?? DEFAULT_ALL_NODES

  const props: Record<string, unknown> = {
    config,
    onUpdate,
    inputSources,
    onDeleteInput,
    allNodes,
  }

  if (overrides.upstreamColumns) {
    props.upstreamColumns = overrides.upstreamColumns
  }

  const result = render(
    <ReactFlowProvider>
      <EditorComponent {...props} />
    </ReactFlowProvider>,
  )

  return { ...result, onUpdate: onUpdate as ReturnType<typeof vi.fn> }
}
