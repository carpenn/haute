import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import {
  ApiError,
  loadPipeline,
  previewNode,
  savePipeline,
  traceCell,
  executeSink,
  fetchSchema,
  fetchDatabricksSchema,
  trainModel,
  solveOptimiser,
  listFiles,
  createSubmodel,
  checkMlflow,
  getTrainStatus,
  estimateTrainingRam,
  getWarehouses,
  getCatalogs,
} from "../client"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let mockFetch: ReturnType<typeof vi.fn>

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: () => Promise.resolve(body),
  })
}

function errorResponse(status: number, body?: unknown) {
  return Promise.resolve({
    ok: false,
    status,
    statusText: "Error",
    json: body !== undefined
      ? () => Promise.resolve(body)
      : () => Promise.reject(new Error("no body")),
  })
}

const dummyGraph = {
  nodes: [{ id: "n1", type: "custom", position: { x: 0, y: 0 }, data: {} }],
  edges: [],
}

// ---------------------------------------------------------------------------
// Setup / Teardown
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockFetch = vi.fn()
  global.fetch = mockFetch as unknown as typeof fetch
})

afterEach(() => {
  vi.restoreAllMocks()
})

// ═══════════════════════════════════════════════════════════════════════════
// request() core function — tested through loadPipeline (a thin GET wrapper)
// ═══════════════════════════════════════════════════════════════════════════

describe("request() core via loadPipeline", () => {
  it("makes a GET request to the correct URL", async () => {
    mockFetch.mockReturnValue(jsonResponse({ nodes: [], edges: [] }))
    await loadPipeline()
    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/pipeline")
  })

  it("returns parsed JSON on success", async () => {
    const data = { nodes: [{ id: "1" }], edges: [] }
    mockFetch.mockReturnValue(jsonResponse(data))
    const result = await loadPipeline()
    expect(result).toEqual(data)
  })

  it("throws ApiError with status and detail on 4xx response", async () => {
    mockFetch.mockReturnValue(errorResponse(422, { detail: "Validation failed" }))
    // loadPipeline catches 404 specifically, so use 422 to test the generic path
    await expect(loadPipeline()).rejects.toThrow(ApiError)
    try {
      await loadPipeline()
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError)
      expect((err as ApiError).status).toBe(422)
      expect((err as ApiError).detail).toBe("Validation failed")
    }
  })

  it("throws ApiError with status and detail on 5xx response", async () => {
    mockFetch.mockReturnValue(errorResponse(500, { detail: "Internal server error" }))
    await expect(loadPipeline()).rejects.toThrow(ApiError)
  })

  it("uses statusText as detail when response body is not JSON", async () => {
    mockFetch.mockReturnValue(errorResponse(503))
    try {
      // Use checkMlflow as it doesn't catch errors like loadPipeline
      await checkMlflow()
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError)
      expect((err as ApiError).detail).toBe("Error")
    }
  })

  it("handles network error (fetch throws)", async () => {
    mockFetch.mockRejectedValue(new TypeError("Failed to fetch"))
    await expect(checkMlflow()).rejects.toThrow("Failed to fetch")
  })

  it("passes AbortController signal to fetch", async () => {
    mockFetch.mockReturnValue(jsonResponse({ nodes: [], edges: [] }))
    await loadPipeline()
    const [, options] = mockFetch.mock.calls[0]
    expect(options.signal).toBeInstanceOf(AbortSignal)
  })

  it("loadPipeline returns empty graph on 404", async () => {
    mockFetch.mockReturnValue(errorResponse(404, { detail: "Not found" }))
    const result = await loadPipeline()
    expect(result).toEqual({ nodes: [], edges: [] })
  })
})

// ═══════════════════════════════════════════════════════════════════════════
// POST requests — tested through specific endpoints
// ═══════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════
// Per-endpoint contract tests
// ═══════════════════════════════════════════════════════════════════════════

describe("endpoint contracts", () => {
  beforeEach(() => {
    mockFetch.mockReturnValue(jsonResponse({}))
  })

  it("previewNode posts to /api/pipeline/preview with correct body", async () => {
    await previewNode(dummyGraph, "node1", 50, "live")
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/pipeline/preview")
    expect(opts.method).toBe("POST")
    const body = JSON.parse(opts.body)
    expect(body.graph).toEqual(dummyGraph)
    expect(body.node_id).toBe("node1")
    expect(body.row_limit).toBe(50)
    expect(body.scenario).toBe("live")
  })

  it("savePipeline posts to /api/pipeline/save", async () => {
    const payload = {
      name: "test",
      description: "desc",
      graph: dummyGraph,
      preamble: "",
      source_file: "pipe.py",
    }
    await savePipeline(payload)
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/pipeline/save")
    expect(opts.method).toBe("POST")
    const body = JSON.parse(opts.body)
    expect(body.name).toBe("test")
    expect(body.source_file).toBe("pipe.py")
  })

  it("traceCell posts to /api/pipeline/trace with correct body", async () => {
    await traceCell({ graph: dummyGraph, row_index: 0, target_node_id: "n1" })
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/pipeline/trace")
    expect(opts.method).toBe("POST")
    const body = JSON.parse(opts.body)
    expect(body.row_index).toBe(0)
    expect(body.target_node_id).toBe("n1")
  })

  it("executeSink posts to /api/pipeline/sink", async () => {
    await executeSink(dummyGraph, "sink1")
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/pipeline/sink")
    const body = JSON.parse(opts.body)
    expect(body.node_id).toBe("sink1")
    expect(body.scenario).toBe("live")
  })

  it("fetchSchema GETs /api/schema with encoded path", async () => {
    await fetchSchema("data/test file.csv")
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/schema?path=data%2Ftest%20file.csv")
  })

  it("fetchDatabricksSchema GETs /api/schema/databricks with encoded table", async () => {
    await fetchDatabricksSchema("catalog.schema.table")
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/schema/databricks?table=catalog.schema.table")
  })

  it("trainModel posts to /api/modelling/train with default scenario", async () => {
    await trainModel({ graph: dummyGraph, node_id: "model1" })
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/modelling/train")
    const body = JSON.parse(opts.body)
    expect(body.node_id).toBe("model1")
    expect(body.scenario).toBe("live")
  })

  it("solveOptimiser posts to /api/optimiser/solve", async () => {
    await solveOptimiser({ graph: dummyGraph, node_id: "opt1" })
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/optimiser/solve")
    const body = JSON.parse(opts.body)
    expect(body.node_id).toBe("opt1")
  })

  it("listFiles GETs /api/files with dir and optional extensions", async () => {
    await listFiles("data", ".csv,.parquet")
    const [url] = mockFetch.mock.calls[0]
    expect(url).toContain("/api/files?")
    expect(url).toContain("dir=data")
    expect(url).toContain("extensions=.csv%2C.parquet")
  })

  it("createSubmodel posts to /api/submodel/create", async () => {
    const payload = {
      name: "sub1",
      node_ids: ["n1", "n2"],
      graph: dummyGraph,
      preamble: "",
      source_file: "pipe.py",
      pipeline_name: "main",
    }
    await createSubmodel(payload)
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/submodel/create")
    const body = JSON.parse(opts.body)
    expect(body.name).toBe("sub1")
    expect(body.node_ids).toEqual(["n1", "n2"])
  })

  it("getTrainStatus GETs /api/modelling/train/status/{jobId}", async () => {
    await getTrainStatus("job-123")
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/modelling/train/status/job-123")
  })

  it("estimateTrainingRam posts to /api/modelling/estimate", async () => {
    await estimateTrainingRam({ graph: dummyGraph, node_id: "m1" })
    const [url, opts] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/modelling/estimate")
    expect(opts.method).toBe("POST")
  })

  it("getWarehouses GETs /api/databricks/warehouses", async () => {
    await getWarehouses()
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/databricks/warehouses")
  })

  it("getCatalogs GETs /api/databricks/catalogs", async () => {
    await getCatalogs()
    const [url] = mockFetch.mock.calls[0]
    expect(url).toBe("/api/databricks/catalogs")
  })
})
