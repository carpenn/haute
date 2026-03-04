import { describe, it, expect } from "vitest"
import { formatBytes } from "../../utils/formatBytes"

describe("formatBytes", () => {
  it("returns bytes for values under 1 KB", () => {
    expect(formatBytes(0)).toBe("0 B")
    expect(formatBytes(1)).toBe("1 B")
    expect(formatBytes(512)).toBe("512 B")
    expect(formatBytes(1023)).toBe("1023 B")
  })

  it("returns KB for values from 1 KB to just under 1 MB", () => {
    expect(formatBytes(1024)).toBe("1.0 KB")
    expect(formatBytes(1536)).toBe("1.5 KB")
    expect(formatBytes(10240)).toBe("10.0 KB")
    expect(formatBytes(1024 * 1024 - 1)).toBe("1024.0 KB")
  })

  it("returns MB for values 1 MB and above", () => {
    expect(formatBytes(1024 * 1024)).toBe("1.0 MB")
    expect(formatBytes(1.5 * 1024 * 1024)).toBe("1.5 MB")
    expect(formatBytes(100 * 1024 * 1024)).toBe("100.0 MB")
  })

  it("handles exact boundary at 1024", () => {
    expect(formatBytes(1024)).toBe("1.0 KB")
  })

  it("handles exact boundary at 1 MB", () => {
    expect(formatBytes(1024 * 1024)).toBe("1.0 MB")
  })
})
