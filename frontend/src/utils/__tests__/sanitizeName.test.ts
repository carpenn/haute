import { describe, it, expect } from "vitest"
import { sanitizeName } from "../sanitizeName"

describe("sanitizeName", () => {
  it("converts spaces to underscores", () => {
    expect(sanitizeName("my node")).toBe("my_node")
  })

  it("converts hyphens to underscores", () => {
    expect(sanitizeName("my-node")).toBe("my_node")
  })

  it("converts mixed spaces and hyphens", () => {
    expect(sanitizeName("my node-name here")).toBe("my_node_name_here")
  })

  it("strips non-alphanumeric/underscore characters", () => {
    expect(sanitizeName("hello@world!")).toBe("helloworld")
    expect(sanitizeName("rate(%)")).toBe("rate")
    expect(sanitizeName("col#1&2")).toBe("col12")
  })

  it("trims leading and trailing whitespace", () => {
    expect(sanitizeName("  padded  ")).toBe("padded")
  })

  it("prefixes with node_ if name starts with a digit", () => {
    expect(sanitizeName("123abc")).toBe("node_123abc")
    expect(sanitizeName("0_start")).toBe("node_0_start")
  })

  it("does not prefix if name starts with a letter", () => {
    expect(sanitizeName("abc123")).toBe("abc123")
  })

  it("does not prefix if name starts with underscore", () => {
    expect(sanitizeName("_private")).toBe("_private")
  })

  it("returns unnamed_node for empty string", () => {
    expect(sanitizeName("")).toBe("unnamed_node")
  })

  it("returns unnamed_node for whitespace-only input", () => {
    expect(sanitizeName("   ")).toBe("unnamed_node")
  })

  it("returns unnamed_node when all characters are stripped", () => {
    expect(sanitizeName("@#$%")).toBe("unnamed_node")
  })

  it("preserves casing", () => {
    expect(sanitizeName("MyNode")).toBe("MyNode")
    expect(sanitizeName("UPPER")).toBe("UPPER")
  })

  it("handles underscores in input (preserved)", () => {
    expect(sanitizeName("already_valid")).toBe("already_valid")
  })

  it("handles digit after stripping leading special chars", () => {
    expect(sanitizeName("!1foo")).toBe("node_1foo")
  })
})
