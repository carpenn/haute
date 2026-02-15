/**
 * Convert a human label to a valid Python function name (preserves casing).
 *
 * This MUST stay in sync with the backend implementation:
 *   src/haute/graph_utils.py → _sanitize_func_name()
 *
 * Both implementations follow the same rules:
 *   1. Trim whitespace
 *   2. Replace spaces and hyphens with underscores
 *   3. Strip non-alphanumeric/underscore characters
 *   4. Prefix with "node_" if it starts with a digit
 *   5. Fall back to "unnamed_node" if empty
 */
export function sanitizeName(label: string): string {
  let name = label.trim().replace(/[\s-]/g, "_")
  name = name.replace(/[^a-zA-Z0-9_]/g, "")
  if (name && /^[0-9]/.test(name)) name = `node_${name}`
  return name || "unnamed_node"
}
