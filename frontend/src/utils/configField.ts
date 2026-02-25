/**
 * Type-safe accessor for node config values.
 * Replaces the repetitive `(config.key as T) || default` pattern.
 *
 * Uses nullish coalescing (`??`) — only falls back on null/undefined.
 * For string fields where empty string should also fall back, callers
 * can chain: `configField(config, "key", "") || "actualDefault"`.
 */
export function configField<T>(config: Record<string, unknown>, key: string, fallback: T): T {
  const val = config[key]
  return (val as T) ?? fallback
}
