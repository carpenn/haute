export type ContinuousRule = { op1: string; val1: string; op2: string; val2: string; assignment: string }
export type CategoricalRule = { value: string; assignment: string }
export type BandingFactor = {
  banding: string
  column: string
  outputColumn: string
  rules: (ContinuousRule | CategoricalRule)[]
  default?: string | null
}
