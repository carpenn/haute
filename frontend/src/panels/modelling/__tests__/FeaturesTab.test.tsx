import { describe, it, expect, afterEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import { FeaturesTab } from "../FeaturesTab"
import { makeTrainResult } from "../../../test-utils/factories"

afterEach(cleanup)

describe("FeaturesTab", () => {
  it("shows empty state when no feature importance data", () => {
    const result = makeTrainResult({ feature_importance: [] })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("No feature importance data available")).toBeInTheDocument()
  })

  it("renders all features (no cap)", () => {
    const features = Array.from({ length: 20 }, (_, i) => ({
      feature: `feat_${i}`,
      importance: 20 - i,
    }))
    const result = makeTrainResult({ feature_importance: features })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("feat_0")).toBeInTheDocument()
    expect(screen.getByText("feat_19")).toBeInTheDocument()
    expect(screen.getByText("20 features")).toBeInTheDocument()
  })

  it("shows feature count summary", () => {
    const result = makeTrainResult({
      feature_importance: [
        { feature: "a", importance: 10 },
        { feature: "b", importance: 5 },
      ],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("2 features")).toBeInTheDocument()
  })

  it("shows singular 'feature' for single feature", () => {
    const result = makeTrainResult({
      feature_importance: [{ feature: "only_one", importance: 10 }],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("1 feature")).toBeInTheDocument()
  })

  it("shows type switcher when loss importance is available", () => {
    const result = makeTrainResult({
      feature_importance: [{ feature: "x", importance: 10 }],
      feature_importance_loss: [{ feature: "x", importance: 7 }],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("Prediction")).toBeInTheDocument()
    expect(screen.getByText("Loss")).toBeInTheDocument()
  })

  it("does not show type switcher when only prediction data exists", () => {
    const result = makeTrainResult({
      feature_importance: [{ feature: "x", importance: 10 }],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.queryByText("Prediction")).not.toBeInTheDocument()
    expect(screen.queryByText("Loss")).not.toBeInTheDocument()
  })

  it("switching to Loss tab shows loss features", () => {
    const result = makeTrainResult({
      feature_importance: [{ feature: "pred_feat", importance: 10 }],
      feature_importance_loss: [{ feature: "loss_feat", importance: 7 }],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("pred_feat")).toBeInTheDocument()

    fireEvent.click(screen.getByText("Loss"))
    expect(screen.getByText("loss_feat")).toBeInTheDocument()
  })

  it("shows SHAP tab when shap_summary is available", () => {
    const result = makeTrainResult({
      feature_importance: [{ feature: "x", importance: 10 }],
      shap_summary: [{ feature: "shap_feat", mean_abs_shap: 5 }],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("SHAP")).toBeInTheDocument()

    fireEvent.click(screen.getByText("SHAP"))
    expect(screen.getByText("shap_feat")).toBeInTheDocument()
  })

  it("displays importance values", () => {
    const result = makeTrainResult({
      feature_importance: [{ feature: "age", importance: 25.3 }],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("25.3")).toBeInTheDocument()
  })

  it("displays rank numbers", () => {
    const result = makeTrainResult({
      feature_importance: [
        { feature: "a", importance: 10 },
        { feature: "b", importance: 5 },
      ],
    })
    render(<FeaturesTab result={result} />)
    expect(screen.getByText("1")).toBeInTheDocument()
    expect(screen.getByText("2")).toBeInTheDocument()
  })
})
