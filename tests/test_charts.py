"""Tests for SVG chart generation (haute.modelling._charts)."""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from haute.modelling._charts import (
    COLOR_ACTUAL,
    COLOR_BARS,
    COLOR_EVAL,
    COLOR_IMPORTANCE,
    COLOR_PREDICTED,
    COLOR_SHAP,
    COLOR_TRAIN,
    render_ave_feature_svg,
    render_double_lift_svg,
    render_horizontal_bars_svg,
    render_lorenz_curve_svg,
    render_loss_curve_svg,
    render_pdp_feature_svg,
    render_residuals_svg,
    render_scatter_svg,
)


def _parse_svg(svg_str: str) -> ET.Element:
    """Parse an SVG string into an ElementTree — validates it's well-formed XML."""
    return ET.fromstring(svg_str)


# ---------------------------------------------------------------------------
# Cross-chart parametrized tests
# ---------------------------------------------------------------------------


def _sample_double_lift() -> list[dict]:
    return [
        {"decile": i + 1, "actual": i * 0.1, "predicted": i * 0.11, "count": 100 + i * 5}
        for i in range(10)
    ]


def _sample_loss_data() -> list[dict]:
    return [
        {"iteration": i, "train_RMSE": 1.0 / (i + 1), "eval_RMSE": 1.1 / (i + 1)} for i in range(50)
    ]


def _sample_importance() -> list[dict]:
    return [{"feature": f"feat_{i}", "importance": 1.0 - i * 0.1} for i in range(5)]


def _sample_numeric_bins() -> list[dict]:
    return [
        {
            "label": f"{i * 10:.0f}\u2013{(i + 1) * 10:.0f}",
            "exposure": 100.0,
            "avg_actual": 0.5 + i * 0.1,
            "avg_predicted": 0.5 + i * 0.12,
        }
        for i in range(8)
    ]


def _sample_lorenz_curve() -> list[dict]:
    return [{"cum_weight_frac": i / 10, "cum_actual_frac": (i / 10) ** 0.6} for i in range(11)]


def _sample_residuals_histogram() -> list[dict]:
    return [
        {"bin_center": -5 + i, "count": 10 + i * 5, "weighted_count": 10.0 + i * 5.0}
        for i in range(10)
    ]


def _sample_scatter_points() -> list[dict]:
    return [{"actual": i * 0.1, "predicted": i * 0.11, "weight": 1.0} for i in range(20)]


def _sample_pdp_numeric() -> list[dict]:
    return [{"value": i * 10, "avg_prediction": 0.5 + i * 0.02} for i in range(10)]


def _sample_pdp_categorical() -> list[dict]:
    return [
        {"value": cat, "avg_prediction": v}
        for cat, v in [("sedan", 0.3), ("suv", 0.5), ("truck", 0.7)]
    ]


def _render_double_lift_data():
    return render_double_lift_svg(_sample_double_lift())


def _render_loss_data():
    return render_loss_curve_svg(_sample_loss_data(), best_iteration=20)


def _render_importance_data():
    return render_horizontal_bars_svg(_sample_importance(), "feature", "importance", title="Test")


def _render_ave_data():
    return render_ave_feature_svg("age", _sample_numeric_bins(), is_categorical=False)


def _render_lorenz_data():
    return render_lorenz_curve_svg(_sample_lorenz_curve(), _sample_lorenz_curve())


def _render_residuals_data():
    return render_residuals_svg(
        _sample_residuals_histogram(), {"mean": 0.1, "std": 0.5, "skew": 0.3}
    )


def _render_scatter_data():
    return render_scatter_svg(_sample_scatter_points())


def _render_pdp_numeric_data():
    return render_pdp_feature_svg("age", _sample_pdp_numeric(), "numeric")


def _render_pdp_cat_data():
    return render_pdp_feature_svg("vehicle", _sample_pdp_categorical(), "categorical")


@pytest.mark.parametrize(
    "render_fn",
    [
        pytest.param(_render_double_lift_data, id="double_lift"),
        pytest.param(_render_loss_data, id="loss_curve"),
        pytest.param(_render_importance_data, id="horizontal_bars"),
        pytest.param(_render_ave_data, id="ave_feature"),
        pytest.param(_render_lorenz_data, id="lorenz_curve"),
        pytest.param(_render_residuals_data, id="residuals"),
        pytest.param(_render_scatter_data, id="scatter"),
        pytest.param(_render_pdp_numeric_data, id="pdp_numeric"),
        pytest.param(_render_pdp_cat_data, id="pdp_categorical"),
    ],
)
class TestChartValidXml:
    """All chart renderers must produce well-formed SVG."""

    def test_valid_xml(self, render_fn):
        root = _parse_svg(render_fn())
        assert root.tag == "{http://www.w3.org/2000/svg}svg"


@pytest.mark.parametrize(
    "render_fn",
    [
        pytest.param(lambda: render_double_lift_svg([]), id="double_lift"),
        pytest.param(lambda: render_loss_curve_svg([]), id="loss_curve"),
        pytest.param(
            lambda: render_horizontal_bars_svg([], "f", "v", title="Empty"), id="horizontal_bars"
        ),
        pytest.param(
            lambda: render_ave_feature_svg("empty", [], is_categorical=False), id="ave_feature"
        ),
        pytest.param(lambda: render_lorenz_curve_svg([], []), id="lorenz_curve"),
        pytest.param(lambda: render_residuals_svg([]), id="residuals"),
        pytest.param(lambda: render_scatter_svg([]), id="scatter"),
        pytest.param(lambda: render_pdp_feature_svg("empty", [], "numeric"), id="pdp"),
    ],
)
class TestChartEmptyPlaceholder:
    """All chart renderers show placeholder text for empty data."""

    def test_empty_data_shows_placeholder(self, render_fn):
        root = _parse_svg(render_fn())
        texts = [t.text for t in root.findall(".//{http://www.w3.org/2000/svg}text")]
        assert any("No" in (t or "") or "empty" in (t or "").lower() for t in texts)


# ---------------------------------------------------------------------------
# Double Lift
# ---------------------------------------------------------------------------


class TestDoubleLiftSvg:
    @pytest.fixture()
    def sample_data(self) -> list[dict]:
        return [
            {"decile": i + 1, "actual": i * 0.1, "predicted": i * 0.11, "count": 100 + i * 5}
            for i in range(10)
        ]

    def test_contains_bars(self, sample_data):
        svg = render_double_lift_svg(sample_data)
        root = _parse_svg(svg)
        rects = root.findall(".//{http://www.w3.org/2000/svg}rect")
        # Background rect + bars for each decile
        assert len(rects) >= 10

    def test_contains_lines(self, sample_data):
        svg = render_double_lift_svg(sample_data)
        root = _parse_svg(svg)
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        assert len(polylines) == 2  # actual + predicted

    def test_correct_colors(self, sample_data):
        svg = render_double_lift_svg(sample_data)
        assert COLOR_ACTUAL in svg
        assert COLOR_PREDICTED in svg
        assert COLOR_BARS in svg

    # empty_data_placeholder covered by TestChartEmptyPlaceholder

    # ---------------------------------------------------------------------------
    # Loss Curve
    # ---------------------------------------------------------------------------


class TestLossCurveSvg:
    @pytest.fixture()
    def loss_data(self) -> list[dict]:
        return [
            {"iteration": i, "train_RMSE": 1.0 / (i + 1), "eval_RMSE": 1.1 / (i + 1)}
            for i in range(50)
        ]

    def test_contains_train_and_eval_lines(self, loss_data):
        svg = render_loss_curve_svg(loss_data)
        root = _parse_svg(svg)
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        assert len(polylines) == 2

    def test_correct_colors(self, loss_data):
        svg = render_loss_curve_svg(loss_data)
        assert COLOR_TRAIN in svg

    def test_best_iteration_marker(self, loss_data):
        svg = render_loss_curve_svg(loss_data, best_iteration=20)
        assert "best=20" in svg

    def test_no_eval_only_train(self):
        data = [{"iteration": i, "train_RMSE": 1.0 / (i + 1)} for i in range(10)]
        svg = render_loss_curve_svg(data)
        root = _parse_svg(svg)
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        assert len(polylines) == 1  # train only

    # empty_data_placeholder covered by TestChartEmptyPlaceholder

    def test_subsamples_large_data(self):
        data = [{"iteration": i, "train_RMSE": 1.0 / (i + 1)} for i in range(500)]
        svg = render_loss_curve_svg(data)
        root = _parse_svg(svg)
        # Should still produce valid SVG
        assert root.tag == "{http://www.w3.org/2000/svg}svg"


# ---------------------------------------------------------------------------
# Horizontal Bars
# ---------------------------------------------------------------------------


class TestHorizontalBarsSvg:
    @pytest.fixture()
    def importance_data(self) -> list[dict]:
        return [{"feature": f"feat_{i}", "importance": 1.0 - i * 0.1} for i in range(5)]

    def test_contains_bars(self, importance_data):
        svg = render_horizontal_bars_svg(
            importance_data,
            "feature",
            "importance",
            title="Test",
        )
        root = _parse_svg(svg)
        rects = root.findall(".//{http://www.w3.org/2000/svg}rect")
        # Background + 5 bars
        assert len(rects) >= 5

    def test_correct_color(self, importance_data):
        svg = render_horizontal_bars_svg(
            importance_data,
            "feature",
            "importance",
            title="Test",
            color=COLOR_SHAP,
        )
        assert COLOR_SHAP in svg

    def test_default_color_is_importance(self, importance_data):
        svg = render_horizontal_bars_svg(
            importance_data,
            "feature",
            "importance",
            title="Test",
        )
        assert COLOR_IMPORTANCE in svg

    # empty_data_placeholder covered by TestChartEmptyPlaceholder

    def test_max_items(self):
        data = [{"feature": f"f{i}", "importance": float(i)} for i in range(30)]
        svg = render_horizontal_bars_svg(
            data,
            "feature",
            "importance",
            title="Test",
            max_items=10,
        )
        root = _parse_svg(svg)
        # Top 10 by importance (f29..f20) should be present, low importance (f0) should not
        texts = [t.text for t in root.findall(".//{http://www.w3.org/2000/svg}text")]
        assert "f29" in texts  # highest importance
        assert "f0" not in texts  # lowest importance, should be truncated


# ---------------------------------------------------------------------------
# AvE Feature
# ---------------------------------------------------------------------------


class TestAveFeatureSvg:
    @pytest.fixture()
    def numeric_bins(self) -> list[dict]:
        return [
            {
                "label": f"{i * 10:.0f}–{(i + 1) * 10:.0f}",
                "exposure": 100.0,
                "avg_actual": 0.5 + i * 0.1,
                "avg_predicted": 0.5 + i * 0.12,
            }
            for i in range(8)
        ]

    @pytest.fixture()
    def categorical_bins(self) -> list[dict]:
        return [
            {"label": cat, "exposure": 50.0, "avg_actual": 0.3, "avg_predicted": 0.35}
            for cat in ["sedan", "suv", "truck", "van"]
        ]

    def test_categorical_valid_xml(self, categorical_bins):
        svg = render_ave_feature_svg("vehicle_type", categorical_bins, is_categorical=True)
        root = _parse_svg(svg)
        assert root.tag == "{http://www.w3.org/2000/svg}svg"

    def test_contains_circles(self, numeric_bins):
        svg = render_ave_feature_svg("age", numeric_bins, is_categorical=False)
        root = _parse_svg(svg)
        circles = root.findall(".//{http://www.w3.org/2000/svg}circle")
        # At least circles for actual and predicted dots
        assert len(circles) >= len(numeric_bins) * 2

    def test_correct_colors(self, numeric_bins):
        svg = render_ave_feature_svg("age", numeric_bins, is_categorical=False)
        assert COLOR_ACTUAL in svg
        assert COLOR_PREDICTED in svg
        assert COLOR_BARS in svg

    # empty_bins_placeholder covered by TestChartEmptyPlaceholder

    def test_single_bin_dot_only(self):
        """Single bin should produce dots, not a line."""
        bins = [{"label": "3.14", "exposure": 100.0, "avg_actual": 0.5, "avg_predicted": 0.6}]
        svg = render_ave_feature_svg("const", bins, is_categorical=False)
        root = _parse_svg(svg)
        circles = root.findall(".//{http://www.w3.org/2000/svg}circle")
        assert len(circles) >= 2  # one for actual, one for predicted
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        assert len(polylines) == 0  # no line for single point


# ---------------------------------------------------------------------------
# Lorenz Curve
# ---------------------------------------------------------------------------


class TestLorenzCurveSvg:
    def test_contains_two_curves(self):
        model = _sample_lorenz_curve()
        perfect = [
            {"cum_weight_frac": i / 10, "cum_actual_frac": (i / 10) ** 0.3} for i in range(11)
        ]
        svg = render_lorenz_curve_svg(model, perfect)
        root = _parse_svg(svg)
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        assert len(polylines) == 2  # model + perfect

    def test_contains_diagonal(self):
        svg = render_lorenz_curve_svg(_sample_lorenz_curve(), _sample_lorenz_curve())
        root = _parse_svg(svg)
        lines = root.findall(".//{http://www.w3.org/2000/svg}line")
        # Grid lines + diagonal
        dashed = [l for l in lines if l.get("stroke-dasharray")]
        assert len(dashed) >= 1

    def test_correct_colors(self):
        svg = render_lorenz_curve_svg(_sample_lorenz_curve(), _sample_lorenz_curve())
        assert COLOR_ACTUAL in svg  # model curve
        assert COLOR_EVAL in svg  # perfect curve

    def test_model_only(self):
        svg = render_lorenz_curve_svg(_sample_lorenz_curve(), [])
        root = _parse_svg(svg)
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        assert len(polylines) == 1


# ---------------------------------------------------------------------------
# Residuals Histogram
# ---------------------------------------------------------------------------


class TestResidualsSvg:
    def test_contains_bars(self):
        svg = render_residuals_svg(_sample_residuals_histogram())
        root = _parse_svg(svg)
        rects = root.findall(".//{http://www.w3.org/2000/svg}rect")
        # background + bars
        assert len(rects) >= 10

    def test_stats_annotation(self):
        stats = {"mean": 0.1234, "std": 0.5678, "skew": 0.3}
        svg = render_residuals_svg(_sample_residuals_histogram(), stats)
        assert "mean=" in svg
        assert "std=" in svg
        assert "skew=" in svg

    def test_no_stats_no_crash(self):
        svg = render_residuals_svg(_sample_residuals_histogram())
        root = _parse_svg(svg)
        assert root.tag == "{http://www.w3.org/2000/svg}svg"


# ---------------------------------------------------------------------------
# Scatter
# ---------------------------------------------------------------------------


class TestScatterSvg:
    def test_contains_dots(self):
        svg = render_scatter_svg(_sample_scatter_points())
        root = _parse_svg(svg)
        circles = root.findall(".//{http://www.w3.org/2000/svg}circle")
        assert len(circles) == 20

    def test_contains_diagonal(self):
        svg = render_scatter_svg(_sample_scatter_points())
        root = _parse_svg(svg)
        lines = root.findall(".//{http://www.w3.org/2000/svg}line")
        dashed = [l for l in lines if l.get("stroke-dasharray")]
        assert len(dashed) >= 1

    def test_axis_labels(self):
        svg = render_scatter_svg(_sample_scatter_points())
        assert "Predicted" in svg
        assert "Actual" in svg


# ---------------------------------------------------------------------------
# PDP
# ---------------------------------------------------------------------------


class TestPdpFeatureSvg:
    def test_numeric_contains_line(self):
        svg = render_pdp_feature_svg("age", _sample_pdp_numeric(), "numeric")
        root = _parse_svg(svg)
        polylines = root.findall(".//{http://www.w3.org/2000/svg}polyline")
        assert len(polylines) == 1

    def test_numeric_contains_dots(self):
        svg = render_pdp_feature_svg("age", _sample_pdp_numeric(), "numeric")
        root = _parse_svg(svg)
        circles = root.findall(".//{http://www.w3.org/2000/svg}circle")
        assert len(circles) == 10

    def test_categorical_contains_bars(self):
        svg = render_pdp_feature_svg("vehicle", _sample_pdp_categorical(), "categorical")
        root = _parse_svg(svg)
        rects = root.findall(".//{http://www.w3.org/2000/svg}rect")
        # background + 3 bars
        assert len(rects) >= 3

    def test_feature_name_in_title(self):
        svg = render_pdp_feature_svg("my_feature", _sample_pdp_numeric(), "numeric")
        assert "my_feature" in svg
