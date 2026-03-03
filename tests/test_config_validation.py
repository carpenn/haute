"""Tests for _config_validation – lightweight config key warnings."""

from __future__ import annotations

import pytest

from haute._config_validation import (
    VALID_KEYS,
    warn_unrecognized_config_keys,
)
from haute._types import NodeType

# ---------------------------------------------------------------------------
# VALID_KEYS registry sanity checks
# ---------------------------------------------------------------------------


class TestValidKeysRegistry:
    """Ensure the registry covers all node types that have TypedDicts."""

    def test_all_typed_dict_node_types_present(self):
        """Every node type with a TypedDict should have an entry."""
        expected = {
            NodeType.API_INPUT,
            NodeType.DATA_SOURCE,
            NodeType.TRANSFORM,
            NodeType.MODEL_SCORE,
            NodeType.BANDING,
            NodeType.RATING_STEP,
            NodeType.OUTPUT,
            NodeType.DATA_SINK,
            NodeType.EXTERNAL_FILE,
            NodeType.LIVE_SWITCH,
            NodeType.MODELLING,
            NodeType.OPTIMISER,
            NodeType.SCENARIO_EXPANDER,
            NodeType.OPTIMISER_APPLY,
            NodeType.CONSTANT,
            NodeType.SUBMODEL,
        }
        assert expected.issubset(VALID_KEYS.keys())

    def test_universal_keys_included(self):
        """instanceOf and inputMapping should be valid for every type."""
        for nt, keys in VALID_KEYS.items():
            assert "instanceOf" in keys, f"instanceOf missing from {nt}"
            assert "inputMapping" in keys, f"inputMapping missing from {nt}"

    @pytest.mark.parametrize(
        "node_type, expected_key",
        [
            (NodeType.API_INPUT, "path"),
            (NodeType.DATA_SOURCE, "sourceType"),
            (NodeType.MODEL_SCORE, "run_id"),
            (NodeType.BANDING, "factors"),
            (NodeType.RATING_STEP, "tables"),
            (NodeType.OUTPUT, "fields"),
            (NodeType.DATA_SINK, "format"),
            (NodeType.EXTERNAL_FILE, "fileType"),
            (NodeType.LIVE_SWITCH, "input_scenario_map"),
            (NodeType.MODELLING, "algorithm"),
            (NodeType.OPTIMISER, "constraints"),
            (NodeType.SCENARIO_EXPANDER, "quote_id"),
            (NodeType.OPTIMISER_APPLY, "artifact_path"),
            (NodeType.CONSTANT, "values"),
            (NodeType.SUBMODEL, "file"),
        ],
    )
    def test_known_key_present(self, node_type, expected_key):
        """Spot-check that well-known keys appear in each type's valid set."""
        assert expected_key in VALID_KEYS[node_type]


# ---------------------------------------------------------------------------
# warn_unrecognized_config_keys
# ---------------------------------------------------------------------------


class TestWarnUnrecognizedConfigKeys:
    def test_no_warning_for_valid_keys(self):
        """Config with only valid keys should produce no warnings."""
        bad = warn_unrecognized_config_keys(
            NodeType.API_INPUT,
            {"path": "/data.json", "row_id_column": "id"},
        )
        assert bad == []

    def test_warns_on_unrecognized_key(self):
        """An unknown key should be returned and logged."""
        bad = warn_unrecognized_config_keys(
            NodeType.API_INPUT,
            {"path": "/data.json", "bogus_key": 42},
        )
        assert bad == ["bogus_key"]

    def test_multiple_unrecognized_keys_sorted(self):
        """Multiple bad keys are returned in sorted order."""
        bad = warn_unrecognized_config_keys(
            NodeType.OUTPUT,
            {"fields": ["a"], "zebra": 1, "alpha": 2},
        )
        assert bad == ["alpha", "zebra"]

    def test_instance_of_always_valid(self):
        """instanceOf is a universal key, valid for any node type."""
        bad = warn_unrecognized_config_keys(
            NodeType.DATA_SOURCE,
            {"path": "x.parquet", "sourceType": "flat_file", "instanceOf": "other"},
        )
        assert bad == []

    def test_input_mapping_always_valid(self):
        """inputMapping is a universal key, valid for any node type."""
        bad = warn_unrecognized_config_keys(
            NodeType.TRANSFORM,
            {"code": "x", "inputMapping": {"a": "b"}},
        )
        assert bad == []

    def test_empty_config_no_warning(self):
        """Empty config dict should produce no warnings."""
        bad = warn_unrecognized_config_keys(NodeType.TRANSFORM, {})
        assert bad == []

    def test_string_node_type_accepted(self):
        """Should accept raw string values matching NodeType enum."""
        bad = warn_unrecognized_config_keys(
            "apiInput",
            {"path": "/data.json", "nope": True},
        )
        assert bad == ["nope"]

    def test_unknown_node_type_string_returns_empty(self):
        """An unrecognised node-type string should not crash, just return []."""
        bad = warn_unrecognized_config_keys("totallyFake", {"x": 1})
        assert bad == []

    def test_logs_warning_via_structlog(self, capsys):
        """Ensure the warning actually appears in the log output."""
        warn_unrecognized_config_keys(
            NodeType.OUTPUT,
            {"fields": ["a"], "bad_key": 99},
            node_label="my_output_node",
        )
        captured = capsys.readouterr()
        assert "unrecognized_config_keys" in captured.out
        assert "bad_key" in captured.out
        assert "my_output_node" in captured.out

    def test_never_raises(self):
        """Even with weird input, the function must not raise."""
        # None node type
        assert warn_unrecognized_config_keys(None, {"x": 1}) == []  # type: ignore[arg-type]
        # Non-dict config -- guard against AttributeError
        # (the function signature says dict, but let's be defensive)


# ---------------------------------------------------------------------------
# Integration: _build_node_config produces valid configs
# ---------------------------------------------------------------------------


class TestBuildNodeConfigProducesValidKeys:
    """Ensure that _build_node_config only sets keys that pass validation."""

    @pytest.mark.parametrize(
        "node_type, kwargs, body, params",
        [
            pytest.param(
                NodeType.API_INPUT,
                {"path": "d.json", "api_input": True, "row_id_column": "id"},
                "", [],
                id="api_input",
            ),
            pytest.param(
                NodeType.DATA_SOURCE,
                {"path": "d.parquet"},
                "", [],
                id="datasource_flat",
            ),
            pytest.param(
                NodeType.DATA_SOURCE,
                {"table": "cat.sch.tbl"},
                "", [],
                id="datasource_databricks",
            ),
            pytest.param(
                NodeType.MODEL_SCORE,
                {"model_score": True, "source_type": "run", "run_id": "abc"},
                "", ["df"],
                id="model_score",
            ),
            pytest.param(
                NodeType.BANDING,
                {"factors": [{"banding": "continuous", "column": "x", "rules": []}]},
                "", ["df"],
                id="banding_multi",
            ),
            pytest.param(
                NodeType.BANDING,
                {"banding": "continuous", "column": "x", "rules": []},
                "", ["df"],
                id="banding_single",
            ),
            pytest.param(
                NodeType.RATING_STEP,
                {"tables": [{"name": "T", "factors": ["x"], "entries": []}]},
                "", ["df"],
                id="rating_step",
            ),
            pytest.param(
                NodeType.OUTPUT,
                {"fields": ["a", "b"]},
                "", ["df"],
                id="output",
            ),
            pytest.param(
                NodeType.DATA_SINK,
                {"sink": "out.csv", "format": "csv"},
                "", ["df"],
                id="data_sink",
            ),
            pytest.param(
                NodeType.EXTERNAL_FILE,
                {"external": "m.pkl", "file_type": "pickle"},
                "", ["df"],
                id="external_file",
            ),
            pytest.param(
                NodeType.EXTERNAL_FILE,
                {"external": "m.cbm", "file_type": "catboost", "model_class": "regressor"},
                "", ["df"],
                id="external_file_catboost",
            ),
            pytest.param(
                NodeType.LIVE_SWITCH,
                {"live_switch": True, "input_scenario_map": {}},
                "", ["a", "b"],
                id="live_switch",
            ),
            pytest.param(
                NodeType.OPTIMISER,
                {"optimiser": True, "mode": "online", "quote_id": "qid"},
                "", ["df"],
                id="optimiser",
            ),
            pytest.param(
                NodeType.OPTIMISER_APPLY,
                {"optimiser_apply": True, "artifact_path": "opt.json"},
                "", ["df"],
                id="optimiser_apply",
            ),
            pytest.param(
                NodeType.SCENARIO_EXPANDER,
                {"scenario_expander": True, "quote_id": "qid", "steps": 10},
                "", ["df"],
                id="scenario_expander",
            ),
            pytest.param(
                NodeType.MODELLING,
                {"modelling": True, "name": "m", "target": "y", "algorithm": "catboost"},
                "", ["df"],
                id="modelling",
            ),
            pytest.param(
                NodeType.CONSTANT,
                {"constant": True, "values": [{"name": "x", "value": "1"}]},
                "", [],
                id="constant",
            ),
            pytest.param(
                NodeType.TRANSFORM,
                {},
                '    """doc"""\n    return df',
                ["df"],
                id="transform",
            ),
        ],
    )
    def test_built_config_has_no_unrecognized_keys(
        self, node_type, kwargs, body, params,
    ):
        from haute._parser_helpers import _build_node_config

        config = _build_node_config(node_type, kwargs, body, params)
        bad = warn_unrecognized_config_keys(node_type, config)
        assert bad == [], f"Unrecognized keys in {node_type}: {bad}"
