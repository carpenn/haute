"""Tests for haute._config_io — config file I/O and path conventions."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from haute._config_io import (
    FOLDER_TO_NODE_TYPE,
    NODE_TYPE_TO_FOLDER,
    _CODE_KEYS,
    collect_node_configs,
    config_path_for_node,
    has_config_folder,
    load_node_config,
    remove_config_file,
    save_node_config,
)
from haute._types import NodeType
from tests.conftest import make_edge, make_graph, make_node


# ---------------------------------------------------------------------------
# Mapping consistency
# ---------------------------------------------------------------------------


class TestMappings:
    def test_folder_to_node_type_is_reverse_of_node_type_to_folder(self):
        assert len(FOLDER_TO_NODE_TYPE) == len(NODE_TYPE_TO_FOLDER)
        for nt, folder in NODE_TYPE_TO_FOLDER.items():
            assert FOLDER_TO_NODE_TYPE[folder] is nt

    def test_all_non_transform_non_submodel_types_mapped(self):
        excluded = {
            NodeType.POLARS,
            NodeType.SUBMODEL,
            NodeType.SUBMODEL_PORT,
        }
        for nt in NodeType:
            if nt in excluded:
                assert not has_config_folder(nt), f"{nt} should NOT have config folder"
            else:
                assert has_config_folder(nt), f"{nt} should have config folder"


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


class TestConfigPathForNode:
    def test_relative_path(self):
        p = config_path_for_node(NodeType.BANDING, "my_banding")
        assert p == Path("config/banding/my_banding.json")

    def test_absolute_path_with_base_dir(self, tmp_path):
        p = config_path_for_node(NodeType.BANDING, "my_banding", base_dir=tmp_path)
        assert p == tmp_path / "config" / "banding" / "my_banding.json"

    def test_all_types_produce_valid_paths(self):
        for nt, folder in NODE_TYPE_TO_FOLDER.items():
            p = config_path_for_node(nt, "test_node")
            assert p == Path(f"config/{folder}/test_node.json")

    def test_transform_raises(self):
        with pytest.raises(ValueError, match="No config folder"):
            config_path_for_node(NodeType.POLARS, "my_transform")


# ---------------------------------------------------------------------------
# Read / Write
# ---------------------------------------------------------------------------


class TestSaveAndLoad:
    def test_save_creates_directories_and_file(self, tmp_path):
        config = {"path": "data/input.parquet", "sourceType": "flat_file"}
        rel = save_node_config(NodeType.DATA_SOURCE, "my_source", config, tmp_path)
        assert rel == Path("config/data_source/my_source.json")
        assert (tmp_path / rel).is_file()

    def test_saved_content_is_valid_json(self, tmp_path):
        config = {"path": "data/input.parquet"}
        save_node_config(NodeType.DATA_SOURCE, "src", config, tmp_path)
        loaded = load_node_config("config/data_source/src.json", base_dir=tmp_path)
        assert loaded == config

    def test_code_key_excluded_from_json(self, tmp_path):
        config = {"path": "model.pkl", "fileType": "pickle", "code": "df = obj.predict(df)"}
        save_node_config(NodeType.EXTERNAL_FILE, "ext", config, tmp_path)
        loaded = load_node_config("config/load_file/ext.json", base_dir=tmp_path)
        assert "code" not in loaded
        assert loaded["path"] == "model.pkl"
        assert loaded["fileType"] == "pickle"

    def test_load_nonexistent_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_node_config("config/data_source/nope.json", base_dir=tmp_path)

    def test_round_trip_complex_config(self, tmp_path):
        config = {
            "factors": [
                {
                    "banding": "continuous",
                    "column": "DrivAge",
                    "outputColumn": "DrivAgeBand",
                    "rules": [
                        {"op1": ">", "val1": "0", "op2": "<=", "val2": "20", "assignment": "0-20"},
                    ],
                },
            ],
        }
        save_node_config(NodeType.BANDING, "band", config, tmp_path)
        loaded = load_node_config("config/banding/band.json", base_dir=tmp_path)
        assert loaded == config


class TestRemoveConfigFile:
    def test_remove_existing_file(self, tmp_path):
        config = {"path": "data.parquet"}
        save_node_config(NodeType.DATA_SOURCE, "src", config, tmp_path)
        assert remove_config_file(NodeType.DATA_SOURCE, "src", tmp_path)
        assert not (tmp_path / "config" / "data_source" / "src.json").exists()

    def test_remove_nonexistent_returns_false(self, tmp_path):
        assert not remove_config_file(NodeType.DATA_SOURCE, "nope", tmp_path)

    def test_remove_transform_returns_false(self, tmp_path):
        assert not remove_config_file(NodeType.POLARS, "t", tmp_path)


# ---------------------------------------------------------------------------
# collect_node_configs
# ---------------------------------------------------------------------------


class TestCollectNodeConfigs:
    def test_datasource_and_transform(self):
        graph = make_graph({
            "nodes": [
                {"id": "src", "data": {"label": "src", "nodeType": "dataSource", "config": {"path": "d.parquet"}}},
                {"id": "t", "data": {"label": "clean", "nodeType": "polars", "config": {"code": ".filter()"}}},
            ],
            "edges": [{"id": "e1", "source": "src", "target": "t"}],
        })
        configs = collect_node_configs(graph)
        assert "config/data_source/src.json" in configs
        # Transform should NOT have a config file
        assert not any("transform" in k for k in configs)

    def test_code_key_excluded(self):
        graph = make_graph({
            "nodes": [
                {"id": "ext", "data": {
                    "label": "ext",
                    "nodeType": "externalFile",
                    "config": {"path": "m.pkl", "fileType": "pickle", "code": "df = obj(df)"},
                }},
            ],
            "edges": [],
        })
        configs = collect_node_configs(graph)
        content = json.loads(configs["config/load_file/ext.json"])
        assert "code" not in content
        assert content["path"] == "m.pkl"

    def test_instance_nodes_skipped(self):
        graph = make_graph({
            "nodes": [
                {"id": "orig", "data": {"label": "orig", "nodeType": "dataSource", "config": {"path": "d.parquet"}}},
                {"id": "inst", "data": {
                    "label": "inst",
                    "nodeType": "dataSource",
                    "config": {"path": "d.parquet", "instanceOf": "orig"},
                }},
            ],
            "edges": [],
        })
        configs = collect_node_configs(graph)
        assert "config/data_source/orig.json" in configs
        assert "config/data_source/inst.json" not in configs

    def test_all_node_types_produce_config(self):
        """Every non-transform, non-submodel node type should generate a config file."""
        nodes = [
            {"id": "a", "data": {"label": "a", "nodeType": "apiInput", "config": {"path": "d.json"}}},
            {"id": "b", "data": {"label": "b", "nodeType": "dataSource", "config": {"path": "d.parquet"}}},
            {"id": "c", "data": {"label": "c", "nodeType": "liveSwitch", "config": {"mode": "live"}}},
            {"id": "d", "data": {"label": "d", "nodeType": "modelScore", "config": {"task": "regression"}}},
            {"id": "e", "data": {"label": "e", "nodeType": "banding", "config": {"factors": []}}},
            {"id": "f", "data": {"label": "f", "nodeType": "ratingStep", "config": {"tables": []}}},
            {"id": "g", "data": {"label": "g", "nodeType": "output", "config": {"fields": []}}},
            {"id": "h", "data": {"label": "h", "nodeType": "dataSink", "config": {"path": "o.parquet"}}},
            {"id": "i", "data": {"label": "i", "nodeType": "externalFile", "config": {"path": "m.pkl"}}},
            {"id": "j", "data": {"label": "j", "nodeType": "modelling", "config": {"target": "y"}}},
            {"id": "k", "data": {"label": "k", "nodeType": "optimiser", "config": {"mode": "online"}}},
            {"id": "l", "data": {"label": "l", "nodeType": "optimiserApply", "config": {}}},
            {"id": "m", "data": {"label": "m", "nodeType": "scenarioExpander", "config": {}}},
            {"id": "n", "data": {"label": "n", "nodeType": "constant", "config": {"values": []}}},
        ]
        graph = make_graph({"nodes": nodes, "edges": []})
        configs = collect_node_configs(graph)
        assert len(configs) == 14

    def test_config_paths_always_use_forward_slashes(self):
        """Config path keys must use forward slashes (not OS-dependent backslashes)."""
        graph = make_graph({
            "nodes": [
                {"id": "b", "data": {"label": "age_band", "nodeType": "banding", "config": {"factors": []}}},
                {"id": "r", "data": {"label": "area_rate", "nodeType": "ratingStep", "config": {"tables": []}}},
            ],
            "edges": [],
        })
        configs = collect_node_configs(graph)
        for path in configs:
            assert "\\" not in path, f"Config path contains backslash: {path}"
            assert path.startswith("config/"), f"Config path should start with config/: {path}"
