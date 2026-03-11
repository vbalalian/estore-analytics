"""Custom OmniComponent that remaps Omni query asset keys to match dbt model keys
and adds topic join dependencies that the Omni API doesn't expose."""

import json
from pathlib import Path
from typing import Any

import dagster as dg
import yaml
from dagster_dbt.asset_utils import default_asset_key_fn
from dagster_omni import OmniComponent
from dagster_omni.objects import OmniDocument, OmniQuery
from dagster_omni.translation import OmniTranslatorData


def _extract_table_name(omni_table_name: str) -> str:
    """Extract the dbt model name from an Omni table name.

    Omni returns table names as 'connection_schema__table_name'.
    Split on '__' and take the last part.
    """
    parts = omni_table_name.split("__")
    return parts[-1] if len(parts) > 1 else parts[0]


def _extract_join_views(joins: dict[str, Any]) -> list[str]:
    """Recursively extract all view names from a topic's joins config."""
    views = []
    for view_name, nested in joins.items():
        views.append(view_name)
        if isinstance(nested, dict) and nested:
            views.extend(_extract_join_views(nested))
    return views


def _build_topic_deps(omni_dir: Path) -> dict[str, list[str]]:
    """Parse topic YAML files to build a mapping from base table -> all tables used.

    Returns a dict like:
        {"fct_sessions": ["fct_sessions", "dim_users", "dim_user_rfm"]}

    Keys and values are extracted dbt model names (not Omni internal names).
    """
    topic_deps: dict[str, list[str]] = {}

    for topic_file in omni_dir.glob("**/*.topic.yaml"):
        with open(topic_file) as f:
            topic = yaml.safe_load(f)

        if not topic or "base_view" not in topic:
            continue

        base_table = _extract_table_name(topic["base_view"])
        all_tables = [base_table]

        joins = topic.get("joins", {})
        if joins:
            for view_name in _extract_join_views(joins):
                extracted = _extract_table_name(view_name)
                if extracted not in all_tables:
                    all_tables.append(extracted)

        topic_deps[base_table] = all_tables

    return topic_deps


def _build_dbt_key_lookup(manifest_path: Path) -> dict[str, dg.AssetKey]:
    """Build a lookup from dbt model/snapshot name to its Dagster asset key."""
    with open(manifest_path) as f:
        manifest = json.load(f)

    lookup = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") in ("model", "snapshot"):
            lookup[node["name"]] = default_asset_key_fn(node)
    return lookup


class CustomOmniComponent(OmniComponent):
    """Extends OmniComponent to:
    1. Remap Omni query asset keys to match dbt model asset keys
    2. Add topic join dependencies the Omni API doesn't expose

    The default OmniComponent only sees each query's base table. Topic joins
    (e.g. fct_events -> dim_products) are invisible to the API. This subclass
    parses the Omni topic YAML files to discover join dependencies and adds
    them to the workbook asset specs.
    """

    _dbt_key_lookup: dict[str, dg.AssetKey] | None = None
    _topic_deps: dict[str, list[str]] | None = None

    def _get_repo_root(self, context: dg.ComponentLoadContext) -> Path:
        """Navigate from component path to repo root.

        context.path = .../dagster-project/src/dagster_project/defs/omni_ingest
        repo root = context.path.parent.parent.parent.parent.parent
        """
        return context.path.parent.parent.parent.parent.parent

    def _get_dbt_key_lookup(self, context: dg.ComponentLoadContext) -> dict[str, dg.AssetKey]:
        if self._dbt_key_lookup is None:
            manifest_path = self._get_repo_root(context) / "dbt-project" / "target" / "manifest.json"
            if manifest_path.exists():
                self._dbt_key_lookup = _build_dbt_key_lookup(manifest_path)
            else:
                self._dbt_key_lookup = {}
        return self._dbt_key_lookup

    def _get_topic_deps(self, context: dg.ComponentLoadContext) -> dict[str, list[str]]:
        if self._topic_deps is None:
            omni_dir = self._get_repo_root(context) / "omni" / "BigQuery"
            if omni_dir.exists():
                self._topic_deps = _build_topic_deps(omni_dir)
            else:
                self._topic_deps = {}
        return self._topic_deps

    def _resolve_key(self, table_name: str, context: dg.ComponentLoadContext) -> dg.AssetKey:
        """Resolve a dbt model name to its Dagster asset key via manifest lookup."""
        dbt_lookup = self._get_dbt_key_lookup(context)
        return dbt_lookup.get(table_name, dg.AssetKey([table_name]))

    def get_asset_spec(
        self, context: dg.ComponentLoadContext, data: OmniTranslatorData
    ) -> dg.AssetSpec | None:
        spec = super().get_asset_spec(context, data)

        if spec is None:
            return None

        if isinstance(data.obj, OmniQuery):
            table_name = data.obj.query_config.table
            extracted_name = _extract_table_name(table_name)
            return spec.replace_attributes(
                key=self._resolve_key(extracted_name, context),
            )

        # For document assets (workbooks), add topic join deps
        if isinstance(data.obj, OmniDocument):
            topic_deps = self._get_topic_deps(context)
            existing_dep_keys = {dep.asset_key for dep in spec.deps}
            additional_deps = []

            for query in data.obj.queries:
                base_table = _extract_table_name(query.query_config.table)
                all_tables = topic_deps.get(base_table, [base_table])

                for table in all_tables:
                    key = self._resolve_key(table, context)
                    if key not in existing_dep_keys:
                        additional_deps.append(dg.AssetDep(key))
                        existing_dep_keys.add(key)

            if additional_deps:
                return spec.replace_attributes(
                    deps=[*spec.deps, *additional_deps],
                )

        return spec
