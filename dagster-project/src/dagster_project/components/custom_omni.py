"""Custom OmniComponent that remaps Omni query asset keys to match dbt model keys."""

import json
from pathlib import Path

import dagster as dg
from dagster_dbt.asset_utils import default_asset_key_fn
from dagster_omni import OmniComponent
from dagster_omni.objects import OmniQuery, OmniWorkspaceData
from dagster_omni.translation import OmniTranslatorData


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
    """Extends OmniComponent to connect Omni workbook assets to dbt model assets.

    The default OmniComponent creates query assets with keys based on Omni's
    internal table naming (e.g. 'BigQuery_omni_dbt_marts__fct_sessions').
    This subclass remaps those keys to match the dbt model asset keys
    (e.g. AssetKey(['marts', 'fct_sessions'])), enabling lineage connections
    in the Dagster UI.

    Uses the dbt manifest to look up the correct asset key for each model,
    so it works regardless of schema configuration.
    """

    _dbt_key_lookup: dict[str, dg.AssetKey] | None = None

    def _get_dbt_key_lookup(self, context: dg.ComponentLoadContext) -> dict[str, dg.AssetKey]:
        if self._dbt_key_lookup is None:
            # Navigate from .../defs/omni_ingest -> repo root -> dbt-project
            # context.path = dagster-project/src/dagster_project/defs/omni_ingest
            dagster_project_root = context.path.parent.parent.parent.parent
            dbt_project_path = dagster_project_root.parent / "dbt-project"
            manifest_path = dbt_project_path / "target" / "manifest.json"
            if manifest_path.exists():
                self._dbt_key_lookup = _build_dbt_key_lookup(manifest_path)
            else:
                self._dbt_key_lookup = {}
        return self._dbt_key_lookup

    def get_asset_spec(
        self, context: dg.ComponentLoadContext, data: OmniTranslatorData
    ) -> dg.AssetSpec | None:
        spec = super().get_asset_spec(context, data)

        if spec is None:
            return None

        if isinstance(data.obj, OmniQuery):
            table_name = data.obj.query_config.table
            parts = table_name.split("__")
            extracted_name = parts[-1] if len(parts) > 1 else parts[0]

            dbt_lookup = self._get_dbt_key_lookup(context)
            key = dbt_lookup.get(extracted_name, dg.AssetKey([extracted_name]))

            return spec.replace_attributes(key=key)

        return spec
