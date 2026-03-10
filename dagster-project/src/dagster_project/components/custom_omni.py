"""Custom OmniComponent that remaps Omni query asset keys to match dbt model keys."""

import dagster as dg
from dagster_omni import OmniComponent
from dagster_omni.objects import OmniQuery
from dagster_omni.translation import OmniTranslatorData


class CustomOmniComponent(OmniComponent):
    """Extends OmniComponent to connect Omni workbook assets to dbt model assets.

    The default OmniComponent creates query assets with keys based on Omni's
    internal table naming (e.g. 'BigQuery_omni_dbt_marts__fct_sessions').
    This subclass remaps those keys to match the dbt model asset keys
    (e.g. 'fct_sessions'), enabling lineage connections in the Dagster UI.
    """

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

            return spec.replace_attributes(
                key=dg.AssetKey([extracted_name]),
            )

        return spec
