"""Processing pipeline for C21_G19_SA2 (Long-term Health Conditions by Age by Sex, SA2+)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import sys

import pandas as pd

# Ensure project root on path when executed as a script
project_root = Path(__file__).parent.parent
if __name__ == "__main__" and str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.config import Settings
from processing.c21_g01_sa2 import split_code_and_label


@dataclass
class ProcessedOutputs:
    """Container for processed outputs."""

    fact: pd.DataFrame
    health_condition_lookup: pd.DataFrame
    geo_lookup: pd.DataFrame
    sex_lookup: pd.DataFrame
    age_lookup: pd.DataFrame
    geog_type_lookup: pd.DataFrame
    state_lookup: pd.DataFrame


def _ensure_processed_dir(settings: Settings) -> None:
    settings.processed_data_dir.mkdir(parents=True, exist_ok=True)


def _merge_and_write_lookup(new_df: pd.DataFrame, path: Path, sort_by: list[str]) -> pd.DataFrame:
    """Union new lookup rows with existing file (if present), de-duplicate, sort, and write."""

    if path.exists():
        existing = pd.read_parquet(path)
        merged = (
            pd.concat([existing, new_df], ignore_index=True)
            .drop_duplicates()
            .sort_values(sort_by)
            .reset_index(drop=True)
        )
    else:
        merged = new_df.sort_values(sort_by).reset_index(drop=True)

    merged.to_parquet(path, index=False)
    return merged


def transform_c21_g19_sa2(df: pd.DataFrame) -> ProcessedOutputs:
    """Transform raw C21_G19_SA2 data into tidy fact and lookup tables."""

    expected_cols = {
        "SEXP: Sex",
        "LTHP: Type of long-term health condition",
        "AGEP: Age",
        "REGION: Region",
        "REGION_TYPE: Region Type",
        "STATE: State",
        "TIME_PERIOD: Time Period",
        "OBS_VALUE",
        "DATAFLOW",
    }

    missing = [c for c in expected_cols if c not in df.columns and c != "DATAFLOW"]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    working = df.copy()
    if "DATAFLOW" in working.columns:
        working = working.drop(columns=["DATAFLOW"])

    # Split code/label pairs
    for src, code_col, label_col in [
        ("SEXP: Sex", "sex", "sex_label"),
        ("LTHP: Type of long-term health condition", "lthc", "health_condition"),
        ("AGEP: Age", "age_group", "age_group_label"),
        ("REGION: Region", "geog_id", "geog_name"),
        ("REGION_TYPE: Region Type", "geog_type", "geog_type_label"),
        ("STATE: State", "state", "state_label"),
    ]:
        codes_labels = working[src].apply(split_code_and_label)
        working[[code_col, label_col]] = pd.DataFrame(codes_labels.tolist(), index=working.index)

    # Rename remaining measures
    working = working.rename(
        columns={
            "TIME_PERIOD: Time Period": "year",
            "OBS_VALUE": "persons",
        }
    )

    # Cast numeric fields
    working["persons"] = pd.to_numeric(working["persons"], errors="coerce").astype("Int64")
    working["year"] = pd.to_numeric(working["year"], errors="coerce").astype("Int64")

    fact_cols = ["sex", "lthc", "age_group", "geog_id", "geog_type", "state", "year", "persons"]
    fact_df = working[fact_cols].copy()

    health_condition_lookup = (
        working[["lthc", "health_condition"]]
        .drop_duplicates()
        .sort_values(by=["lthc"])
        .reset_index(drop=True)
    )

    geo_lookup = (
        working[["geog_id", "geog_name", "geog_type", "state"]]
        .drop_duplicates()
        .sort_values(by=["geog_type", "geog_id"])
        .reset_index(drop=True)
    )

    sex_lookup = working[["sex", "sex_label"]].drop_duplicates().sort_values(by=["sex"]).reset_index(drop=True)
    age_lookup = (
        working[["age_group", "age_group_label"]]
        .drop_duplicates()
        .sort_values(by=["age_group"])
        .reset_index(drop=True)
    )
    geog_type_lookup = (
        working[["geog_type", "geog_type_label"]]
        .drop_duplicates()
        .sort_values(by=["geog_type"])
        .reset_index(drop=True)
    )
    state_lookup = (
        working[["state", "state_label"]]
        .drop_duplicates()
        .sort_values(by=["state"])
        .reset_index(drop=True)
    )

    return ProcessedOutputs(
        fact=fact_df.reset_index(drop=True),
        health_condition_lookup=health_condition_lookup,
        geo_lookup=geo_lookup,
        sex_lookup=sex_lookup,
        age_lookup=age_lookup,
        geog_type_lookup=geog_type_lookup,
        state_lookup=state_lookup,
    )


def process_c21_g19_sa2(
    input_path: str | Path | None = None,
    settings: Settings | None = None,
    write_output: bool = True,
    reuse_base_lookups: bool = True,
    merge_health_lookup: bool = True,
) -> ProcessedOutputs:
    """Load raw C21_G19_SA2 data and persist processed outputs.

    Args:
        input_path: Optional path to a raw Parquet file. If omitted, uses
            data/raw/c21_g19_sa2_health_conditions.parquet.
        settings: Optional Settings instance.
        write_output: Whether to write Parquet outputs to data/processed.
    """

    settings = settings or Settings()
    _ensure_processed_dir(settings)

    if input_path is None:
        input_path = settings.raw_data_dir / "c21_g19_sa2_health_conditions.parquet"

    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Raw data not found at: {input_path}")

    raw_df = pd.read_parquet(input_path)
    outputs = transform_c21_g19_sa2(raw_df)

    if write_output:
        outputs.fact.to_parquet(settings.processed_data_dir / "c21_g19_sa2_health_conditions.parquet", index=False)
        outputs.health_condition_lookup.to_parquet(
            settings.processed_data_dir / "c21_g19_sa2_health_condition_lookup.parquet", index=False
        )

        if merge_health_lookup:
            # Merge health condition codes into a common cross-dataset lookup
            common_health_path = settings.processed_data_dir / "common_health_condition_lookup.parquet"
            outputs.health_condition_lookup = _merge_and_write_lookup(
                outputs.health_condition_lookup, common_health_path, ["lthc"]
            )

        if reuse_base_lookups:
            # Union with base G01 lookups to avoid duplication across datasets
            outputs.geo_lookup = _merge_and_write_lookup(
                outputs.geo_lookup,
                settings.processed_data_dir / "c21_g01_sa2_geo_lookup.parquet",
                ["geog_type", "geog_id"],
            )
            outputs.sex_lookup = _merge_and_write_lookup(
                outputs.sex_lookup,
                settings.processed_data_dir / "c21_g01_sa2_sex_lookup.parquet",
                ["sex"],
            )
            outputs.age_lookup = _merge_and_write_lookup(
                outputs.age_lookup,
                settings.processed_data_dir / "c21_g01_sa2_age_lookup.parquet",
                ["age_group"],
            )
            outputs.geog_type_lookup = _merge_and_write_lookup(
                outputs.geog_type_lookup,
                settings.processed_data_dir / "c21_g01_sa2_geog_type_lookup.parquet",
                ["geog_type"],
            )
            outputs.state_lookup = _merge_and_write_lookup(
                outputs.state_lookup,
                settings.processed_data_dir / "c21_g01_sa2_state_lookup.parquet",
                ["state"],
            )
        else:
            outputs.geo_lookup.to_parquet(
                settings.processed_data_dir / "c21_g19_sa2_geo_lookup.parquet", index=False
            )
            outputs.sex_lookup.to_parquet(
                settings.processed_data_dir / "c21_g19_sa2_sex_lookup.parquet", index=False
            )
            outputs.age_lookup.to_parquet(
                settings.processed_data_dir / "c21_g19_sa2_age_lookup.parquet", index=False
            )
            outputs.geog_type_lookup.to_parquet(
                settings.processed_data_dir / "c21_g19_sa2_geog_type_lookup.parquet", index=False
            )
            outputs.state_lookup.to_parquet(
                settings.processed_data_dir / "c21_g19_sa2_state_lookup.parquet", index=False
            )

    return outputs


if __name__ == "__main__":
    process_c21_g19_sa2()
