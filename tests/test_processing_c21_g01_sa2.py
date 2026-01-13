"""Tests for C21_G01_SA2 processing."""

import pandas as pd
import pytest

from processing.c21_g01_sa2 import split_code_and_label, transform_c21_g01_sa2


def test_split_code_and_label_parses_values():
    code, label = split_code_and_label("3: Persons")
    assert code == "3"
    assert label == "Persons"

    code_only, label_only = split_code_and_label("SA2")
    assert code_only == "SA2"
    assert label_only is None

    none_code, none_label = split_code_and_label(None)
    assert none_code is None
    assert none_label is None


def test_transform_c21_g01_sa2_shapes_fact_and_lookups():
    sample = pd.DataFrame(
        {
            "DATAFLOW": ["ABS:C21_G01_SA2(1.0.0)", "ABS:C21_G01_SA2(1.0.0)"],
            "SEXP: Sex": ["1: Males", "2: Females"],
            "PCHAR: Selected person characteristic": [
                "65_74: Age groups: 65-74 years",
                "45_54: Age groups: 45-54 years",
            ],
            "REGION: Region": [
                "213051588: Truganina - South West",
                "114: Southern Highlands and Shoalhaven",
            ],
            "REGION_TYPE: Region Type": [
                "SA2: Statistical Area Level 2",
                "SA4: Statistical Area Level 4",
            ],
            "STATE: State": ["2: Victoria", "1: New South Wales"],
            "TIME_PERIOD: Time Period": ["2021", "2021"],
            "OBS_VALUE": [601, 11921],
        }
    )

    outputs = transform_c21_g01_sa2(sample)

    fact = outputs.fact
    assert list(fact.columns) == ["sex", "age_group", "geog_id", "geog_type", "state", "year", "persons"]
    assert fact.loc[0, "sex"] == "1"
    assert fact.loc[0, "age_group"] == "65_74"
    assert fact.loc[0, "geog_id"] == "213051588"
    assert fact.loc[0, "geog_type"] == "SA2"
    assert fact.loc[0, "state"] == "2"
    assert fact.loc[0, "persons"] == 601

    # Geo lookup contains names and is de-duplicated
    geo_lookup = outputs.geo_lookup
    assert set(geo_lookup.columns) == {"geog_id", "geog_name", "geog_type", "state"}
    assert len(geo_lookup) == 2
    assert "Truganina - South West" in geo_lookup["geog_name"].values

    # Other lookups exist
    assert not outputs.sex_lookup.empty
    assert not outputs.age_lookup.empty
    assert not outputs.geog_type_lookup.empty
    assert not outputs.state_lookup.empty


@pytest.mark.parametrize(
    "missing_column",
    [
        "SEXP: Sex",
        "PCHAR: Selected person characteristic",
        "REGION: Region",
        "REGION_TYPE: Region Type",
        "STATE: State",
        "TIME_PERIOD: Time Period",
        "OBS_VALUE",
    ],
)
def test_transform_c21_g01_sa2_raises_for_missing_column(missing_column):
    base_cols = {
        "DATAFLOW": ["ABS:C21_G01_SA2(1.0.0)"],
        "SEXP: Sex": ["3: Persons"],
        "PCHAR: Selected person characteristic": ["25_34: Age groups: 25-34 years"],
        "REGION: Region": ["21305: Wyndham"],
        "REGION_TYPE: Region Type": ["SA3: Statistical Area Level 3"],
        "STATE: State": ["2: Victoria"],
        "TIME_PERIOD: Time Period": ["2021"],
        "OBS_VALUE": [100],
    }
    base_cols.pop(missing_column, None)
    df = pd.DataFrame(base_cols)

    with pytest.raises(ValueError):
        transform_c21_g01_sa2(df)
