"""Tests for C21_G19_SA2 processing (long-term health conditions)."""

import pandas as pd
import pytest

from processing.c21_g19_sa2 import transform_c21_g19_sa2


def test_transform_c21_g19_sa2_shapes_fact_and_lookups():
    sample = pd.DataFrame(
        {
            "DATAFLOW": ["ABS:C21_G19_SA2(1.0.0)", "ABS:C21_G19_SA2(1.0.0)"],
            "SEXP: Sex": ["1: Males", "2: Females"],
            "LTHP: Type of long-term health condition": [
                "81: Lung condition (including COPD or emphysema)",
                "10: Arthritis",
            ],
            "AGEP: Age": ["25_34: Age groups: 25-34 years", "65_74: Age groups: 65-74 years"],
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
            "OBS_VALUE": [123, 456],
        }
    )

    outputs = transform_c21_g19_sa2(sample)

    fact = outputs.fact
    assert list(fact.columns) == [
        "sex",
        "lthc",
        "age_group",
        "geog_id",
        "geog_type",
        "state",
        "year",
        "persons",
    ]

    assert fact.loc[0, "lthc"] == "81"
    assert fact.loc[0, "age_group"] == "25_34"
    assert fact.loc[0, "geog_id"] == "213051588"
    assert fact.loc[0, "geog_type"] == "SA2"
    assert fact.loc[0, "state"] == "2"
    assert fact.loc[0, "persons"] == 123

    # Health condition lookup
    hc_lookup = outputs.health_condition_lookup
    assert set(hc_lookup.columns) == {"lthc", "health_condition"}
    assert len(hc_lookup) == 2
    assert "Lung condition (including COPD or emphysema)" in hc_lookup["health_condition"].values

    # Geo lookup contains names and is de-duplicated
    geo_lookup = outputs.geo_lookup
    assert set(geo_lookup.columns) == {"geog_id", "geog_name", "geog_type", "state"}
    assert len(geo_lookup) == 2

    # Other lookups exist
    assert not outputs.sex_lookup.empty
    assert not outputs.age_lookup.empty
    assert not outputs.geog_type_lookup.empty
    assert not outputs.state_lookup.empty


@pytest.mark.parametrize(
    "missing_column",
    [
        "SEXP: Sex",
        "LTHP: Type of long-term health condition",
        "AGEP: Age",
        "REGION: Region",
        "REGION_TYPE: Region Type",
        "STATE: State",
        "TIME_PERIOD: Time Period",
        "OBS_VALUE",
    ],
)
def test_transform_c21_g19_sa2_raises_for_missing_column(missing_column):
    base_cols = {
        "DATAFLOW": ["ABS:C21_G19_SA2(1.0.0)"],
        "SEXP: Sex": ["3: Persons"],
        "LTHP: Type of long-term health condition": ["10: Arthritis"],
        "AGEP: Age": ["25_34: Age groups: 25-34 years"],
        "REGION: Region": ["21305: Wyndham"],
        "REGION_TYPE: Region Type": ["SA3: Statistical Area Level 3"],
        "STATE: State": ["2: Victoria"],
        "TIME_PERIOD: Time Period": ["2021"],
        "OBS_VALUE": [100],
    }
    base_cols.pop(missing_column, None)
    df = pd.DataFrame(base_cols)

    with pytest.raises(ValueError):
        transform_c21_g19_sa2(df)
