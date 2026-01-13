# ABS Census Data Extraction

A Python tool for exploring and extracting Australian Bureau of Statistics (ABS) Census data using their SDMX REST API. Designed for one-off bulk extractions that are saved to Parquet format for efficient reuse.

## Features

- **Data Discovery**: Explore available Census dataflows and their structures
- **Bulk Extraction**: Pull large volumes of data for multiple regions/time periods
- **Parquet Storage**: Save extracted data in efficient columnar format for fast reloading
- **DRY Principles**: Reusable API client and extraction utilities

## Project Structure

```
ABS_Data/
├── src/
│   ├── __init__.py
│   ├── api_client.py      # Core ABS API client
│   ├── config.py          # Configuration and constants
│   ├── data_extractor.py  # Bulk extraction and Parquet storage
│   └── explorer.py        # Census data discovery utilities
├── tests/
│   ├── test_api_client.py
│   └── test_config.py
├── data/
│   ├── raw/               # Raw extracted Parquet files
│   └── processed/         # Transformed/cleaned data
├── data_extraction/       # Dataflow extraction scripts (formerly examples)
├── docs/                  # Additional documentation
├── main.py               # Exploration entry point
└── pyproject.toml
```

## Setup

```powershell
# Install dependencies
uv sync

# Install with dev dependencies (for testing)
uv sync --all-extras
```

## Quick Start

### 1. Explore Available Data

Run the main exploration script to discover Census dataflows:

```powershell
uv run python main.py
```

### 2. Interactive Exploration

```python
from src.explorer import CensusExplorer

explorer = CensusExplorer()

# Find 2021 Census dataflows
census_dfs = explorer.find_census_dataflows(year="2021")
print(census_dfs)

# Search by topic
population_dfs = explorer.find_dataflows_by_topic("population")
indigenous_dfs = explorer.find_dataflows_by_topic("indigenous")
health_dfs = explorer.find_dataflows_by_topic("health")

# Get details about a specific dataflow
explorer.summarize_dataflow("C21_G01_SA2")  # Example dataflow ID

# List available geography codes
geo_codes = explorer.list_geography_codes("C21_G01_SA2")
```

### 3. Extract Data

```python
from src.data_extractor import DataExtractor

extractor = DataExtractor()

# Extract a full dataflow
df = extractor.extract_dataflow(
    dataflow_id="C21_G01_SA2",
    start_period="2021",
    end_period="2021",
    output_name="census_2021_g01_sa2"
)

# Extract specific regions
sa2_codes = ["101011001", "101011002", "101011003"]  # Example SA2 codes
df = extractor.extract_multiple_regions(
    dataflow_id="C21_G01_SA2",
    region_codes=sa2_codes,
    region_dimension="REGION",  # Dimension name varies by dataflow
    output_name="census_2021_selected_sa2s"
)

# Extract many regions in batches (avoids API limits)
all_sa2_codes = [...]  # Large list of SA2 codes
df = extractor.extract_in_batches(
    dataflow_id="C21_G01_SA2",
    codes=all_sa2_codes,
    dimension_id="REGION",
    batch_size=50,
    output_name="census_2021_all_sa2s"
)
```

### 5. Process C21_G01_SA2 (Selected Person Characteristics)

Once the raw Parquet is downloaded (e.g., via `data_extraction/extract_c21_g01_sa2.py`), you can normalize it:

```python
from processing import process_c21_g01_sa2

outputs = process_c21_g01_sa2()

# Main fact table
fact = outputs.fact  # columns: sex, age_group, geog_id, geog_type, state, year, persons

# Lookups
geo_lookup = outputs.geo_lookup        # geog_id -> geog_name
sex_lookup = outputs.sex_lookup        # sex codes -> labels
age_lookup = outputs.age_lookup        # age codes -> labels
```

Processed files are written to `data/processed/` by default:

- `c21_g01_sa2_population.parquet`
- `c21_g01_sa2_geo_lookup.parquet`
- `c21_g01_sa2_sex_lookup.parquet`
- `c21_g01_sa2_age_lookup.parquet`
- `c21_g01_sa2_geog_type_lookup.parquet`
- `c21_g01_sa2_state_lookup.parquet`

### 6. Process C21_G19_SA2 (Long-term Health Conditions)

Once the raw Parquet is downloaded (e.g., via `data_extraction/extract_c21_g19_sa2.py`), you can normalize it:

```python
from processing import process_c21_g19_sa2

outputs = process_c21_g19_sa2()

# Main fact table
fact = outputs.fact  # columns: sex, lthc, age_group, geog_id, geog_type, state, year, persons

# Lookups
health_lookup = outputs.health_condition_lookup
geo_lookup = outputs.geo_lookup

# Shared lookups
# By default, any new codes merge into the C21_G01 lookup files (sex/age/geography/state).
# Set reuse_base_lookups=False to write G19-specific lookup files instead.
```

Processed files are written to `data/processed/` by default:

- `c21_g19_sa2_health_conditions.parquet`
- `c21_g19_sa2_health_condition_lookup.parquet`
- `c21_g19_sa2_geo_lookup.parquet`
- `c21_g19_sa2_sex_lookup.parquet`
- `c21_g19_sa2_age_lookup.parquet`
- `c21_g19_sa2_geog_type_lookup.parquet`
- `c21_g19_sa2_state_lookup.parquet`

### 4. Reload Saved Data

```python
from src.data_extractor import DataExtractor

extractor = DataExtractor()

# List saved files
print(extractor.list_saved_data())

# Load previously extracted data
df = extractor.load_parquet("census_2021_g01_sa2")
```

## API Client Direct Usage

For more control, use the API client directly:

```python
from src.api_client import ABSClient

client = ABSClient()

# List all dataflows
dataflows = client.list_dataflows()

# Search dataflows
census_dfs = client.list_dataflows(search_term="Census")

# Get dataflow structure (dimensions and codelists)
structure = client.get_dataflow_structure("C21_G01_SA2")
dimensions = client.parse_dimensions(structure)

# Get a specific codelist
sa2_codes = client.get_codelist("CL_SA2_2021")

# Get data with filters
df = client.get_data(
    dataflow_id="C21_G01_SA2",
    data_key="all",
    start_period="2021",
    end_period="2021"
)

# Get data with dimension-based filters
df = client.get_data_with_filters(
    dataflow_id="C21_G01_SA2",
    dimension_filters={
        "REGION": ["101011001", "101011002"],
        "SEX": "1"  # Males only
    }
)
```

## ABS API Concepts

### Dataflows

A dataflow is a dataset. Each Census table at each geography level is typically a separate dataflow.

### Data Keys

Data keys filter results using dimension codes:

- Format: `dim1.dim2.dim3` (dots separate dimensions)
- Wildcard: Leave dimension empty (e.g., `1..Q` wildcards the 2nd dimension)
- Multiple values: Use `+` (e.g., `1+2.AUS.Q` for codes 1 OR 2)
- All data: Use `all` as the data key

### Geography Levels

- **SA1**: Statistical Area Level 1 (smallest, ~400 people)
- **SA2**: Statistical Area Level 2 (~10,000 people, suburb-like)
- **SA3**: Statistical Area Level 3 (~30,000-130,000 people)
- **SA4**: Statistical Area Level 4 (labour market regions)
- **LGA**: Local Government Areas
- **STE**: States and Territories

## Running Tests

```powershell
uv run pytest
```

## Configuration

Settings can be overridden via environment variables (prefix with `ABS_`):

```powershell
$env:ABS_API_TIMEOUT = "180"  # Increase timeout for large requests
```

Or create a `.env` file in the project root.

## Links

- [ABS Data API User Guide](https://www.abs.gov.au/about/data-services/application-programming-interfaces-apis/data-api-user-guide)
- [ABS Data Explorer](https://explore.data.abs.gov.au/) - Web interface for exploring data
- [SDMX REST API Documentation](https://github.com/sdmx-twg/sdmx-rest/wiki)
