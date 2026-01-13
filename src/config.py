"""Configuration settings for the ABS Data project."""

from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment or defaults."""

    # API Configuration
    api_base_url: str = "https://data.api.abs.gov.au/rest"
    api_timeout: int = 120  # seconds - Census data can be large
    api_agency_id: str = "ABS"

    # Data storage paths
    project_root: Path = Path(__file__).parent.parent
    data_dir: Path = project_root / "data"
    raw_data_dir: Path = data_dir / "raw"
    processed_data_dir: Path = data_dir / "processed"

    # Default response format
    default_format: str = "csv"  # csv is easier to work with for bulk data

    class Config:
        env_prefix = "ABS_"
        env_file = ".env"


# Geographic code prefixes for reference
GEOGRAPHY_LEVELS = {
    "SA1": "Statistical Area Level 1",
    "SA2": "Statistical Area Level 2",
    "SA3": "Statistical Area Level 3",
    "SA4": "Statistical Area Level 4",
    "LGA": "Local Government Area",
    "STE": "State/Territory",
    "AUS": "Australia",
}

# Common Census dataflow IDs (to be populated during exploration)
CENSUS_DATAFLOWS = {
    # These will be discovered and documented during exploration
    # Example structure:
    # "C21_G01_SA2": "2021 Census - General Community Profile - SA2 level",
}
