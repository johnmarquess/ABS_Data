"""Census data exploration utilities."""

import pandas as pd
from src.api_client import ABSClient
from src.config import Settings


class CensusExplorer:
    """Explore available Census dataflows and their structures."""

    # Known Census dataflow prefixes
    CENSUS_PREFIXES = {
        "C21": "2021 Census",
        "C16": "2016 Census",
        "C11": "2011 Census",
        "C06": "2006 Census",
        "C01": "2001 Census",
    }

    # Common topic keywords to search for
    TOPIC_KEYWORDS = {
        "population": ["population", "persons", "usual resident"],
        "age": ["age", "median age"],
        "indigenous": ["indigenous", "aboriginal", "torres strait"],
        "health": ["health", "disability", "assistance"],
        "income": ["income", "earnings"],
        "education": ["education", "qualification", "school"],
        "employment": ["employment", "occupation", "labour force"],
        "housing": ["dwelling", "housing", "tenure", "rent", "mortgage"],
        "family": ["family", "household", "children"],
        "migration": ["birthplace", "migration", "ancestry", "citizenship"],
        "language": ["language", "english proficiency"],
    }

    def __init__(
        self, client: ABSClient | None = None, settings: Settings | None = None
    ):
        """Initialize the explorer.

        Args:
            client: Optional ABSClient instance.
            settings: Optional Settings instance.
        """
        self.settings = settings or Settings()
        self.client = client or ABSClient(self.settings)
        self._dataflows_cache: pd.DataFrame | None = None

    @property
    def dataflows(self) -> pd.DataFrame:
        """Get all dataflows, caching for repeated use."""
        if self._dataflows_cache is None:
            self._dataflows_cache = self.client.list_dataflows()
        return self._dataflows_cache

    def refresh_dataflows(self) -> pd.DataFrame:
        """Force refresh of the dataflows cache."""
        self._dataflows_cache = self.client.list_dataflows()
        return self._dataflows_cache

    def find_census_dataflows(
        self, year: str | None = None, geography_level: str | None = None
    ) -> pd.DataFrame:
        """Find Census-specific dataflows.

        Args:
            year: Filter by census year (e.g., "2021", "2016").
            geography_level: Filter by geography (e.g., "SA2", "SA3", "LGA").

        Returns:
            DataFrame of matching Census dataflows.
        """
        df = self.dataflows.copy()

        # Filter for Census dataflows (usually start with C followed by year)
        census_mask = df["id"].str.match(r"^C\d{2}", na=False) | df["name"].str.contains(
            "Census", case=False, na=False
        )
        df = df[census_mask]

        if year:
            year_suffix = year[-2:]  # "2021" -> "21"
            df = df[df["id"].str.startswith(f"C{year_suffix}", na=False)]

        if geography_level:
            df = df[
                df["id"].str.contains(geography_level, case=False, na=False)
                | df["name"].str.contains(geography_level, case=False, na=False)
            ]

        return df.reset_index(drop=True)

    def find_dataflows_by_topic(self, topic: str) -> pd.DataFrame:
        """Find dataflows related to a topic.

        Args:
            topic: Topic keyword or category (e.g., "population", "health").

        Returns:
            DataFrame of matching dataflows.
        """
        df = self.dataflows.copy()

        # Check if topic is a known category
        keywords = self.TOPIC_KEYWORDS.get(topic.lower(), [topic.lower()])

        # Build regex pattern from keywords
        pattern = "|".join(keywords)
        mask = df["name"].str.contains(pattern, case=False, na=False) | df[
            "id"
        ].str.contains(pattern, case=False, na=False)

        return df[mask].reset_index(drop=True)

    def get_dataflow_details(self, dataflow_id: str) -> dict:
        """Get detailed information about a dataflow.

        Args:
            dataflow_id: The dataflow ID to inspect.

        Returns:
            Dictionary with dataflow details including dimensions and codelists.
        """
        structure = self.client.get_dataflow_structure(
            dataflow_id, include_codelists=True
        )
        dimensions = self.client.parse_dimensions(structure)

        # Extract codelist information
        codelists = {}
        for codelist in structure.get("data", {}).get("codelists", []):
            cl_id = codelist.get("id")
            codes = [
                {"code": c.get("id"), "name": c.get("name")}
                for c in codelist.get("codes", [])
            ]
            codelists[cl_id] = pd.DataFrame(codes)

        return {
            "dataflow_id": dataflow_id,
            "dimensions": dimensions,
            "codelists": codelists,
            "raw_structure": structure,
        }

    def summarize_dataflow(self, dataflow_id: str) -> None:
        """Print a human-readable summary of a dataflow.

        Args:
            dataflow_id: The dataflow ID to summarize.
        """
        details = self.get_dataflow_details(dataflow_id)

        print(f"\n{'='*60}")
        print(f"Dataflow: {dataflow_id}")
        print(f"{'='*60}\n")

        print("Dimensions:")
        print("-" * 40)
        for _, dim in details["dimensions"].iterrows():
            codelist = dim.get("codelist", "N/A")
            print(f"  {dim['position']}. {dim['id']}")
            if codelist and codelist in details["codelists"]:
                cl_df = details["codelists"][codelist]
                print(f"     Codelist: {codelist} ({len(cl_df)} codes)")

        print(f"\n{'='*60}\n")

    def list_geography_codes(
        self, dataflow_id: str, geography_dimension: str | None = None
    ) -> pd.DataFrame:
        """List available geography codes for a dataflow.

        Args:
            dataflow_id: The dataflow ID.
            geography_dimension: Optional dimension ID. Auto-detected if not provided.

        Returns:
            DataFrame of geography codes.
        """
        details = self.get_dataflow_details(dataflow_id)

        # Auto-detect geography dimension
        if not geography_dimension:
            geo_patterns = ["REGION", "ASGS", "SA1", "SA2", "SA3", "SA4", "LGA", "STE"]
            for _, dim in details["dimensions"].iterrows():
                if any(p in dim["id"].upper() for p in geo_patterns):
                    geography_dimension = dim["id"]
                    break

        if not geography_dimension:
            print("Could not auto-detect geography dimension. Available dimensions:")
            print(details["dimensions"])
            return pd.DataFrame()

        # Find the codelist for this dimension
        dim_row = details["dimensions"][
            details["dimensions"]["id"] == geography_dimension
        ]
        if dim_row.empty:
            print(f"Dimension {geography_dimension} not found")
            return pd.DataFrame()

        codelist = dim_row.iloc[0].get("codelist")
        if codelist and codelist in details["codelists"]:
            return details["codelists"][codelist]

        return pd.DataFrame()

    def search_dataflows(self, search_term: str) -> pd.DataFrame:
        """Search dataflows by name or ID.

        Args:
            search_term: Text to search for.

        Returns:
            DataFrame of matching dataflows.
        """
        return self.client.list_dataflows(search_term=search_term)

    def get_available_topics(self) -> list[str]:
        """Get list of known topic categories.

        Returns:
            List of topic category names.
        """
        return list(self.TOPIC_KEYWORDS.keys())
