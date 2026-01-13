"""ABS Data API client for querying Census and other statistical data."""

from typing import Any
import httpx
import pandas as pd
from io import StringIO

from src.config import Settings


class ABSClient:
    """Client for interacting with the ABS Data API (SDMX REST)."""

    # Accept headers for different response formats
    ACCEPT_HEADERS = {
        "xml": "application/xml",
        "json": "application/vnd.sdmx.data+json",
        "csv": "text/csv",
        "csv_labels": "application/vnd.sdmx.data+csv;labels=both",
        "structure_json": "application/vnd.sdmx.structure+json",
    }

    def __init__(self, settings: Settings | None = None):
        """Initialize the ABS API client.

        Args:
            settings: Optional Settings object. Uses defaults if not provided.
        """
        self.settings = settings or Settings()
        self.base_url = self.settings.api_base_url
        self.timeout = self.settings.api_timeout
        self.agency_id = self.settings.api_agency_id

    def _get_client(self) -> httpx.Client:
        """Create a configured HTTP client."""
        return httpx.Client(timeout=self.timeout)

    def _build_url(self, *parts: str) -> str:
        """Build a URL from path parts."""
        clean_parts = [str(p).strip("/") for p in parts if p]
        return f"{self.base_url}/{'/'.join(clean_parts)}"

    # -------------------------------------------------------------------------
    # Metadata Discovery Methods
    # -------------------------------------------------------------------------

    def list_dataflows(
        self, search_term: str | None = None, as_dataframe: bool = True
    ) -> pd.DataFrame | dict:
        """List all available dataflows (datasets).

        Args:
            search_term: Optional filter to search dataflow names/IDs.
            as_dataframe: If True, returns a pandas DataFrame.

        Returns:
            DataFrame or dict of available dataflows.
        """
        url = self._build_url("dataflow", self.agency_id)

        with self._get_client() as client:
            response = client.get(
                url, headers={"Accept": self.ACCEPT_HEADERS["structure_json"]}
            )
            response.raise_for_status()

        data = response.json()
        dataflows = self._parse_dataflows(data)

        if search_term and as_dataframe:
            search_lower = search_term.lower()
            dataflows = dataflows[
                dataflows["name"].str.lower().str.contains(search_lower, na=False)
                | dataflows["id"].str.lower().str.contains(search_lower, na=False)
            ]

        return dataflows if as_dataframe else data

    def _parse_dataflows(self, json_data: dict) -> pd.DataFrame:
        """Parse dataflow JSON response into a DataFrame."""
        dataflows = []
        structures = json_data.get("data", {}).get("dataflows", [])

        for df in structures:
            dataflows.append(
                {
                    "id": df.get("id"),
                    "name": df.get("name"),
                    "version": df.get("version"),
                    "agency_id": df.get("agencyID"),
                }
            )

        return pd.DataFrame(dataflows)

    def get_dataflow_structure(
        self, dataflow_id: str, include_codelists: bool = True
    ) -> dict:
        """Get the structure (dimensions, codelists) for a dataflow.

        Args:
            dataflow_id: The ID of the dataflow (e.g., "C21_G01_SA2").
            include_codelists: If True, includes referenced codelists.

        Returns:
            Dictionary containing structure information.
        """
        references = "descendants" if include_codelists else "none"
        url = self._build_url("dataflow", self.agency_id, dataflow_id)

        with self._get_client() as client:
            response = client.get(
                url,
                params={"references": references},
                headers={"Accept": self.ACCEPT_HEADERS["structure_json"]},
            )
            response.raise_for_status()

        return response.json()

    def get_datastructure(
        self, structure_id: str, include_codelists: bool = True
    ) -> dict:
        """Get a Data Structure Definition (DSD) with optional codelists.

        Args:
            structure_id: The ID of the data structure.
            include_codelists: If True, includes referenced codelists.

        Returns:
            Dictionary containing the DSD and related structures.
        """
        references = "children" if include_codelists else "none"
        url = self._build_url("datastructure", self.agency_id, structure_id)

        with self._get_client() as client:
            response = client.get(
                url,
                params={"references": references},
                headers={"Accept": self.ACCEPT_HEADERS["structure_json"]},
            )
            response.raise_for_status()

        return response.json()

    def get_codelist(self, codelist_id: str) -> pd.DataFrame:
        """Get a codelist (dimension values) as a DataFrame.

        Args:
            codelist_id: The ID of the codelist (e.g., "CL_SA2_2021").

        Returns:
            DataFrame with code IDs and names.
        """
        url = self._build_url("codelist", self.agency_id, codelist_id)

        with self._get_client() as client:
            response = client.get(
                url, headers={"Accept": self.ACCEPT_HEADERS["structure_json"]}
            )
            response.raise_for_status()

        return self._parse_codelist(response.json())

    def _parse_codelist(self, json_data: dict) -> pd.DataFrame:
        """Parse codelist JSON into a DataFrame."""
        codes = []
        codelists = json_data.get("data", {}).get("codelists", [])

        for codelist in codelists:
            codelist_id = codelist.get("id")
            for code in codelist.get("codes", []):
                codes.append(
                    {
                        "codelist_id": codelist_id,
                        "code": code.get("id"),
                        "name": code.get("name"),
                        "description": code.get("description"),
                    }
                )

        return pd.DataFrame(codes)

    def parse_dimensions(self, structure_data: dict) -> pd.DataFrame:
        """Extract dimensions from a structure response.

        Args:
            structure_data: JSON response from get_dataflow_structure.

        Returns:
            DataFrame with dimension information.
        """
        dimensions = []
        data_structures = structure_data.get("data", {}).get("dataStructures", [])

        for ds in data_structures:
            components = ds.get("dataStructureComponents", {})
            dim_list = components.get("dimensionList", {}).get("dimensions", [])

            for dim in dim_list:
                codelist_ref = dim.get("localRepresentation", {}).get("enumeration")
                dimensions.append(
                    {
                        "id": dim.get("id"),
                        "position": dim.get("position"),
                        "codelist": codelist_ref,
                        "concept": dim.get("conceptIdentity"),
                    }
                )

        return pd.DataFrame(dimensions).sort_values("position")

    # -------------------------------------------------------------------------
    # Data Retrieval Methods
    # -------------------------------------------------------------------------

    def get_data(
        self,
        dataflow_id: str,
        data_key: str = "all",
        start_period: str | None = None,
        end_period: str | None = None,
        response_format: str = "csv_labels",
    ) -> pd.DataFrame | str:
        """Retrieve data from a dataflow.

        Args:
            dataflow_id: The dataflow ID (e.g., "ABS,RES_DWELL").
            data_key: Data key filter (e.g., "1.2.3" or "all").
            start_period: Start period filter (e.g., "2021").
            end_period: End period filter (e.g., "2021").
            response_format: One of 'csv', 'csv_labels', 'json', 'xml'.

        Returns:
            DataFrame for CSV formats, raw string/dict for others.
        """
        # Handle dataflow_id with or without agency prefix
        if "," not in dataflow_id:
            dataflow_id = f"{self.agency_id},{dataflow_id}"

        url = self._build_url("data", dataflow_id, data_key)

        params = {}
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        accept_header = self.ACCEPT_HEADERS.get(
            response_format, self.ACCEPT_HEADERS["csv_labels"]
        )

        with self._get_client() as client:
            response = client.get(url, params=params, headers={"Accept": accept_header})
            response.raise_for_status()

        if response_format in ("csv", "csv_labels"):
            return pd.read_csv(StringIO(response.text))
        elif response_format == "json":
            return response.json()
        else:
            return response.text

    def get_data_with_filters(
        self,
        dataflow_id: str,
        dimension_filters: dict[str, str | list[str]],
        start_period: str | None = None,
        end_period: str | None = None,
    ) -> pd.DataFrame:
        """Retrieve data with dimension-based filters.

        This is a convenience method that builds the data key from a dict.

        Args:
            dataflow_id: The dataflow ID.
            dimension_filters: Dict mapping dimension IDs to values.
                Values can be single codes or lists (OR'd together).
                Use None or empty string for wildcards.
            start_period: Optional start period.
            end_period: Optional end period.

        Returns:
            DataFrame of the requested data.
        """
        # First, get the structure to know dimension order
        structure = self.get_dataflow_structure(dataflow_id, include_codelists=False)
        dimensions = self.parse_dimensions(structure)

        # Build data key in dimension order
        key_parts = []
        for _, dim in dimensions.iterrows():
            dim_id = dim["id"]
            if dim_id == "TIME_PERIOD":
                continue  # Time is handled separately

            filter_val = dimension_filters.get(dim_id, "")
            if isinstance(filter_val, list):
                key_parts.append("+".join(str(v) for v in filter_val))
            elif filter_val:
                key_parts.append(str(filter_val))
            else:
                key_parts.append("")  # Wildcard

        data_key = ".".join(key_parts)

        return self.get_data(
            dataflow_id=dataflow_id,
            data_key=data_key,
            start_period=start_period,
            end_period=end_period,
        )
