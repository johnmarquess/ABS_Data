"""Bulk data extraction and Parquet storage utilities."""

from pathlib import Path
from typing import Callable
import pandas as pd

from src.api_client import ABSClient
from src.config import Settings


class DataExtractor:
    """Extract and store ABS data in Parquet format."""

    def __init__(
        self, client: ABSClient | None = None, settings: Settings | None = None
    ):
        """Initialize the data extractor.

        Args:
            client: Optional ABSClient instance.
            settings: Optional Settings instance.
        """
        self.settings = settings or Settings()
        self.client = client or ABSClient(self.settings)
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Create data directories if they don't exist."""
        self.settings.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.settings.processed_data_dir.mkdir(parents=True, exist_ok=True)

    def extract_dataflow(
        self,
        dataflow_id: str,
        data_key: str = "all",
        start_period: str | None = None,
        end_period: str | None = None,
        output_name: str | None = None,
        save_raw: bool = True,
    ) -> pd.DataFrame:
        """Extract a full dataflow and optionally save to Parquet.

        Args:
            dataflow_id: The dataflow to extract.
            data_key: Data key filter.
            start_period: Optional start period.
            end_period: Optional end period.
            output_name: Name for the output file (without extension).
            save_raw: Whether to save raw data to Parquet.

        Returns:
            DataFrame of extracted data.
        """
        print(f"Extracting dataflow: {dataflow_id}")

        df = self.client.get_data(
            dataflow_id=dataflow_id,
            data_key=data_key,
            start_period=start_period,
            end_period=end_period,
        )

        if save_raw and output_name:
            output_path = self.settings.raw_data_dir / f"{output_name}.parquet"
            df.to_parquet(output_path, index=False)
            print(f"Saved to: {output_path}")

        return df

    def extract_multiple_regions(
        self,
        dataflow_id: str,
        region_codes: list[str],
        region_dimension: str,
        other_filters: dict[str, str] | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        output_name: str | None = None,
    ) -> pd.DataFrame:
        """Extract data for multiple geographic regions.

        Uses the OR operator to request multiple regions in one call.

        Args:
            dataflow_id: The dataflow to extract.
            region_codes: List of region codes (e.g., SA2 codes).
            region_dimension: The dimension ID for regions.
            other_filters: Other dimension filters to apply.
            start_period: Optional start period.
            end_period: Optional end period.
            output_name: Name for the output file (without extension).

        Returns:
            DataFrame with combined data for all regions.
        """
        filters = other_filters or {}
        filters[region_dimension] = region_codes

        df = self.client.get_data_with_filters(
            dataflow_id=dataflow_id,
            dimension_filters=filters,
            start_period=start_period,
            end_period=end_period,
        )

        if output_name:
            output_path = self.settings.raw_data_dir / f"{output_name}.parquet"
            df.to_parquet(output_path, index=False)
            print(f"Saved {len(df)} rows to: {output_path}")

        return df

    def extract_in_batches(
        self,
        dataflow_id: str,
        codes: list[str],
        dimension_id: str,
        batch_size: int = 50,
        other_filters: dict[str, str] | None = None,
        start_period: str | None = None,
        end_period: str | None = None,
        output_name: str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> pd.DataFrame:
        """Extract data in batches to avoid API limits.

        Useful when requesting many region codes that might exceed URL limits.

        Args:
            dataflow_id: The dataflow to extract.
            codes: List of codes to batch.
            dimension_id: The dimension these codes belong to.
            batch_size: Number of codes per batch.
            other_filters: Other dimension filters.
            start_period: Optional start period.
            end_period: Optional end period.
            output_name: Name for the output file.
            progress_callback: Optional callback(batch_num, total_batches).

        Returns:
            Combined DataFrame from all batches.
        """
        all_data = []
        total_batches = (len(codes) + batch_size - 1) // batch_size

        for i in range(0, len(codes), batch_size):
            batch_num = i // batch_size + 1
            batch_codes = codes[i : i + batch_size]

            if progress_callback:
                progress_callback(batch_num, total_batches)
            else:
                print(f"Processing batch {batch_num}/{total_batches}")

            df = self.extract_multiple_regions(
                dataflow_id=dataflow_id,
                region_codes=batch_codes,
                region_dimension=dimension_id,
                other_filters=other_filters,
                start_period=start_period,
                end_period=end_period,
            )
            all_data.append(df)

        combined = pd.concat(all_data, ignore_index=True)

        if output_name:
            output_path = self.settings.raw_data_dir / f"{output_name}.parquet"
            combined.to_parquet(output_path, index=False)
            print(f"Saved {len(combined)} total rows to: {output_path}")

        return combined

    def load_parquet(self, name: str, from_processed: bool = False) -> pd.DataFrame:
        """Load a previously saved Parquet file.

        Args:
            name: The file name (without extension).
            from_processed: If True, load from processed dir instead of raw.

        Returns:
            DataFrame loaded from Parquet.
        """
        base_dir = (
            self.settings.processed_data_dir
            if from_processed
            else self.settings.raw_data_dir
        )
        path = base_dir / f"{name}.parquet"
        return pd.read_parquet(path)

    def save_processed(self, df: pd.DataFrame, name: str) -> Path:
        """Save a processed DataFrame to the processed directory.

        Args:
            df: DataFrame to save.
            name: Output file name (without extension).

        Returns:
            Path to the saved file.
        """
        output_path = self.settings.processed_data_dir / f"{name}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"Saved to: {output_path}")
        return output_path

    def list_saved_data(self) -> dict[str, list[str]]:
        """List all saved Parquet files.

        Returns:
            Dict with 'raw' and 'processed' lists of file names.
        """
        return {
            "raw": [f.stem for f in self.settings.raw_data_dir.glob("*.parquet")],
            "processed": [
                f.stem for f in self.settings.processed_data_dir.glob("*.parquet")
            ],
        }
