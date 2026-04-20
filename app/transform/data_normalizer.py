"""
Data normalization module (Normalization Layer).

Validates and normalizes transformed DataFrames before loading into database.
Applies business rules and data quality constraints.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for normalization validation results.

    Attributes:
        is_valid: Whether the data passed all validation rules.
        errors: List of validation error messages.
        warnings: List of non-critical warnings.
        rows_removed: Number of rows removed during normalization.
        original_count: Original row count before normalization.
        final_count: Row count after normalization.
    """

    def __init__(self):
        self.is_valid: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.rows_removed: int = 0
        self.original_count: int = 0
        self.final_count: int = 0

    def add_error(self, message: str):
        """Add validation error."""
        self.errors.append(message)
        self.is_valid = False
        logger.error(f"Validation error: {message}")

    def add_warning(self, message: str):
        """Add validation warning."""
        self.warnings.append(message)
        logger.warning(f"Validation warning: {message}")

    def to_dict(self) -> Dict:
        """Convert to dictionary for logging/reporting."""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "rows_removed": self.rows_removed,
            "original_count": self.original_count,
            "final_count": self.final_count,
        }


class IDataNormalizer(ABC):
    """Abstract base class for data normalization.

    Defines the contract for all normalizer implementations.
    Ensures consistent data quality processing across different data sources.

    Design rationale:
        - Separates validation logic from transformation logic
        - Enables different validation strategies per data source
        - Provides clear interface for business rule enforcement
    """

    @abstractmethod
    def normalize(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, ValidationResult]:
        """Normalize and validate DataFrame.

        Args:
            df: Raw DataFrame from transformation layer.

        Returns:
            Tuple of (normalized_df, validation_result).
            normalized_df contains only valid rows.
            validation_result contains errors, warnings, and statistics.

        Raises:
            ValueError: If DataFrame structure is invalid.
        """
        pass


class ExcelDataNormalizer(IDataNormalizer):
    """Normalizer for Excel-sourced data with Fecha, Rebanadas, and id columns.

    Business Rules:
        - Fecha: Valid datetime, not null, not in future, no duplicates
        - Rebanadas: Positive integer, not null, reasonable range
        - id: Unique integer identifier (auto-generated if missing)

    Validation Strategy:
        - Remove rows with null critical values
        - Remove rows with invalid data types
        - Remove duplicates by Fecha (keep first occurrence)
        - Log all removed rows for audit trail

    Attributes:
        max_rebanadas: Maximum acceptable value for Rebanadas (default 10000)
        allow_future_dates: Whether to accept future dates (default False)
    """

    def __init__(self, max_rebanadas: int = 10000, allow_future_dates: bool = False):
        """Initialize normalizer with validation parameters.

        Args:
            max_rebanadas: Maximum acceptable value for Rebanadas column.
            allow_future_dates: Whether to allow dates in the future.
        """
        self.max_rebanadas = max_rebanadas
        self.allow_future_dates = allow_future_dates
        logger.info(
            f"Initialized ExcelDataNormalizer: max_rebanadas={max_rebanadas}, "
            f"allow_future_dates={allow_future_dates}"
        )

    def normalize(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, ValidationResult]:
        """Normalize Excel data following business rules.

        Process:
            1. Validate structure (required columns present)
            2. Add id column if missing
            3. Validate and clean Fecha column
            4. Validate and clean Rebanadas column
            5. Remove duplicates by date
            6. Sort by date ascending
            7. Reset index

        Args:
            df: DataFrame with at least Fecha and Rebanadas columns.

        Returns:
            Tuple of (normalized DataFrame, validation result).

        Raises:
            ValueError: If required columns are missing.
        """
        result = ValidationResult()
        result.original_count = len(df)

        logger.info(f"Starting normalization for {result.original_count} rows")

        # Create working copy to avoid modifying original
        df_clean = df.copy()

        try:
            # Step 1: Validate structure
            self._validate_structure(df_clean)

            # Step 2: Add or validate id column
            df_clean = self._process_id_column(df_clean, result)

            # Step 3: Validate and clean Fecha
            df_clean = self._validate_fecha(df_clean, result)

            # Step 4: Validate and clean Rebanadas
            df_clean = self._validate_rebanadas(df_clean, result)

            # Step 5: Remove duplicates by Fecha (keep first)
            df_clean = self._remove_duplicates(df_clean, result)

            # Step 6: Sort by Fecha ascending
            df_clean = df_clean.sort_values(by="Fecha", ascending=True)

            # Step 7: Reset index and ensure id is sequential
            df_clean = df_clean.reset_index(drop=True)
            df_clean["id"] = range(1, len(df_clean) + 1)

            # Final statistics
            result.final_count = len(df_clean)
            result.rows_removed = result.original_count - result.final_count

            if result.rows_removed > 0:
                result.add_warning(
                    f"Removed {result.rows_removed} invalid rows "
                    f"({result.rows_removed / result.original_count * 100:.2f}%)"
                )

            logger.info(
                f"Normalization complete: {result.final_count} valid rows, "
                f"{result.rows_removed} removed"
            )

            return df_clean, result

        except Exception as e:
            result.add_error(f"Normalization failed: {str(e)}")
            logger.exception("Normalization failed")
            return pd.DataFrame(), result

    def _validate_structure(self, df: pd.DataFrame) -> None:
        """Validate DataFrame has required columns.

        Args:
            df: DataFrame to validate.

        Raises:
            ValueError: If required columns are missing.
        """
        required_columns = {"Fecha", "Rebanadas"}
        actual_columns = set(df.columns)
        missing = required_columns - actual_columns

        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        logger.debug(f"Structure validation passed. Columns: {list(df.columns)}")

    def _process_id_column(
        self, df: pd.DataFrame, result: ValidationResult
    ) -> pd.DataFrame:
        """Add or validate id column.

        If id column exists, validate it's numeric and unique.
        If missing, generate sequential IDs starting from 1.

        Args:
            df: DataFrame to process.
            result: ValidationResult to update.

        Returns:
            DataFrame with valid id column.
        """
        if "id" not in df.columns:
            # Generate sequential IDs
            df["id"] = range(1, len(df) + 1)
            result.add_warning("Column 'id' was missing - generated sequential IDs")
            logger.info("Generated sequential id column")
        else:
            # Validate existing id column
            if not pd.api.types.is_numeric_dtype(df["id"]):
                result.add_warning(
                    "Column 'id' was non-numeric - regenerated sequential IDs"
                )
                df["id"] = range(1, len(df) + 1)
            elif df["id"].duplicated().any():
                result.add_warning(
                    "Column 'id' had duplicates - regenerated sequential IDs"
                )
                df["id"] = range(1, len(df) + 1)

        # Ensure id is integer type
        df["id"] = df["id"].astype(int)
        return df

    def _validate_fecha(
        self, df: pd.DataFrame, result: ValidationResult
    ) -> pd.DataFrame:
        """Validate and clean Fecha column.

        Rules:
            - Must be valid datetime
            - Cannot be null
            - Cannot be in future (if allow_future_dates=False)

        Args:
            df: DataFrame to validate.
            result: ValidationResult to update.

        Returns:
            DataFrame with only valid Fecha values.
        """
        initial_count = len(df)

        # Remove null dates
        null_count = df["Fecha"].isnull().sum()
        if null_count > 0:
            df = df[df["Fecha"].notnull()]
            result.add_warning(f"Removed {null_count} rows with null Fecha")

        # Ensure datetime type
        if not pd.api.types.is_datetime64_any_dtype(df["Fecha"]):
            try:
                df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
                invalid_dates = df["Fecha"].isnull().sum()
                if invalid_dates > 0:
                    df = df[df["Fecha"].notnull()]
                    result.add_warning(
                        f"Removed {invalid_dates} rows with invalid Fecha format"
                    )
            except Exception as e:
                result.add_error(f"Failed to convert Fecha to datetime: {e}")
                return df

        # Check for future dates
        if not self.allow_future_dates:
            today = pd.Timestamp.now().normalize()
            future_dates = df["Fecha"] > today
            future_count = future_dates.sum()
            if future_count > 0:
                df = df[~future_dates]
                result.add_warning(f"Removed {future_count} rows with future dates")

        logger.debug(f"Fecha validation: {initial_count - len(df)} rows removed")
        return df

    def _validate_rebanadas(
        self, df: pd.DataFrame, result: ValidationResult
    ) -> pd.DataFrame:
        """Validate and clean Rebanadas column.

        Rules:
            - Must be numeric
            - Cannot be null
            - Must be positive
            - Must be <= max_rebanadas

        Args:
            df: DataFrame to validate.
            result: ValidationResult to update.

        Returns:
            DataFrame with only valid Rebanadas values.
        """
        initial_count = len(df)

        # Remove null values
        null_count = df["Rebanadas"].isnull().sum()
        if null_count > 0:
            df = df[df["Rebanadas"].notnull()]
            result.add_warning(f"Removed {null_count} rows with null Rebanadas")

        # Ensure numeric type
        if not pd.api.types.is_numeric_dtype(df["Rebanadas"]):
            try:
                df["Rebanadas"] = pd.to_numeric(df["Rebanadas"], errors="coerce")
                invalid_numbers = df["Rebanadas"].isnull().sum()
                if invalid_numbers > 0:
                    df = df[df["Rebanadas"].notnull()]
                    result.add_warning(
                        f"Removed {invalid_numbers} rows with non-numeric Rebanadas"
                    )
            except Exception as e:
                result.add_error(f"Failed to convert Rebanadas to numeric: {e}")
                return df

        # Remove negative or zero values
        invalid_values = df["Rebanadas"] <= 0
        invalid_count = invalid_values.sum()
        if invalid_count > 0:
            df = df[~invalid_values]
            result.add_warning(f"Removed {invalid_count} rows with Rebanadas <= 0")

        # Remove values exceeding maximum
        excessive_values = df["Rebanadas"] > self.max_rebanadas
        excessive_count = excessive_values.sum()
        if excessive_count > 0:
            df = df[~excessive_values]
            result.add_warning(
                f"Removed {excessive_count} rows with Rebanadas > {self.max_rebanadas}"
            )

        # Convert to integer
        df["Rebanadas"] = df["Rebanadas"].astype(int)

        logger.debug(f"Rebanadas validation: {initial_count - len(df)} rows removed")
        return df

    def _remove_duplicates(
        self, df: pd.DataFrame, result: ValidationResult
    ) -> pd.DataFrame:
        """Remove duplicate dates, keeping first occurrence.

        Args:
            df: DataFrame to deduplicate.
            result: ValidationResult to update.

        Returns:
            DataFrame without duplicate Fecha values.
        """
        initial_count = len(df)
        duplicate_count = df.duplicated(subset=["Fecha"], keep="first").sum()

        if duplicate_count > 0:
            df = df.drop_duplicates(subset=["Fecha"], keep="first")
            result.add_warning(
                f"Removed {duplicate_count} duplicate dates (kept first occurrence)"
            )
            logger.debug(f"Removed {duplicate_count} duplicate dates")

        return df
