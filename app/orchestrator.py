"""
Excel ETL Pipeline Orchestrator.

Complete pipeline flow:
1. Read Excel file (Ingestion Layer)
2. Convert to DataFrame (Transformation Layer)
3. Normalize and validate data (Normalization Layer)
4. Load into PostgreSQL (Repository Layer)
5. Send email notification with execution status

This module orchestrates the entire ETL process with comprehensive
logging and error handling at each stage.
"""

import logging
import time
from datetime import datetime

from app.config.db_config import configPostgre, configTargetTable
from app.config.excel_config import configExcel
from app.config.logging_config import setup_logging
from app.repository.postgresql_repository import PostgreSQLRepository
from app.extract.excel_extractor import ExcelReader
from app.transform.excel_to_dataframe_converter import ExcelToDataFrameConverter
from app.transform.data_normalizer import ExcelDataNormalizer
from app.utils.email_notifier import (
    send_success_notification,
    send_failure_notification,
)

# Initialize logging
setup_logging(log_level="INFO", enable_file_logging=True)
logger = logging.getLogger(__name__)


def main():
    """Execute full ETL pipeline: Extract → Transform → Normalize → Load → Notify.

    Returns:
        bool: True if pipeline executed successfully, False otherwise.
    """
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("ETL PIPELINE EXECUTION STARTED")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # ===== STAGE 1: CONFIGURATION =====
        logger.info("STAGE 1: Loading configuration...")
        try:
            excel_route, excel_sheet = configExcel()
            db_engine = configPostgre()
            target_table = configTargetTable()
            logger.info(f"  ✓ Excel source: {excel_route} → Sheet: {excel_sheet}")
            logger.info(f"  ✓ Target table: {target_table}")
        except Exception as config_error:
            logger.error(f"Configuration failed: {config_error}")
            send_failure_notification(str(config_error), stage="Configuration")
            return False

        # ===== STAGE 2: EXTRACTION =====
        logger.info("STAGE 2: Extracting data from Excel...")
        try:
            reader = ExcelReader()
            worksheet = reader.read_sheet(excel_route, excel_sheet)
            logger.info(
                f"  ✓ Worksheet loaded: {worksheet.max_row} rows × {worksheet.max_column} columns"
            )
        except FileNotFoundError as file_error:
            logger.error(f"Excel file not found: {file_error}")
            send_failure_notification(str(file_error), stage="Extraction")
            return False
        except Exception as extract_error:
            logger.error(f"Extraction failed: {extract_error}")
            send_failure_notification(str(extract_error), stage="Extraction")
            return False

        # ===== STAGE 3: TRANSFORMATION =====
        logger.info("STAGE 3: Converting to DataFrame...")
        try:
            converter = ExcelToDataFrameConverter(has_header=True, skip_rows=0)
            df = converter.convert(worksheet)
            logger.info(
                f"  ✓ DataFrame created: {len(df)} rows × {len(df.columns)} columns"
            )
            logger.info(f"  ✓ Columns: {list(df.columns)}")
        except Exception as transform_error:
            logger.error(f"Transformation failed: {transform_error}")
            send_failure_notification(str(transform_error), stage="Transformation")
            return False

        # ===== STAGE 4: NORMALIZATION =====
        logger.info("STAGE 4: Normalizing and validating data...")
        try:
            # allow_future_dates=True because DB constraint requires fecha >= current_year
            normalizer = ExcelDataNormalizer(
                max_rebanadas=10000, allow_future_dates=True
            )
            df_normalized, validation_result = normalizer.normalize(df)

            logger.info(f"  ✓ Original rows: {validation_result.original_count}")
            logger.info(
                f"  ✓ Rows after normalization: {validation_result.final_count}"
            )
            logger.info(f"  ✓ Rows removed: {validation_result.rows_removed}")

            if validation_result.warnings:
                logger.warning("Validation warnings:")
                for warning in validation_result.warnings:
                    logger.warning(f"  - {warning}")

            if validation_result.errors:
                logger.error("Validation errors detected:")
                for error in validation_result.errors:
                    logger.error(f"  - {error}")
                error_summary = "\n".join(validation_result.errors)
                send_failure_notification(error_summary, stage="Normalization")
                return False

            logger.info("  ✓ Data validation passed")

        except Exception as norm_error:
            logger.error(f"Normalization failed: {norm_error}")
            send_failure_notification(str(norm_error), stage="Normalization")
            return False

        # ===== STAGE 5: SYNCHRONIZATION =====
        logger.info("STAGE 5: Checking synchronization status...")
        try:
            # Prepare database repository
            db_repo = PostgreSQLRepository(db_engine)

            # Get last id from database
            last_db_id = db_repo.get_last_id(target_table, id_column="id")

            # Get last id from Excel (normalized DataFrame)
            if "id" not in df_normalized.columns:
                logger.error("Column 'id' not found in DataFrame")
                send_failure_notification(
                    "Column 'id' not found in normalized data", stage="Synchronization"
                )
                return False

            last_excel_id = int(df_normalized["id"].max())

            logger.info(
                f"  ✓ Last id in database: {last_db_id if last_db_id else 'None (empty table)'}"
            )
            logger.info(f"  ✓ Last id in Excel: {last_excel_id}")

            # Determine synchronization action
            if last_db_id is None:
                # Database is empty - insert all records
                logger.info("  → Database is empty - inserting all records from Excel")
                load_df = df_normalized
                sync_action = "full_load"
            elif last_excel_id > last_db_id:
                # New records exist - insert only records with id > last_db_id
                new_records_count = len(df_normalized[df_normalized["id"] > last_db_id])
                logger.info(
                    f"  → New records detected: {new_records_count} records with id > {last_db_id}"
                )
                load_df = df_normalized[df_normalized["id"] > last_db_id]
                sync_action = "incremental_load"
            elif last_excel_id == last_db_id:
                # No new records - skip insertion
                logger.info("  ✓ Database is already synchronized (no new records)")
                logger.info("=" * 80)
                logger.info("ETL PIPELINE COMPLETED - NO CHANGES")
                logger.info(f"  → Last synchronized id: {last_db_id}")
                logger.info(
                    f"  → Execution time: {time.time() - start_time:.2f} seconds"
                )
                logger.info("=" * 80)

                # Send notification about sync status
                send_success_notification(
                    rows_inserted=0, execution_time=time.time() - start_time
                )
                return True
            else:
                # Excel has lower id than database - data integrity issue
                error_msg = f"Data integrity error: Excel last id ({last_excel_id}) is lower than database last id ({last_db_id})"
                logger.error(f"  ✗ {error_msg}")
                send_failure_notification(error_msg, stage="Synchronization")
                return False

            logger.info(f"  ✓ Synchronization mode: {sync_action}")
            logger.info(f"  ✓ Records to insert: {len(load_df)}")

        except Exception as sync_error:
            logger.error(f"Synchronization check failed: {sync_error}")
            send_failure_notification(str(sync_error), stage="Synchronization")
            return False

        # ===== STAGE 6: LOADING =====
        logger.info("STAGE 6: Loading data into PostgreSQL...")
        try:
            # Transform DataFrame to match database schema
            final_df = (
                load_df.loc[:, ["id", "Fecha", "Rebanadas"]]
                .rename(columns={"Fecha": "fecha", "Rebanadas": "rebanadas"})
                .assign(fecha=lambda frame: frame["fecha"].dt.date)
            )

            logger.info(f"  → Inserting {len(final_df)} rows into {target_table}...")
            inserted_rows = db_repo.bulk_insert(target_table, final_df)
            logger.info(f"  ✓ Successfully inserted {inserted_rows} rows")

        except Exception as db_error:
            logger.error(f"Database loading failed: {db_error}")
            if "chk_anio_vigente" in str(db_error):
                logger.error(
                    "  → Check constraint violation: fecha must have year >= current year"
                )
            send_failure_notification(str(db_error), stage="Loading")
            return False

        # ===== STAGE 7: SUCCESS NOTIFICATION =====
        execution_time = time.time() - start_time
        logger.info("=" * 80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  → Synchronization mode: {sync_action}")
        logger.info(f"  → Rows inserted: {inserted_rows}")
        logger.info(f"  → Execution time: {execution_time:.2f} seconds")
        logger.info("=" * 80)

        # Send success email
        send_success_notification(inserted_rows, execution_time)

        return True

    except Exception as e:
        execution_time = time.time() - start_time
        logger.error("=" * 80)
        logger.error("ETL PIPELINE FAILED")
        logger.error(f"  → Error: {type(e).__name__}: {e}")
        logger.error(f"  → Execution time: {execution_time:.2f} seconds")
        logger.error("=" * 80)
        send_failure_notification(f"{type(e).__name__}: {e}", stage="Unknown")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
