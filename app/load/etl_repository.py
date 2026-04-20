"""
ETL-specific repository for batch operations.
"""

import logging
import pandas as pd

from app.repository.postgresql_repository import PostgreSQLRepository
from app.error.exceptions import QueryExecutionError

logger = logging.getLogger(__name__)


class ETLRepository(PostgreSQLRepository):

    def __init__(self, engine, table_name: str):
        super().__init__(engine)
        self.table_name = table_name

    def insert_batch(self, records: list[dict]) -> int:
        """
        Batch insert using executemany.
        """

        if not records:
            return 0

        df = pd.DataFrame(records)

        # NaN → None
        df = df.where(pd.notna(df), None)

        data = [tuple(row) for row in df.values]
        columns = df.columns.tolist()

        columns_str = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        query = f"""
            INSERT INTO {self.table_name} ({columns_str})
            VALUES ({placeholders})
        """

        try:
            with self.engine.begin() as conn:
                result = conn.exec_driver_sql(query, data)

            inserted = result.rowcount if result else len(data)
            logger.info("Inserted %s rows into %s", inserted, self.table_name)

            return inserted

        except Exception as e:
            logger.error("Insert batch failed — %s", e)
            raise QueryExecutionError(query, e)

    def upsert_batch(self, records: list[dict], conflict_column: str) -> int:
        """
        UPSERT using ON CONFLICT.
        """

        if not records:
            return 0

        df = pd.DataFrame(records)
        df = df.where(pd.notna(df), None)

        data = [tuple(row) for row in df.values]
        columns = df.columns.tolist()

        columns_str = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        update_set = ", ".join(
            [f'"{col}" = EXCLUDED."{col}"' for col in columns if col != conflict_column]
        )

        query = f"""
            INSERT INTO {self.table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ("{conflict_column}")
            DO UPDATE SET {update_set}
        """

        try:
            with self.engine.begin() as conn:
                result = conn.exec_driver_sql(query, data)

            return result.rowcount if result else len(data)

        except Exception as e:
            logger.error("Upsert failed — %s", e)
            raise QueryExecutionError(query, e)

    def exists_by_fecha(self, fecha) -> bool:
        query = f"""
            SELECT 1 FROM {self.table_name}
            WHERE fecha = :fecha
            LIMIT 1
        """

        result = self.execute_query(query, {"fecha": fecha})
        return len(result) > 0

    def get_max_fecha(self):
        query = f"""
            SELECT MAX(fecha) FROM {self.table_name}
        """

        result = self.execute_query(query)

        return result[0][0] if result and result[0][0] else None
