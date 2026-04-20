"""PostgreSQL-specific repository implementation.

Provides PostgreSQL database operations by extending BaseRepository.
Currently inherits all functionality from base class without
PostgreSQL-specific customizations.

Consumed by:
    - ETLRepository
    - Future PostgreSQL-specific modules (if needed)

Consumes:
    - app.repository.base_repository.BaseRepository
"""

import logging
import pandas as pd
from psycopg2.extras import execute_values
from typing import Optional

from app.repository.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class PostgreSQLRepository(BaseRepository):
    """Repository for PostgreSQL-specific database operations.

    Inherits core query execution from BaseRepository.
    Reserved for future PostgreSQL-specific methods.

    Used by:
        PostgreSQL-specific validation or query modules.

    Example:
        >>> from config.data_base_engine_factory import DatabaseEngineFactory
        >>> factory = DatabaseEngineFactory()
        >>> postgres_repo = PostgreSQLRepository(factory.get_PostgreSQL())
    """

    def __init__(self, engine):
        """Initialize PostgreSQL repository with a PostgreSQL engine.

        Args:
            engine: SQLAlchemy Engine instance configured for PostgreSQL.
        """
        super().__init__(engine)

    def get_last_id(self, table_name: str, id_column: str = "id") -> Optional[int]:
        """Get the last (maximum) id value from a table.

        Args:
            table_name: Fully qualified table name (e.g., '"schema"."table"').
            id_column: Name of the id column (default: "id").

        Returns:
            The maximum id value in the table, or None if table is empty.

        Example:
            >>> repo = PostgreSQLRepository(engine)
            >>> last_id = repo.get_last_id('"sad"."tabla"', id_column="id")
            >>> print(last_id)  # 1500
        """
        query = f'SELECT MAX("{id_column}") FROM {table_name}'

        try:
            result = self.execute_query(query)
            if result and result[0][0] is not None:
                last_id = int(result[0][0])
                logger.info(f"Last id in {table_name}: {last_id}")
                return last_id
            else:
                logger.info(f"Table {table_name} is empty (no records found)")
                return None
        except Exception as e:
            logger.error(f"Failed to get last id from {table_name}: {e}")
            return None

    def bulk_insert(self, table_name: str, data: pd.DataFrame | list[dict]) -> int:
        """Insert many rows efficiently into a PostgreSQL table.

        Args:
            table_name: Fully qualified table name (e.g., '"schema"."table"').
            data: DataFrame or list of dictionaries containing data to insert.

        Returns:
            Number of rows successfully inserted.

        Raises:
            Exception: If insert operation fails (automatically rolls back).
        """
        if isinstance(data, pd.DataFrame):
            if data.empty:
                return 0
            df = data.copy()
        else:
            if not data:
                return 0
            df = pd.DataFrame(data)

        df = df.where(pd.notna(df), None)
        columns = df.columns.tolist()
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        columns_sql = ", ".join(f'"{column}"' for column in columns)
        query = f"INSERT INTO {table_name} ({columns_sql}) VALUES %s"

        raw_connection = self.engine.raw_connection()
        try:
            with raw_connection.cursor() as cursor:
                execute_values(cursor, query, rows, page_size=1000)
            raw_connection.commit()
            return len(rows)
        except Exception:
            raw_connection.rollback()
            raise
        finally:
            raw_connection.close()
