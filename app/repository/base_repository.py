"""
Base repository implementation for database access.

Provides concrete implementation of IRepository interface using SQLAlchemy.
This serves as the foundation for all database-specific repositories.
"""

import logging
from typing import Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from app.repository.i_repository import IRepository
from app.error.exceptions import QueryExecutionError

logger = logging.getLogger(__name__)


class BaseRepository(IRepository):
    """Concrete implementation of IRepository using SQLAlchemy.

    Provides safe query execution with connection management and
    error handling. All database-specific repositories should extend
    this class to inherit standard query execution behavior.

    Attributes:
        engine: SQLAlchemy Engine instance for database connections.

    Design notes:
        - Uses connection context manager for automatic cleanup
        - Wraps all database errors in QueryExecutionError
        - Supports parameterized queries to prevent SQL injection
    """

    def __init__(self, engine: Engine):
        """Initialize repository with a SQLAlchemy engine.

        Args:
            engine: SQLAlchemy Engine instance configured for target database.
        """
        self.engine = engine

    def execute_query(self, query: str, params: Optional[dict] = None) -> list[Any]:
        """Execute a SQL query and return results.

        Args:
            query: SQL query string (parameterized with :param_name syntax).
            params: Optional dictionary of query parameters.

        Returns:
            List of result rows as tuples.

        Raises:
            QueryExecutionError: If query execution fails for any reason.

        Example:
            >>> repo = BaseRepository(engine)
            >>> result = repo.execute_query(
            ...     "SELECT * FROM users WHERE id = :user_id",
            ...     {"user_id": 123}
            ... )
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return result.fetchall()
        except Exception as e:
            logger.error("Query failed — %s", e)
            raise QueryExecutionError(query, e)


# from app.config.db_config import configPostgre

# engine = configPostgre()
# repo = BaseRepository(engine)

# print("\n" + "=" * 60)
# print("BaseRepository - Quick Test")
# print("=" * 60 + "\n")

# # 2. Test your queries here
# # -------------------------

# # Example 1: Simple query
# result = repo.execute_query("SELECT current_database(), current_user")
# print(f"Database: {result[0][0]}")
# print(f"User: {result[0][1]}\n")
