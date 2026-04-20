"""
Repository interface (abstract base class).

Defines the contract that all repository implementations must follow.
This enforces consistency across different database engines and promotes
dependency inversion principle (SOLID).
"""

from abc import ABC, abstractmethod
from typing import Any, Optional


class IRepository(ABC):
    """Abstract base class for all repository implementations.

    All concrete repositories (PostgreSQL, MySQL, etc.) must implement
    this interface to ensure a consistent API for data access operations.

    Design rationale:
        - Decouples business logic from specific database implementations
        - Enables testing with mock repositories
        - Allows swapping database engines without changing consumer code
        - Enforces consistent error handling across implementations
    """

    @abstractmethod
    def execute_query(self, query: str, params: Optional[dict] = None) -> list[Any]:
        """Execute a SQL query and return results.

        Args:
            query: SQL query string (use parameterized queries).
            params: Optional dictionary of query parameters.

        Returns:
            List of result rows (tuples or Row objects).

        Raises:
            QueryExecutionError: If query execution fails.

        Note:
            Implementations must never propagate raw database exceptions.
            All errors should be wrapped in domain-specific exceptions.
        """
        pass
