"""Repository package for database access.

This package provides data access layer with defensive error handling:
    - IRepository: Abstract interface defining repository contract
    - BaseRepository: Core query execution functionality
    - PostgreSQLRepository: PostgreSQL-specific operations
    - ETLRepository: ETL-specific batch operations

All repositories return safe defaults on errors instead of propagating exceptions.
All concrete repositories implement the IRepository interface.
"""

from app.repository.i_repository import IRepository
from app.repository.base_repository import BaseRepository
from app.repository.postgresql_repository import PostgreSQLRepository
from app.load.etl_repository import ETLRepository

__all__ = [
    "IRepository",
    "BaseRepository",
    "PostgreSQLRepository",
    "ETLRepository",
]
