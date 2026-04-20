"""Repository package for database access.

This package provides data access layer with defensive error handling:
    - BaseRepository: Core query execution functionality
    - TableRepository: Table and column metadata queries
    - MySQLRepository: MySQL-specific operations
    - PostgreSQLRepository: PostgreSQL-specific operations

All repositories return safe defaults on errors instead of propagating exceptions.
"""
