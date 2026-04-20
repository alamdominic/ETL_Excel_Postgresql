"""Custom exceptions for ETL Quality Assurance pipeline.

This module defines the exception hierarchy used throughout the application.
All custom exceptions inherit from ExceptionBase for unified error handling.

Consumed by:
    - config.sql_config (environment validation)
    - repository.base_repository (query execution)
    - validation modules (configuration validation)

Consumes:
    - None (base exception module)
"""


class ExceptionBase(Exception):
    """Base exception class for all custom application exceptions.

    Provides a common ancestor for all custom exceptions, enabling:
    - Unified exception handling (catch all via ExceptionBase)
    - Centralized error attributes and methods
    - Easy extension for new exception types

    Used by:
        All custom exceptions in this module inherit from this class.
    """

    pass


class MissingEnvironmentVariableError(ExceptionBase):
    """Raised when a required environment variable is not found in .env file.

    Attributes:
        variable_name (str): Name of the missing environment variable.

    Raised by:
        config.sql_config.SQLConfig._require()

    Example:
        >>> raise MissingEnvironmentVariableError("MYSQL_HOST")
    """

    def __init__(self, variable_name: str):
        """Initialize the exception with the missing variable name.

        Args:
            variable_name: Name of the environment variable that was expected but not found.
        """
        self.variable_name = variable_name
        super().__init__(f"Missing required environment variable: {variable_name}")


class QueryExecutionError(ExceptionBase):
    """Raised when a database query fails to execute.

    Attributes:
        query (str): The SQL query that failed.
        original_exception (Exception): The underlying database exception.

    Raised by:
        repository.base_repository.BaseRepository.execute_query()

    Example:
        >>> raise QueryExecutionError("SELECT * FROM table", ValueError("Connection lost"))
    """

    def __init__(self, query: str, original_exception: Exception):
        """Initialize the exception with query details and original error.

        Args:
            query: The SQL query string that failed.
            original_exception: The original exception raised by the database driver.
        """
        self.query = query
        self.original_exception = original_exception
        super().__init__(
            f"Failed to execute query: {str(query)}. Original error: {str(original_exception)}"
        )


class QueryNotFoundError(ExceptionBase):
    """Raised when an external SQL query file cannot be found.

    Attributes:
        query (str): Path to the SQL file that was not found.

    Raised by:
        repository.table_repository.TableRepository._load_sql()

    Example:
        >>> raise QueryNotFoundError("sql/repository/table_exists.sql")
    """

    def __init__(self, query_path_name: str):
        """Initialize the exception with the missing query file path.

        Args:
            query_path_name: Relative or absolute path to the SQL file that was expected.
        """
        self.query = query_path_name
        super().__init__(f"Query file not found: {query_path_name}")
