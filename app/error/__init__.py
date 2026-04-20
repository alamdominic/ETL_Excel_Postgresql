"""Custom exceptions package.

This package defines the exception hierarchy for the ETL QA pipeline:
    - ExceptionBase: Base class for all custom exceptions
    - MissingEnvironmentVariableError: Missing .env variable
    - QueryExecutionError: Database query failure
    - ETLConfigurationError: Invalid ETL config structure
    - QueryNotFoundError: SQL file not found

All exceptions inherit from ExceptionBase for unified error handling.
"""
