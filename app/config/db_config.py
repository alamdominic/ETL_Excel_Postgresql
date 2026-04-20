"""
Database engine factory for PostgreSQL.

Creates and returns SQLAlchemy engine using environment variables.
"""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def configTargetTable():
    """
    Get target table schema and name from environment variables.

    Required ENV variables:
        SAD_SCHEMA - Database schema name
        SAD_TABLE - Table name

    Returns:
        str: Formatted table identifier as '"schema"."table"'

    Raises:
        ValueError: If required environment variables are missing.
    """
    load_dotenv()

    schema = os.getenv("SAD_SCHEMA")
    table = os.getenv("SAD_TABLE")

    if not all([schema, table]):
        raise ValueError(
            "Missing target table environment variables (SAD_SCHEMA, SAD_TABLE)"
        )

    # Format with quotes around schema and table for PostgreSQL
    return f'"{schema}"."{table}"'


def configPostgre():
    """
    Create PostgreSQL SQLAlchemy engine.

    Required ENV variables:
        DB_HOST
        DB_PORT
        DB_DB
        DB_USER
        DB_PASSWORD
    """
    load_dotenv()

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([host, port, db, user, password]):
        raise ValueError("Missing PostgreSQL environment variables")

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

    return engine


# engine = configPostgre()
# if engine:
#     print("PostgreSQL engine created successfully.")
