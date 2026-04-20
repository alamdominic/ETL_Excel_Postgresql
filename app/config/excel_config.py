"""
Database engine factory for PostgreSQL.

Creates and returns SQLAlchemy engine using environment variables.
"""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def configExcel():
    load_dotenv()

    route = os.getenv("SAD_EXCEL_PATH")
    sheet = os.getenv("SAD_SHEET")

    if not all([route, sheet]):
        raise ValueError("Missing excel environment variables")

    return route, sheet


# data = configExcel()
# print("Excel configuration loaded successfully:", data)
