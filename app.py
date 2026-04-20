"""Main entry point for Excel ETL application.

Delegates execution to app.orchestrator.main().
"""

from app.orchestrator import main


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
