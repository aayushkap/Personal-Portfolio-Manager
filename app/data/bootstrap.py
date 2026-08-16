"""Create and configure the local SQLite database before services start."""

from app.data.db import DB


def main() -> None:
    DB.bootstrap()
    print("SQLite database is ready (WAL enabled).")


if __name__ == "__main__":
    main()
