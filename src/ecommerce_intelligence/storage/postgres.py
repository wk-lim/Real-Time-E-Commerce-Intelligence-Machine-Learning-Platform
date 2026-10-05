from __future__ import annotations

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def connect_postgres() -> psycopg.Connection:
    # Existing process environment values take precedence over local .env.
    load_dotenv(PROJECT_ROOT / ".env", override=False)

    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is not set")

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_HOST_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ecommerce_intelligence"),
        user=os.getenv("POSTGRES_USER", "ecommerce_admin"),
        password=password,
        connect_timeout=5,
    )


def main() -> None:
    with connect_postgres() as connection:
        row = connection.execute("SELECT current_database(), current_user").fetchone()

        if row is None:
            raise RuntimeError("PostgreSQL connection check returned no row")

        print(f"Connected to {row[0]} as {row[1]}")

if __name__ == "__main__":
    main()