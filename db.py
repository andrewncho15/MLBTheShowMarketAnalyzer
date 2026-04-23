import os

from dotenv import load_dotenv
import psycopg

load_dotenv()


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Create a .env file and add your Neon connection string."
        )
    return database_url


def get_connection():
    return psycopg.connect(get_database_url())