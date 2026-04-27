import os

from dotenv import load_dotenv
import psycopg

load_dotenv()


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    try:
        import streamlit as st

        if "DATABASE_URL" in st.secrets:
            return st.secrets["DATABASE_URL"]
    except Exception:
        pass

    raise RuntimeError(
        "DATABASE_URL is not set. Add it to your local .env or Streamlit secrets."
    )


def get_connection():
    return psycopg.connect(get_database_url())