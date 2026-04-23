from db import get_connection


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select table_schema, table_name
                from information_schema.tables
                where table_schema in ('raw', 'staging', 'analytics')
                order by table_schema, table_name
                """
            )
            tables = cur.fetchall()

            print("Tables found:")
            if not tables:
                print("  No raw/staging/analytics tables found yet.")
            else:
                for schema_name, table_name in tables:
                    print(f"  {schema_name}.{table_name}")

            print("\nRow counts:")
            for full_table_name in [
                "raw.api_pull_log",
                "raw.listings_json",
                "raw.items_json",
                "staging.items",
                "staging.listings_current",
                "staging.listing_price_history",
                "analytics.market_snapshot",
            ]:
                cur.execute(f"select count(*) from {full_table_name}")
                row_count = cur.fetchone()[0]
                print(f"  {full_table_name}: {row_count}")


if __name__ == "__main__":
    main()