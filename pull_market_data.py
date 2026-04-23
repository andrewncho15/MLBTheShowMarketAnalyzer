#python3 -u pull_market_data.py
#python3 -m streamlit run app.py

import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from db import get_connection

load_dotenv()

BASE_URL = os.getenv("SHOW_API_BASE_URL")
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 0.10

if not BASE_URL:
    raise RuntimeError(
        "SHOW_API_BASE_URL is not set. Add SHOW_API_BASE_URL=https://mlb26.theshow.com/apis to your .env file."
    )


def pick_first(data, *keys, default=None):
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return default


def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ["listings", "items", "results", "data"]:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def extract_total_pages(payload):
    if not isinstance(payload, dict):
        return None
    for key in ["total_pages", "totalPages", "pages", "page_count", "pageCount"]:
        value = payload.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def fetch_all_listings():
    all_records = []
    seen_ids = set()
    page = 1
    total_pages = None

    while True:
        response = requests.get(
            f"{BASE_URL}/listings.json",
            params={"page": page},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        payload = response.json()
        page_records = extract_records(payload)

        if total_pages is None:
            total_pages = extract_total_pages(payload)

        if not page_records:
            print(f"listings.json: page {page} -> 0 records", flush=True)
            break

        new_records = 0
        for record in page_records:
            record_id = (
                record.get("uuid")
                or record.get("item_uuid")
                or json.dumps(record, sort_keys=True)
            )
            if record_id not in seen_ids:
                seen_ids.add(record_id)
                all_records.append(record)
                new_records += 1

        print(
            f"listings.json: page {page} -> {len(page_records)} records ({new_records} new)",
            flush=True,
        )

        if new_records == 0:
            break

        if total_pages is not None and page >= total_pages:
            break

        page += 1
        time.sleep(SLEEP_SECONDS)

    return all_records


def normalize_listing(listing):
    item = listing.get("item", {}) if isinstance(listing.get("item"), dict) else {}

    uuid = pick_first(listing, "uuid", "item_uuid", default=item.get("uuid"))
    name = pick_first(listing, "listing_name", "name", default=item.get("name"))
    best_buy_price = int(pick_first(listing, "best_buy_price", "buy_price", default=0) or 0)
    best_sell_price = int(pick_first(listing, "best_sell_price", "sell_price", default=0) or 0)

    return {
        "uuid": uuid,
        "listing_name": name,
        "best_buy_price": best_buy_price,
        "best_sell_price": best_sell_price,
        "listing_payload": json.dumps(listing),
        "item_payload": json.dumps(item) if item else None,
        "item_name": pick_first(item, "name", default=name),
        "img": pick_first(item, "img", "image", "image_url"),
        "item_type": pick_first(item, "item_type", "type"),
        "rarity": pick_first(item, "rarity"),
        "series": pick_first(item, "series"),
        "team": pick_first(item, "team", "team_name"),
        "player_name": pick_first(item, "player_name", "name", default=name),
        "overall": pick_first(item, "ovr", "overall"),
        "bats": pick_first(item, "bats"),
        "throws": pick_first(item, "throws"),
    }


def main():
    print("Fetching market listings...", flush=True)
    listings = fetch_all_listings()
    print(f"Fetched {len(listings)} market listings", flush=True)

    print("Normalizing listings...", flush=True)
    pulled_at = datetime.now(timezone.utc)

    raw_listings = []
    listing_rows = []
    history_rows = []
    item_rows = []
    seen_item_ids = set()

    for listing in listings:
        row = normalize_listing(listing)
        if not row["uuid"]:
            continue

        raw_listings.append((pulled_at, row["uuid"], row["listing_payload"]))

        listing_rows.append(
            (
                row["uuid"],
                row["listing_name"],
                row["best_buy_price"],
                row["best_sell_price"],
                row["listing_payload"],
            )
        )

        history_rows.append(
            (
                row["uuid"],
                pulled_at,
                row["best_buy_price"],
                row["best_sell_price"],
            )
        )

        if row["uuid"] not in seen_item_ids:
            seen_item_ids.add(row["uuid"])
            item_rows.append(
                (
                    row["uuid"],
                    row["item_name"],
                    row["img"],
                    row["item_type"],
                    row["rarity"],
                    row["series"],
                    row["team"],
                    row["player_name"],
                    row["overall"],
                    row["bats"],
                    row["throws"],
                    row["item_payload"] or row["listing_payload"],
                )
            )

    print("Writing to Postgres...", flush=True)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into raw.api_pull_log (
                    endpoint, request_url, pulled_at, status_code, payload
                )
                values (%s, %s, now(), %s, %s::jsonb)
                """,
                (
                    "listings.json",
                    f"{BASE_URL}/listings.json",
                    200,
                    json.dumps({"record_count": len(listings)}),
                ),
            )

            print("Inserting raw.listings_json...", flush=True)
            cur.executemany(
                """
                insert into raw.listings_json (pulled_at, uuid, payload)
                values (%s, %s, %s::jsonb)
                """,
                raw_listings,
            )

            print("Upserting staging.items...", flush=True)
            cur.executemany(
                """
                insert into staging.items (
                    uuid, name, img, item_type, rarity, series, team,
                    player_name, overall, bats, throws, raw_payload, updated_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                on conflict (uuid) do update
                set
                    name = coalesce(excluded.name, staging.items.name),
                    img = coalesce(excluded.img, staging.items.img),
                    item_type = coalesce(excluded.item_type, staging.items.item_type),
                    rarity = coalesce(excluded.rarity, staging.items.rarity),
                    series = coalesce(excluded.series, staging.items.series),
                    team = coalesce(excluded.team, staging.items.team),
                    player_name = coalesce(excluded.player_name, staging.items.player_name),
                    overall = coalesce(excluded.overall, staging.items.overall),
                    bats = coalesce(excluded.bats, staging.items.bats),
                    throws = coalesce(excluded.throws, staging.items.throws),
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                """,
                item_rows,
            )

            print("Upserting staging.listings_current...", flush=True)
            cur.executemany(
                """
                insert into staging.listings_current (
                    uuid, listing_name, best_buy_price, best_sell_price, raw_payload, updated_at
                )
                values (%s, %s, %s, %s, %s::jsonb, now())
                on conflict (uuid) do update
                set
                    listing_name = excluded.listing_name,
                    best_buy_price = excluded.best_buy_price,
                    best_sell_price = excluded.best_sell_price,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                """,
                listing_rows,
            )

            print("Appending staging.listing_price_history...", flush=True)
            cur.executemany(
                """
                insert into staging.listing_price_history (
                    uuid, pulled_at, best_buy_price, best_sell_price
                )
                values (%s, %s, %s, %s)
                """,
                history_rows,
            )

            print("Refreshing analytics.market_snapshot...", flush=True)
            cur.execute(
                """
                insert into analytics.market_snapshot (
                    uuid, item_name, team, overall,
                    best_buy_price, best_sell_price, spread, margin_pct, snapshot_at
                )
                select
                    i.uuid,
                    coalesce(i.player_name, i.name) as item_name,
                    i.team,
                    i.overall,
                    l.best_buy_price,
                    l.best_sell_price,
                    l.best_sell_price - l.best_buy_price as spread,
                    case
                        when l.best_buy_price > 0
                            then (l.best_sell_price - l.best_buy_price)::numeric / l.best_buy_price
                        else null
                    end as margin_pct,
                    now()
                from staging.items i
                join staging.listings_current l using (uuid)
                on conflict (uuid) do update
                set
                    item_name = excluded.item_name,
                    team = excluded.team,
                    overall = excluded.overall,
                    best_buy_price = excluded.best_buy_price,
                    best_sell_price = excluded.best_sell_price,
                    spread = excluded.spread,
                    margin_pct = excluded.margin_pct,
                    snapshot_at = now()
                """
            )

        conn.commit()

    print("Done.", flush=True)


if __name__ == "__main__":
    main()