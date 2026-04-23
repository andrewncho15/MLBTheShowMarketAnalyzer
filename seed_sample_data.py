from datetime import datetime, timedelta, timezone

from db import get_connection


SAMPLE_CARDS = [
    {
        "uuid": "card-001",
        "name": "A.J. Cole",
        "team": "Yankees",
        "overall": 84,
        "prices": [
            (7, 900, 980),
            (6, 920, 1000),
            (5, 940, 1030),
            (4, 970, 1070),
            (3, 1010, 1110),
            (2, 1080, 1185),
            (1, 1160, 1280),
            (0, 1250, 1375),
        ],
    },
    {
        "uuid": "card-002",
        "name": "Corey Seager",
        "team": "Rangers",
        "overall": 91,
        "prices": [
            (7, 5200, 5600),
            (6, 5150, 5530),
            (5, 5050, 5480),
            (4, 4975, 5400),
            (3, 4900, 5325),
            (2, 4825, 5250),
            (1, 4750, 5180),
            (0, 4690, 5100),
        ],
    },
    {
        "uuid": "card-003",
        "name": "Elly De La Cruz",
        "team": "Reds",
        "overall": 93,
        "prices": [
            (7, 8100, 8600),
            (6, 8200, 8750),
            (5, 7900, 8450),
            (4, 8400, 9000),
            (3, 9000, 9650),
            (2, 9600, 10300),
            (1, 10400, 11150),
            (0, 11250, 12000),
        ],
    },
]


def main():
    now = datetime.now(timezone.utc)

    with get_connection() as conn:
        with conn.cursor() as cur:
            for card in SAMPLE_CARDS:
                raw_item_payload = {
                    "uuid": card["uuid"],
                    "name": card["name"],
                    "team": card["team"],
                    "overall": card["overall"],
                }

                cur.execute(
                    """
                    insert into staging.items (
                        uuid, name, team, player_name, overall, raw_payload, updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s::jsonb, now())
                    on conflict (uuid) do update
                    set
                        name = excluded.name,
                        team = excluded.team,
                        player_name = excluded.player_name,
                        overall = excluded.overall,
                        raw_payload = excluded.raw_payload,
                        updated_at = now()
                    """,
                    (
                        card["uuid"],
                        card["name"],
                        card["team"],
                        card["name"],
                        card["overall"],
                        str(raw_item_payload).replace("'", '"'),
                    ),
                )

                for days_ago, best_buy_price, best_sell_price in card["prices"]:
                    pulled_at = now - timedelta(days=days_ago)

                    raw_listing_payload = {
                        "uuid": card["uuid"],
                        "listing_name": card["name"],
                        "best_buy_price": best_buy_price,
                        "best_sell_price": best_sell_price,
                    }

                    cur.execute(
                        """
                        insert into staging.listing_price_history (
                            uuid, pulled_at, best_buy_price, best_sell_price
                        )
                        values (%s, %s, %s, %s)
                        """,
                        (
                            card["uuid"],
                            pulled_at,
                            best_buy_price,
                            best_sell_price,
                        ),
                    )

                    cur.execute(
                        """
                        insert into raw.listings_json (pulled_at, uuid, payload)
                        values (%s, %s, %s::jsonb)
                        """,
                        (
                            pulled_at,
                            card["uuid"],
                            str(raw_listing_payload).replace("'", '"'),
                        ),
                    )

                latest_buy = card["prices"][-1][1]
                latest_sell = card["prices"][-1][2]
                latest_listing_payload = {
                    "uuid": card["uuid"],
                    "listing_name": card["name"],
                    "best_buy_price": latest_buy,
                    "best_sell_price": latest_sell,
                }

                cur.execute(
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
                    (
                        card["uuid"],
                        card["name"],
                        latest_buy,
                        latest_sell,
                        str(latest_listing_payload).replace("'", '"'),
                    ),
                )

                cur.execute(
                    """
                    insert into raw.items_json (pulled_at, uuid, payload)
                    values (now(), %s, %s::jsonb)
                    """,
                    (
                        card["uuid"],
                        str(raw_item_payload).replace("'", '"'),
                    ),
                )

                cur.execute(
                    """
                    insert into raw.api_pull_log (
                        endpoint, request_url, pulled_at, status_code, payload
                    )
                    values (%s, %s, now(), %s, %s::jsonb)
                    """,
                    (
                        "sample_seed",
                        "local_sample_data",
                        200,
                        str({"uuid": card["uuid"], "name": card["name"]}).replace("'", '"'),
                    ),
                )

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

    print("Sample data loaded into raw, staging, and analytics tables.")


if __name__ == "__main__":
    main()