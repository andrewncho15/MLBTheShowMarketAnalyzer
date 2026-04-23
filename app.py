import pandas as pd
import streamlit as st

from db import get_connection


st.set_page_config(page_title="MLB The Show Market Dashboard", layout="wide")


def load_current_market():
    query = """
    select
        uuid,
        item_name,
        team,
        overall,
        best_buy_price,
        best_sell_price,
        spread,
        margin_pct,
        snapshot_at
    from analytics.market_snapshot
    order by best_sell_price desc
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn)


def load_price_history():
    query = """
    select
        h.uuid,
        i.player_name as item_name,
        i.team,
        i.overall,
        h.pulled_at,
        h.best_buy_price,
        h.best_sell_price
    from staging.listing_price_history h
    join staging.items i using (uuid)
    order by h.uuid, h.pulled_at
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn)


def build_summary(history_df):
    history_df = history_df.copy()
    history_df["mid_price"] = (
        history_df["best_buy_price"] + history_df["best_sell_price"]
    ) / 2

    summary = (
        history_df.sort_values("pulled_at")
        .groupby(["uuid", "item_name", "team", "overall"], as_index=False)
        .agg(
            first_mid_price=("mid_price", "first"),
            latest_mid_price=("mid_price", "last"),
            avg_buy_price=("best_buy_price", "mean"),
            avg_sell_price=("best_sell_price", "mean"),
        )
    )

    summary["price_change"] = summary["latest_mid_price"] - summary["first_mid_price"]
    summary["pct_change"] = (
        summary["price_change"] / summary["first_mid_price"]
    ) * 100

    return summary.sort_values("pct_change", ascending=False)


current_df = load_current_market()
history_df = load_price_history()
summary_df = build_summary(history_df)

st.title("MLB The Show Market Dashboard")
st.caption("Starter dashboard using sample data stored in Neon Postgres")

metric_col1, metric_col2, metric_col3 = st.columns(3)
metric_col1.metric("Cards tracked", len(current_df))
metric_col2.metric("Price history rows", len(history_df))
metric_col3.metric(
    "Top riser",
    summary_df.iloc[0]["item_name"],
    f"{summary_df.iloc[0]['pct_change']:.2f}%",
)

st.subheader("Investment Candidates")
st.dataframe(
    summary_df[
        [
            "item_name",
            "team",
            "overall",
            "first_mid_price",
            "latest_mid_price",
            "price_change",
            "pct_change",
        ]
    ],
    use_container_width=True,
)

st.subheader("Current Market Snapshot")
st.dataframe(current_df, use_container_width=True)

st.subheader("Price History")
selected_card = st.selectbox("Choose a card", summary_df["item_name"].tolist())

card_history = history_df[history_df["item_name"] == selected_card].copy()
card_history["mid_price"] = (
    card_history["best_buy_price"] + card_history["best_sell_price"]
) / 2

st.line_chart(
    card_history.set_index("pulled_at")[
        ["best_buy_price", "best_sell_price", "mid_price"]
    ]
)