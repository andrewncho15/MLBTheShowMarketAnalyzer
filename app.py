import math

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
    order by best_sell_price desc nulls last
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn)


def load_price_history():
    query = """
    select
        h.uuid,
        coalesce(i.player_name, i.name) as item_name,
        i.team,
        i.overall,
        h.pulled_at,
        h.best_buy_price,
        h.best_sell_price
    from staging.listing_price_history h
    left join staging.items i using (uuid)
    order by h.uuid, h.pulled_at
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn)


def build_summary(history_df):
    df = history_df.copy()

    df["best_buy_price"] = pd.to_numeric(df["best_buy_price"], errors="coerce")
    df["best_sell_price"] = pd.to_numeric(df["best_sell_price"], errors="coerce")
    df["mid_price"] = (df["best_buy_price"] + df["best_sell_price"]) / 2

    def pct_return(series):
        first = series.iloc[0]
        last = series.iloc[-1]
        if pd.isna(first) or first <= 0 or pd.isna(last):
            return None
        return ((last - first) / first) * 100

    def price_volatility(series):
        clean = series.dropna()
        if len(clean) < 2:
            return None
        return clean.std()

    summary = (
        df.sort_values("pulled_at")
        .groupby(["uuid", "item_name", "team", "overall"], dropna=False, as_index=False)
        .agg(
            history_points=("mid_price", "size"),
            first_mid_price=("mid_price", "first"),
            latest_mid_price=("mid_price", "last"),
            avg_mid_price=("mid_price", "mean"),
            min_mid_price=("mid_price", "min"),
            max_mid_price=("mid_price", "max"),
            avg_buy_price=("best_buy_price", "mean"),
            avg_sell_price=("best_sell_price", "mean"),
            latest_buy_price=("best_buy_price", "last"),
            latest_sell_price=("best_sell_price", "last"),
            latest_seen=("pulled_at", "last"),
            pct_change=("mid_price", pct_return),
            volatility=("mid_price", price_volatility),
        )
    )

    summary["price_change"] = summary["latest_mid_price"] - summary["first_mid_price"]
    summary["spread"] = summary["latest_sell_price"] - summary["latest_buy_price"]

    summary["discount_from_avg_pct"] = (
        (summary["avg_mid_price"] - summary["latest_mid_price"]) / summary["avg_mid_price"]
    ) * 100

    summary["range_position"] = (
        (summary["latest_mid_price"] - summary["min_mid_price"])
        / (summary["max_mid_price"] - summary["min_mid_price"])
    )

    summary["range_position"] = summary["range_position"].replace([float("inf"), -float("inf")], pd.NA)
    summary["enough_history"] = summary["history_points"] >= 2

    summary["momentum_score"] = summary["pct_change"].fillna(0)
    summary["value_score"] = summary["discount_from_avg_pct"].fillna(0)
    summary["liquidity_score"] = summary["spread"].fillna(0)
    summary["stability_score"] = -summary["volatility"].fillna(0)

    summary["investment_score"] = (
        summary["momentum_score"] * 0.35
        + summary["value_score"] * 0.30
        + summary["liquidity_score"] * 0.20
        + summary["stability_score"] * 0.15
    )

    summary.loc[summary["history_points"] < 2, "investment_score"] = pd.NA

    return summary.sort_values(
        ["investment_score", "pct_change", "latest_mid_price"],
        ascending=[False, False, False],
        na_position="last",
    )


def build_market_insights(current_df, summary_df):
    insights = []

    if not summary_df.empty:
        valid_investments = summary_df.dropna(subset=["investment_score"])
        if not valid_investments.empty:
            best = valid_investments.iloc[0]
            insights.append(
                f"Best current investment candidate is {best['item_name']} with an investment score of {best['investment_score']:.2f}, "
                f"{best['pct_change']:.2f}% price momentum, and a current spread of {best['spread']:.0f}."
            )

        biggest_spread = summary_df.dropna(subset=["spread"]).sort_values("spread", ascending=False).head(1)
        if not biggest_spread.empty:
            row = biggest_spread.iloc[0]
            insights.append(
                f"Widest current spread belongs to {row['item_name']} at {row['spread']:.0f}, which may indicate stronger flip potential but also higher execution risk."
            )

        most_undervalued = summary_df.dropna(subset=["discount_from_avg_pct"]).sort_values(
            "discount_from_avg_pct", ascending=False
        ).head(1)
        if not most_undervalued.empty:
            row = most_undervalued.iloc[0]
            insights.append(
                f"Most discounted card versus its own average tracked price is {row['item_name']}, currently {row['discount_from_avg_pct']:.2f}% below its average observed price."
            )

    if not current_df.empty:
        avg_spread = current_df["spread"].dropna().mean()
        avg_sell = current_df["best_sell_price"].dropna().mean()
        insights.append(
            f"Across the current market snapshot, average sell price is {avg_sell:,.0f} and average spread is {avg_spread:,.0f}."
        )

    return insights


current_df = load_current_market()
history_df = load_price_history()
summary_df = build_summary(history_df)
ranked_df = summary_df.dropna(subset=["investment_score"]).copy()
top_10_df = ranked_df.head(10).copy()
insights = build_market_insights(current_df, summary_df)

st.title("MLB The Show Market Dashboard")
st.caption("Market analytics and investment signals powered by Neon Postgres")

top_riser_df = summary_df.dropna(subset=["pct_change"]).sort_values("pct_change", ascending=False)

metric_col = st.columns(1)[0]
if not top_riser_df.empty:
    top_riser = top_riser_df.iloc[0]
    metric_col.metric("Top riser", top_riser["item_name"], f"{top_riser['pct_change']:.2f}%")
else:
    metric_col.metric("Top riser", "Not enough history yet", "")

st.subheader("Top 10 Investment Targets")
st.caption("Ranking blends momentum, relative value, spread/liquidity, and stability from observed pricing history.")

if top_10_df.empty:
    st.info("Not enough history yet to calculate investment rankings.")
else:
    display_top_10 = top_10_df[
        [
            "item_name",
            "team",
            "overall",
            "history_points",
            "latest_buy_price",
            "latest_sell_price",
            "spread",
            "pct_change",
            "discount_from_avg_pct",
            "volatility",
            "investment_score",
        ]
    ].copy()

    display_top_10 = display_top_10.rename(
        columns={
            "item_name": "Player",
            "team": "Team",
            "overall": "Overall",
            "history_points": "Snapshots",
            "latest_buy_price": "Buy Now",
            "latest_sell_price": "Sell Now",
            "spread": "Spread",
            "pct_change": "% Change",
            "discount_from_avg_pct": "% Below Avg",
            "volatility": "Volatility",
            "investment_score": "Investment Score",
        }
    )

    st.dataframe(display_top_10, use_container_width=True)

st.subheader("Market Insights")
if insights:
    for insight in insights:
        st.markdown(f"- {insight}")
else:
    st.info("Market insights will improve as more price snapshots are collected.")

st.subheader("Current Market Analysis")
analysis_df = summary_df[
    [
        "item_name",
        "team",
        "overall",
        "latest_buy_price",
        "latest_sell_price",
        "spread",
        "pct_change",
        "discount_from_avg_pct",
        "volatility",
        "history_points",
    ]
].copy()

analysis_df = analysis_df.rename(
    columns={
        "item_name": "Player",
        "team": "Team",
        "overall": "Overall",
        "latest_buy_price": "Buy Now",
        "latest_sell_price": "Sell Now",
        "spread": "Spread",
        "pct_change": "% Change",
        "discount_from_avg_pct": "% Below Avg",
        "volatility": "Volatility",
        "history_points": "Snapshots",
    }
)

st.dataframe(analysis_df, use_container_width=True)

st.subheader("Price History Search")
player_names = sorted([name for name in history_df["item_name"].dropna().unique().tolist() if name])

search_text = st.text_input("Search for a player", placeholder="Type a player name...")
filtered_names = [name for name in player_names if search_text.lower() in name.lower()] if search_text else player_names

if not filtered_names:
    st.warning("No players match that search.")
else:
    selected_player = st.selectbox("Select a player", filtered_names)

    player_history = history_df[history_df["item_name"] == selected_player].copy()
    player_history["mid_price"] = (
        pd.to_numeric(player_history["best_buy_price"], errors="coerce")
        + pd.to_numeric(player_history["best_sell_price"], errors="coerce")
    ) / 2

    if player_history.empty:
        st.info("No history found for this player yet.")
    else:
        st.line_chart(
            player_history.set_index("pulled_at")[
                ["best_buy_price", "best_sell_price", "mid_price"]
            ]
        )

        st.dataframe(
            player_history[
                ["pulled_at", "item_name", "team", "overall", "best_buy_price", "best_sell_price", "mid_price"]
            ].rename(
                columns={
                    "pulled_at": "Timestamp",
                    "item_name": "Player",
                    "team": "Team",
                    "overall": "Overall",
                    "best_buy_price": "Buy Now",
                    "best_sell_price": "Sell Now",
                    "mid_price": "Mid Price",
                }
            ),
            use_container_width=True,
        )