import math

import pandas as pd
import streamlit as st

from db import get_connection

st.set_page_config(page_title="MLB The Show Market Dashboard", layout="wide")

COVER_IMAGE_PATH = "assets/mlb_the_show_26_cover.jpg"


def round_display(df, digits=2):
    rounded = df.copy()
    numeric_cols = rounded.select_dtypes(include=["number"]).columns
    rounded[numeric_cols] = rounded[numeric_cols].round(digits)
    return rounded


def load_current_market():
    query = """
    select
        m.uuid,
        m.item_name,
        i.series as set_name,
        m.team,
        m.overall,
        m.best_buy_price,
        m.best_sell_price,
        m.spread,
        m.margin_pct,
        m.snapshot_at
    from analytics.market_snapshot m
    left join staging.items i using (uuid)
    order by m.best_sell_price desc nulls last
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn)


def load_price_history():
    query = """
    select
        h.uuid,
        coalesce(i.player_name, i.name) as item_name,
        i.series as set_name,
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


def percentile_rank(series, ascending=True):
    clean = pd.to_numeric(series, errors="coerce")
    ranked = clean.rank(pct=True, ascending=ascending)
    return ranked * 100


def safe_divide(a, b):
    if pd.isna(a) or pd.isna(b) or b == 0:
        return None
    return a / b


def compute_trend_slope(times, prices):
    clean = pd.DataFrame({"t": times, "p": prices}).dropna()
    if len(clean) < 2:
        return None

    t0 = clean["t"].min()
    clean["x"] = (clean["t"] - t0).dt.total_seconds() / 3600.0
    clean["y"] = clean["p"].astype(float)

    x = clean["x"]
    y = clean["y"]

    x_mean = x.mean()
    y_mean = y.mean()

    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return None

    slope = (((x - x_mean) * (y - y_mean)).sum()) / denom
    return slope


def compute_max_drawdown(prices):
    clean = pd.Series(prices).dropna().astype(float)
    if len(clean) < 2:
        return None

    running_max = clean.cummax()
    drawdown = (clean - running_max) / running_max
    return abs(drawdown.min()) * 100


def compute_downside_volatility(returns):
    clean = pd.Series(returns).dropna().astype(float)
    negative = clean[clean < 0]
    if len(negative) < 2:
        return 0.0
    return negative.std()


def compute_positive_interval_ratio(returns):
    clean = pd.Series(returns).dropna().astype(float)
    if len(clean) == 0:
        return None
    return (clean > 0).mean() * 100


def build_summary(history_df):
    df = history_df.copy()

    df["pulled_at"] = pd.to_datetime(df["pulled_at"])
    df["best_buy_price"] = pd.to_numeric(df["best_buy_price"], errors="coerce")
    df["best_sell_price"] = pd.to_numeric(df["best_sell_price"], errors="coerce")
    df["mid_price"] = (df["best_buy_price"] + df["best_sell_price"]) / 2

    rows = []

    for (uuid, item_name, set_name, team, overall), group in df.groupby(
        ["uuid", "item_name", "set_name", "team", "overall"], dropna=False
    ):
        group = group.sort_values("pulled_at").copy()
        group["mid_return"] = group["mid_price"].pct_change() * 100

        history_points = len(group)
        first_mid = group["mid_price"].iloc[0]
        latest_mid = group["mid_price"].iloc[-1]
        avg_mid = group["mid_price"].mean()
        min_mid = group["mid_price"].min()
        max_mid = group["mid_price"].max()
        latest_buy = group["best_buy_price"].iloc[-1]
        latest_sell = group["best_sell_price"].iloc[-1]
        latest_seen = group["pulled_at"].iloc[-1]

        pct_change = None
        if pd.notna(first_mid) and first_mid > 0 and pd.notna(latest_mid):
            pct_change = ((latest_mid - first_mid) / first_mid) * 100

        spread_abs = latest_sell - latest_buy if pd.notna(latest_sell) and pd.notna(latest_buy) else None
        spread_pct = safe_divide(spread_abs, latest_mid)
        if spread_pct is not None:
            spread_pct *= 100

        discount_from_avg_pct = None
        if pd.notna(avg_mid) and avg_mid > 0 and pd.notna(latest_mid):
            discount_from_avg_pct = ((avg_mid - latest_mid) / avg_mid) * 100

        volatility_abs = group["mid_price"].std() if history_points >= 2 else None
        volatility_pct = None
        if pd.notna(volatility_abs) and pd.notna(avg_mid) and avg_mid > 0:
            volatility_pct = (volatility_abs / avg_mid) * 100

        trend_slope_abs = compute_trend_slope(group["pulled_at"], group["mid_price"])
        trend_slope_pct = None
        if trend_slope_abs is not None and pd.notna(avg_mid) and avg_mid > 0:
            trend_slope_pct = (trend_slope_abs / avg_mid) * 100

        max_drawdown_pct = compute_max_drawdown(group["mid_price"])
        downside_volatility = compute_downside_volatility(group["mid_return"])
        positive_interval_ratio = compute_positive_interval_ratio(group["mid_return"])

        range_position = None
        if pd.notna(min_mid) and pd.notna(max_mid) and max_mid != min_mid and pd.notna(latest_mid):
            range_position = (latest_mid - min_mid) / (max_mid - min_mid)

        recency_return_pct = None
        if history_points >= 2:
            prev_mid = group["mid_price"].iloc[-2]
            if pd.notna(prev_mid) and prev_mid > 0 and pd.notna(latest_mid):
                recency_return_pct = ((latest_mid - prev_mid) / prev_mid) * 100

        rows.append(
            {
                "uuid": uuid,
                "item_name": item_name,
                "set_name": set_name,
                "team": team,
                "overall": overall,
                "history_points": history_points,
                "first_mid_price": first_mid,
                "latest_mid_price": latest_mid,
                "avg_mid_price": avg_mid,
                "min_mid_price": min_mid,
                "max_mid_price": max_mid,
                "latest_buy_price": latest_buy,
                "latest_sell_price": latest_sell,
                "latest_seen": latest_seen,
                "pct_change": pct_change,
                "price_change": latest_mid - first_mid if pd.notna(first_mid) and pd.notna(latest_mid) else None,
                "spread": spread_abs,
                "spread_pct": spread_pct,
                "discount_from_avg_pct": discount_from_avg_pct,
                "volatility_abs": volatility_abs,
                "volatility_pct": volatility_pct,
                "trend_slope_pct": trend_slope_pct,
                "max_drawdown_pct": max_drawdown_pct,
                "downside_volatility": downside_volatility,
                "positive_interval_ratio": positive_interval_ratio,
                "range_position": range_position,
                "recency_return_pct": recency_return_pct,
            }
        )

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    summary["set_name"] = summary["set_name"].fillna("Unknown")
    summary["enough_history"] = summary["history_points"] >= 3

    eligible = summary["enough_history"]

    summary["momentum_component"] = percentile_rank(
        summary["trend_slope_pct"].fillna(summary["pct_change"]), ascending=True
    )
    summary["value_component"] = percentile_rank(
        summary["discount_from_avg_pct"].fillna(0), ascending=True
    )
    summary["liquidity_component"] = percentile_rank(
        summary["spread_pct"].fillna(0), ascending=True
    )
    summary["consistency_component"] = percentile_rank(
        summary["positive_interval_ratio"].fillna(0), ascending=True
    )
    summary["stability_component"] = percentile_rank(
        summary["volatility_pct"].fillna(summary["downside_volatility"]).fillna(0),
        ascending=False,
    )
    summary["drawdown_component"] = percentile_rank(
        summary["max_drawdown_pct"].fillna(0), ascending=False
    )
    summary["confidence_component"] = percentile_rank(
        summary["history_points"].fillna(0), ascending=True
    )
    summary["timing_component"] = percentile_rank(
        (1 - summary["range_position"]).fillna(0), ascending=True
    )

    summary["risk_vol_component"] = percentile_rank(
        summary["volatility_pct"].fillna(0), ascending=True
    )
    summary["risk_downside_component"] = percentile_rank(
        summary["downside_volatility"].fillna(0), ascending=True
    )
    summary["risk_drawdown_component"] = percentile_rank(
        summary["max_drawdown_pct"].fillna(0), ascending=True
    )
    summary["risk_spread_component"] = percentile_rank(
        summary["spread_pct"].fillna(0), ascending=True
    )
    summary["risk_thin_history_component"] = percentile_rank(
        -summary["history_points"].fillna(0), ascending=True
    )

    summary["investment_score"] = (
        summary["momentum_component"] * 0.22
        + summary["value_component"] * 0.18
        + summary["consistency_component"] * 0.16
        + summary["stability_component"] * 0.12
        + summary["confidence_component"] * 0.12
        + summary["timing_component"] * 0.10
        + summary["liquidity_component"] * 0.10
    )

    summary["risk_score"] = (
        summary["risk_vol_component"] * 0.28
        + summary["risk_downside_component"] * 0.24
        + summary["risk_drawdown_component"] * 0.24
        + summary["risk_spread_component"] * 0.12
        + summary["risk_thin_history_component"] * 0.12
    )

    summary.loc[~eligible, "investment_score"] = pd.NA
    summary.loc[~eligible, "risk_score"] = pd.NA

    summary["risk_adjusted_score"] = summary["investment_score"] - (summary["risk_score"] * 0.35)
    summary.loc[~eligible, "risk_adjusted_score"] = pd.NA

    summary["risk_adjusted_component"] = percentile_rank(
        summary["risk_adjusted_score"].fillna(0), ascending=True
    )

    summary["affordability_component"] = 100 - percentile_rank(
        summary["latest_buy_price"].fillna(summary["latest_mid_price"]).fillna(0),
        ascending=True,
    )

    summary["margin_of_safety_component"] = percentile_rank(
        summary["discount_from_avg_pct"].clip(lower=0).fillna(0), ascending=True
    )

    summary["expected_upside_pct"] = (
        summary["pct_change"].clip(lower=0).fillna(0) * 0.40
        + summary["discount_from_avg_pct"].clip(lower=0).fillna(0) * 0.35
        + summary["spread_pct"].clip(lower=0).fillna(0) * 0.25
    )

    summary["price_scale"] = summary["latest_buy_price"].apply(
        lambda x: math.log1p(x) if pd.notna(x) and x > 0 else None
    )

    summary["capital_efficiency_raw"] = summary["expected_upside_pct"] / summary["price_scale"]
    summary["capital_efficiency_component"] = percentile_rank(
        summary["capital_efficiency_raw"].fillna(0), ascending=True
    )

    summary["price_adjusted_value_score"] = (
        summary["risk_adjusted_component"] * 0.30
        + summary["affordability_component"] * 0.25
        + summary["capital_efficiency_component"] * 0.20
        + summary["margin_of_safety_component"] * 0.15
        + summary["stability_component"] * 0.10
    )

    summary.loc[~eligible, "price_adjusted_value_score"] = pd.NA

    return summary.sort_values(
        ["risk_adjusted_score", "investment_score", "pct_change"],
        ascending=[False, False, False],
        na_position="last",
    )


def build_market_insights(summary_df):
    insights = []

    ranked = summary_df.dropna(subset=["risk_adjusted_score"]).copy()
    if ranked.empty:
        return insights

    best = ranked.iloc[0]
    insights.append(
        f"Best risk-adjusted target is {best['item_name']} ({best['set_name']}) with Investment Score {best['investment_score']:.2f}/100 and Risk Score {best['risk_score']:.2f}/100."
    )

    undervalued = ranked.sort_values("discount_from_avg_pct", ascending=False).head(1)
    if not undervalued.empty and pd.notna(undervalued.iloc[0]["discount_from_avg_pct"]):
        row = undervalued.iloc[0]
        insights.append(
            f"Most undervalued tracked card is {row['item_name']} ({row['set_name']}), trading {row['discount_from_avg_pct']:.2f}% below its observed average price."
        )

    stable = ranked.sort_values("risk_score", ascending=True).head(1)
    if not stable.empty:
        row = stable.iloc[0]
        insights.append(
            f"Lowest-risk tracked card is {row['item_name']} ({row['set_name']}) with a Risk Score of {row['risk_score']:.2f}/100."
        )

    cheap_alpha = ranked.dropna(subset=["price_adjusted_value_score"]).copy()
    cheap_alpha = cheap_alpha[
        (cheap_alpha["latest_buy_price"].notna()) &
        (cheap_alpha["latest_buy_price"] > 0) &
        (cheap_alpha["latest_buy_price"] <= 5000)
    ].sort_values("price_adjusted_value_score", ascending=False).head(1)

    if not cheap_alpha.empty:
        row = cheap_alpha.iloc[0]
        insights.append(
            f"Best lower-cost value target under 5000 stubs is {row['item_name']} ({row['set_name']}) with Price-Adjusted Value Score {row['price_adjusted_value_score']:.2f}/100 and Buy Now price {row['latest_buy_price']:.2f}."
        )

    return insights


current_df = load_current_market()
history_df = load_price_history()
summary_df = build_summary(history_df)

header_col1, header_col2 = st.columns([1, 5])

with header_col1:
    st.image(COVER_IMAGE_PATH, width=180)

with header_col2:
    st.title("MLB The Show Market Dashboard")
    st.caption("Risk-adjusted market analytics powered by Neon Postgres")

if summary_df.empty:
    st.warning("No market history is loaded yet.")
    st.stop()

ranked_df = summary_df.dropna(subset=["risk_adjusted_score"]).copy()
ranked_df = ranked_df[
    (ranked_df["latest_buy_price"].notna()) &
    (ranked_df["latest_buy_price"] > 0)
]
top_10_df = ranked_df.head(10).copy()

price_adjusted_df = summary_df.dropna(subset=["price_adjusted_value_score"]).copy()
price_adjusted_df = price_adjusted_df[
    (price_adjusted_df["latest_buy_price"].notna()) &
    (price_adjusted_df["latest_buy_price"] > 0) &
    (price_adjusted_df["latest_buy_price"] <= 5000)
]
price_adjusted_df = price_adjusted_df.sort_values(
    ["price_adjusted_value_score", "investment_score", "latest_buy_price"],
    ascending=[False, False, True],
    na_position="last",
)
price_adjusted_top_10 = price_adjusted_df.head(10).copy()

insights = build_market_insights(summary_df)

top_riser_df = summary_df.dropna(subset=["pct_change"]).sort_values("pct_change", ascending=False)

top_bar_left, top_bar_right = st.columns([3, 1])

with top_bar_left:
    st.markdown(
        """
        <div style="
            padding: 14px 18px;
            border-radius: 14px;
            background: linear-gradient(90deg, #0f1724, #1c2940);
            border: 1px solid rgba(255,255,255,0.08);
            margin-bottom: 10px;
        ">
            <div style="font-size:14px; color:#9fb3c8;">Live Market Intelligence</div>
            <div style="font-size:24px; font-weight:700; color:#ffffff;">Track price momentum, risk, and opportunity across the MLB The Show market.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with top_bar_right:
    if not top_riser_df.empty:
        top_riser = top_riser_df.iloc[0]
        st.metric("Top riser", top_riser["item_name"], f"{top_riser['pct_change']:.2f}%")
    else:
        st.metric("Top riser", "Not enough history yet", "")

st.subheader("Top 10 Investment Targets")
st.caption(
    "Ranked on normalized momentum, value, consistency, stability, timing, liquidity, and history confidence. Scores are scaled 0-100."
)

if top_10_df.empty:
    st.info("Not enough history yet to build robust investment rankings.")
else:
    display_top_10 = top_10_df[
        [
            "item_name",
            "set_name",
            "team",
            "overall",
            "history_points",
            "latest_buy_price",
            "latest_sell_price",
            "spread_pct",
            "pct_change",
            "discount_from_avg_pct",
            "investment_score",
            "risk_score",
            "risk_adjusted_score",
        ]
    ].copy()

    display_top_10 = display_top_10.rename(
        columns={
            "item_name": "Player",
            "set_name": "Set",
            "team": "Team",
            "overall": "Overall",
            "history_points": "Snapshots",
            "latest_buy_price": "Buy Now",
            "latest_sell_price": "Sell Now",
            "spread_pct": "Spread %",
            "pct_change": "% Change",
            "discount_from_avg_pct": "% Below Avg",
            "investment_score": "Investment Score",
            "risk_score": "Risk Score",
            "risk_adjusted_score": "Risk-Adjusted Score",
        }
    )

    st.dataframe(round_display(display_top_10), use_container_width=True)

st.subheader("Top 10 Price-Adjusted Value Targets")
st.caption(
    "Only cards with a Buy Now price between 0 and 5000 stubs are included here. This ranking favors cheaper cards with strong quality per unit of capital."
)

if price_adjusted_top_10.empty:
    st.info("No qualifying lower-cost value targets are available right now.")
else:
    display_price_adjusted = price_adjusted_top_10[
        [
            "item_name",
            "set_name",
            "team",
            "overall",
            "latest_buy_price",
            "latest_sell_price",
            "expected_upside_pct",
            "affordability_component",
            "capital_efficiency_component",
            "investment_score",
            "risk_score",
            "price_adjusted_value_score",
        ]
    ].copy()

    display_price_adjusted = display_price_adjusted.rename(
        columns={
            "item_name": "Player",
            "set_name": "Set",
            "team": "Team",
            "overall": "Overall",
            "latest_buy_price": "Buy Now",
            "latest_sell_price": "Sell Now",
            "expected_upside_pct": "Expected Upside %",
            "affordability_component": "Affordability Score",
            "capital_efficiency_component": "Capital Efficiency Score",
            "investment_score": "Investment Score",
            "risk_score": "Risk Score",
            "price_adjusted_value_score": "Price-Adjusted Value Score",
        }
    )

    st.dataframe(round_display(display_price_adjusted), use_container_width=True)

st.subheader("How The Scores Work")

score_col1, score_col2, score_col3, score_col4 = st.columns(4)

with score_col1:
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #18253a, #22385a);
            padding: 18px;
            border-radius: 16px;
            min-height: 200px;
            border: 1px solid rgba(255,255,255,0.08);
        ">
            <h4 style="margin-top:0; color:#ffffff;">Investment Score</h4>
            <p style="color:#d7e3f4; font-size:15px; line-height:1.5;">
                Measures upside potential using normalized momentum, value versus historical average,
                trend consistency, timing within the observed range, spread opportunity, and snapshot depth.
            </p>
            <p style="color:#8ec5ff; font-weight:600; margin-bottom:0;">Scale: 0 to 100</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with score_col2:
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #3a1d1d, #5a2b2b);
            padding: 18px;
            border-radius: 16px;
            min-height: 200px;
            border: 1px solid rgba(255,255,255,0.08);
        ">
            <h4 style="margin-top:0; color:#ffffff;">Risk Score</h4>
            <p style="color:#f4d7d7; font-size:15px; line-height:1.5;">
                Measures downside exposure using relative volatility, downside volatility,
                max drawdown, spread risk, and limited history risk.
            </p>
            <p style="color:#ffb3b3; font-weight:600; margin-bottom:0;">Scale: 0 to 100</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with score_col3:
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #1f3a24, #2d5a38);
            padding: 18px;
            border-radius: 16px;
            min-height: 200px;
            border: 1px solid rgba(255,255,255,0.08);
        ">
            <h4 style="margin-top:0; color:#ffffff;">Risk-Adjusted Score</h4>
            <p style="color:#d9f0dd; font-size:15px; line-height:1.5;">
                Combines opportunity and danger by rewarding strong investments while penalizing
                unstable, high-risk cards. This is the best single ranking for balanced decisions.
            </p>
            <p style="color:#9ff0b0; font-weight:600; margin-bottom:0;">Higher is better</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with score_col4:
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #3b2a16, #5e4323);
            padding: 18px;
            border-radius: 16px;
            min-height: 200px;
            border: 1px solid rgba(255,255,255,0.08);
        ">
            <h4 style="margin-top:0; color:#ffffff;">Price-Adjusted Value Score</h4>
            <p style="color:#f3e2c9; font-size:15px; line-height:1.5;">
                Favors cards that combine strong investment quality with lower capital requirements,
                using affordability, capital efficiency, margin of safety, stability, and risk-adjusted strength.
            </p>
            <p style="color:#f5c67a; font-weight:600; margin-bottom:0;">Best for cheaper targets</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.subheader("Market Insights")
if insights:
    for insight in insights:
        st.markdown(f"- {insight}")
else:
    st.info("Insights will improve as more price snapshots are collected.")

st.subheader("Current Market Analysis")
analysis_df = summary_df[
    [
        "item_name",
        "set_name",
        "team",
        "overall",
        "latest_buy_price",
        "latest_sell_price",
        "spread_pct",
        "pct_change",
        "discount_from_avg_pct",
        "volatility_pct",
        "max_drawdown_pct",
        "history_points",
        "investment_score",
        "risk_score",
        "price_adjusted_value_score",
    ]
].copy()

analysis_df = analysis_df.rename(
    columns={
        "item_name": "Player",
        "set_name": "Set",
        "team": "Team",
        "overall": "Overall",
        "latest_buy_price": "Buy Now",
        "latest_sell_price": "Sell Now",
        "spread_pct": "Spread %",
        "pct_change": "% Change",
        "discount_from_avg_pct": "% Below Avg",
        "volatility_pct": "Volatility %",
        "max_drawdown_pct": "Max Drawdown %",
        "history_points": "Snapshots",
        "investment_score": "Investment Score",
        "risk_score": "Risk Score",
        "price_adjusted_value_score": "Price-Adjusted Value Score",
    }
)

st.dataframe(round_display(analysis_df), use_container_width=True)

st.subheader("Price History Search")
st.caption("Click the dropdown and start typing for autocomplete suggestions.")

player_options = (
    history_df[["item_name", "set_name"]]
    .dropna(subset=["item_name"])
    .drop_duplicates()
    .fillna("Unknown")
)

player_options["display_name"] = player_options["item_name"] + " (" + player_options["set_name"] + ")"
player_options = player_options.sort_values("display_name")

selected_display = st.selectbox(
    "Search and select a player",
    player_options["display_name"].tolist(),
    index=None,
    placeholder="Start typing a player name...",
)

if selected_display:
    selected_row = player_options[player_options["display_name"] == selected_display].iloc[0]
    selected_player = selected_row["item_name"]
    selected_set = selected_row["set_name"]

    player_history = history_df[
        (history_df["item_name"] == selected_player) &
        (history_df["set_name"].fillna("Unknown") == selected_set)
    ].copy()

    player_history["best_buy_price"] = pd.to_numeric(player_history["best_buy_price"], errors="coerce")
    player_history["best_sell_price"] = pd.to_numeric(player_history["best_sell_price"], errors="coerce")
    player_history["mid_price"] = (
        player_history["best_buy_price"] + player_history["best_sell_price"]
    ) / 2

    player_summary = summary_df[
        (summary_df["item_name"] == selected_player) &
        (summary_df["set_name"].fillna("Unknown") == selected_set)
    ].head(1)

    if not player_summary.empty:
        row = player_summary.iloc[0]
        score_col1, score_col2, score_col3, score_col4 = st.columns(4)
        score_col1.metric(
            "Investment Score",
            f"{row['investment_score']:.2f}" if pd.notna(row["investment_score"]) else "N/A",
        )
        score_col2.metric(
            "Risk Score",
            f"{row['risk_score']:.2f}" if pd.notna(row["risk_score"]) else "N/A",
        )
        score_col3.metric(
            "Risk-Adjusted Score",
            f"{row['risk_adjusted_score']:.2f}" if pd.notna(row["risk_adjusted_score"]) else "N/A",
        )
        score_col4.metric(
            "Price-Adjusted Value Score",
            f"{row['price_adjusted_value_score']:.2f}" if pd.notna(row["price_adjusted_value_score"]) else "N/A",
        )

    if player_history.empty:
        st.info("No history found for this player yet.")
    else:
        st.line_chart(
            player_history.set_index("pulled_at")[
                ["best_buy_price", "best_sell_price", "mid_price"]
            ]
        )

        history_display = player_history[
            ["pulled_at", "item_name", "set_name", "team", "overall", "best_buy_price", "best_sell_price", "mid_price"]
        ].rename(
            columns={
                "pulled_at": "Timestamp",
                "item_name": "Player",
                "set_name": "Set",
                "team": "Team",
                "overall": "Overall",
                "best_buy_price": "Buy Now",
                "best_sell_price": "Sell Now",
                "mid_price": "Mid Price",
            }
        )

        st.dataframe(round_display(history_display), use_container_width=True)