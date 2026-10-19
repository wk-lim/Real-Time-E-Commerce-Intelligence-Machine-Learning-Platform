import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import streamlit as st


API_URL = os.getenv("PURCHASE_API_URL", "http://127.0.0.1:18081").rstrip("/")

COUNT_FIELDS = (
    "events_previous_day",
    "page_visits_previous_day",
    "search_previous_day",
    "client_cart_additions_previous_day",
    "client_cart_removals_previous_day",
    "client_purchases_previous_day",
    "distinct_skus_previous_day",
    "sku_interactions_previous_day",
    "sku_cart_additions_previous_day",
    "sku_cart_removals_previous_day",
    "sku_purchases_previous_day",
)
RECENCY_FIELDS = (
    "hours_since_last_client_event",
    "hours_since_last_sku_event",
)

st.set_page_config(page_title="Purchase scoring", page_icon="🛒")
st.title("Client-product purchase scoring")
st.caption(
    "Enter an already-computed feature snapshot. This dashboard does not "
    "look up clients or calculate features from live events."
)

with st.form("feature_snapshot"):
    features = {}
    for name in COUNT_FIELDS:
        features[name] = st.number_input(
            name.replace("_", " ").capitalize(),
            min_value=0,
            value=0,
            step=1,
        )
    for name in RECENCY_FIELDS:
        features[name] = st.number_input(
            name.replace("_", " ").capitalize(),
            min_value=0.0,
            value=0.0,
            step=0.5,
        )
    submitted = st.form_submit_button("Score purchase")

if submitted:
    request = Request(
        f"{API_URL}/predict",
        data=json.dumps(features).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            result = json.load(response)
    except HTTPError as exc:
        st.error(f"Scoring API returned HTTP {exc.code}: {exc.read().decode()}")
    except URLError as exc:
        st.error(f"Cannot reach the scoring API at {API_URL}: {exc.reason}")
    else:
        st.metric(
            "Purchase observed within 7 days",
            f"{result['purchase_within_7d_probability']:.2%}",
        )
        st.subheader("Timing, conditional on a purchase")
        timing = result["timing_given_purchase"]
        st.write(f"Under 24 hours: {timing['under_24h']:.2%}")
        st.write(f"24–72 hours: {timing['hours_24_to_72']:.2%}")
        st.write(f"72–168 hours: {timing['hours_72_to_168']:.2%}")
        st.caption(
            "These scores are experimental, not a reason for non-purchase "
            "or a production customer decision."
        )