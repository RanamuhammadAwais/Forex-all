import streamlit as st
import pandas as pd
from datetime import datetime, time

import dukascopy_python
import dukascopy_python.instruments as instr_module

st.title("ACE Treasury Forex Data Analyzer")
st.markdown('Developed by <a href="https://www.linkedin.com/in/rmuhammadawais/" target="_blank" style="color:#0077B5; font-weight:bold; text-decoration:none;">RMA</a>', unsafe_allow_html=True)

# ------------------------------------------------------------------
# Safe instrument lookup
# ------------------------------------------------------------------
def find_instrument(pair: str):
    base = pair[:3].upper()
    quote = pair[3:].upper()
    candidates = [
        f"INSTRUMENT_FX_MAJORS_{base}_{quote}",
        f"INSTRUMENT_FX_MAJORS_{quote}_{base}",
        f"INSTRUMENT_FX_OTHERS_{base}_{quote}",
        f"INSTRUMENT_FX_OTHERS_{quote}_{base}",
        f"INSTRUMENT_FX_{base}_{quote}",
        f"INSTRUMENT_FX_{quote}_{base}",
    ]
    for name in candidates:
        val = getattr(instr_module, name, None)
        if val is not None:
            return val, name
    for attr in dir(instr_module):
        if base in attr and quote in attr:
            return getattr(instr_module, attr), attr
    return None, None

# ------------------------------------------------------------------
# Default pairs
# ------------------------------------------------------------------
DEFAULT_PAIRS = ["AUDUSD", "CZKUSD", "CHFUSD", "EURUSD", "GBPUSD",
                 "SEKUSD", "NOKUSD", "RONUSD", "PLNUSD"]

verified_pairs = {}
unresolved = []

for p in DEFAULT_PAIRS:
    instrument_val, instrument_name = find_instrument(p)
    if instrument_val is not None:
        verified_pairs[p] = instrument_val
    else:
        unresolved.append(p)

if unresolved:
    st.warning(
        f"These pairs could not be resolved in your installed dukascopy_python version "
        f"and have been skipped: {', '.join(unresolved)}"
    )

# ------------------------------------------------------------------
# UI
# ------------------------------------------------------------------
st.subheader("Currency Pair Selection")

selected_pairs = st.multiselect(
    "Select one or more default pairs",
    options=list(verified_pairs.keys()),
    default=["EURUSD"] if "EURUSD" in verified_pairs else list(verified_pairs.keys())[:1]
)

custom_input = st.text_input(
    "Add custom pairs (comma-separated, e.g. USDJPY, USDCAD)",
    value=""
).strip().upper()

# Parse and validate custom pairs
custom_pairs = {}
if custom_input:
    for raw in custom_input.split(","):
        p = raw.strip()
        if len(p) == 6:
            instr_val, instr_name = find_instrument(p)
            if instr_val is not None:
                custom_pairs[p] = instr_val
                st.success(f"✅ '{p}' resolved → `{instr_name}`")
            else:
                st.error(f"❌ Could not resolve '{p}' — check the pair name.")

# Merge default selected + custom
all_selected = {p: verified_pairs[p] for p in selected_pairs}
all_selected.update(custom_pairs)

if not all_selected:
    st.warning("Please select at least one currency pair.")
    st.stop()

st.info(f"**Selected pairs:** {', '.join(all_selected.keys())}")

start_date = st.date_input("Start Date")
end_date   = st.date_input("End Date")

start_time = st.time_input("Start Time (GMT)", time(7, 0))
end_time   = st.time_input("End Time (GMT)", time(13, 0))

run = st.button("Fetch Data")

# ------------------------------------------------------------------
# Helper: fetch + process one pair
# ------------------------------------------------------------------
def fetch_pair(pair_name, instrument, start, end, start_time, end_time):

    interval     = dukascopy_python.INTERVAL_HOUR_1
    offer_side   = dukascopy_python.OFFER_SIDE_BID

    raw = dukascopy_python.fetch(instrument, interval, offer_side, start, end)

    if isinstance(raw, pd.DataFrame):
        df = raw.copy()
    else:
        df = pd.DataFrame(raw)

    if df.empty:
        return None, "No data returned from Dukascopy."

    # Normalize index / time column
    if df.index.name and df.index.name.lower() in ("time", "timestamp", "date"):
        df = df.reset_index()

    for col in df.columns:
        if col.lower() in ("time", "timestamp", "date"):
            df.rename(columns={col: "time"}, inplace=True)
            break

    df.columns = [c.lower() for c in df.columns]

    df["time"] = pd.to_datetime(df["time"])
    if df["time"].dt.tz is not None:
        df["time"] = df["time"].dt.tz_convert(None)

    # Resample to 30 min
    df = df.set_index("time")
    agg_dict = {"open": "first", "high": "max", "low": "min", "close": "last"}
    if "volume" in df.columns:
        agg_dict["volume"] = "sum"
    df_30 = df.resample("30min").agg(agg_dict).dropna(subset=["open", "high", "low", "close"])
    df_30 = df_30.reset_index()

    # Filter by session time
    def to_seconds(t):
        return t.hour * 3600 + t.minute * 60 + t.second

    df_30["_secs"] = (
        df_30["time"].dt.hour * 3600 +
        df_30["time"].dt.minute * 60 +
        df_30["time"].dt.second
    )
    df_30 = df_30[
        (df_30["_secs"] >= to_seconds(start_time)) &
        (df_30["_secs"] <= to_seconds(end_time))
    ].drop(columns=["_secs"]).copy()

    if df_30.empty:
        return None, "No data in selected session window."

    return df_30, None

# ------------------------------------------------------------------
# Main logic
# ------------------------------------------------------------------
if run:
    start = datetime.combine(start_date, time(0, 0))
    end   = datetime.combine(end_date,   time(23, 59))

    st.write(f"Fetching data for: **{', '.join(all_selected.keys())}** ...")

    all_stats = []   # for combined summary table

    for pair_name, instrument in all_selected.items():

        st.markdown(f"---")
        st.subheader(f"🔹 {pair_name}")

        with st.spinner(f"Downloading {pair_name}..."):
            try:
                df_30, error = fetch_pair(
                    pair_name, instrument, start, end, start_time, end_time
                )

                if error:
                    st.warning(f"{pair_name}: {error}")
                    continue

                # Session stats
                high_price = df_30["high"].max()
                low_price  = df_30["low"].min()
                high_time  = df_30.loc[df_30["high"] == high_price, "time"].iloc[0]
                low_time   = df_30.loc[df_30["low"]  == low_price,  "time"].iloc[0]
                range_pips = round((high_price - low_price) * 10000, 2)

                # Per-pair stats table
                stats = pd.DataFrame({
                    "Metric": [
                        "Session High",
                        "Session Low",
                        "High Time (UTC)",
                        "Low Time (UTC)",
                        "Range (pips)"
                    ],
                    "Value": [
                        f"{high_price:.5f}",
                        f"{low_price:.5f}",
                        str(high_time),
                        str(low_time),
                        range_pips
                    ],
                })
                st.table(stats)

                # Raw data expander
                with st.expander(f"📄 Raw 30-min Data — {pair_name}"):
                    st.dataframe(df_30.reset_index(drop=True))

                # CSV download per pair
                csv = df_30.to_csv(index=False).encode()
                st.download_button(
                    label=f"⬇️ Download {pair_name} CSV",
                    data=csv,
                    file_name=f"{pair_name}_30min_{start_date}_to_{end_date}.csv",
                    mime="text/csv",
                    key=f"dl_{pair_name}"   # unique key per button
                )

                # Collect for combined summary
                all_stats.append({
                    "Pair":           pair_name,
                    "Session High":   f"{high_price:.5f}",
                    "Session Low":    f"{low_price:.5f}",
                    "High Time":      str(high_time),
                    "Low Time":       str(low_time),
                    "Range (pips)":   range_pips
                })

            except Exception as e:
                st.error(f"{pair_name} — Error: {e}")
                st.exception(e)

    # ------------------------------------------------------------------
    # Combined summary at the bottom
    # ------------------------------------------------------------------
    if len(all_stats) > 1:
        st.markdown("---")
        st.subheader("📊 Combined Summary — All Pairs")
        summary_df = pd.DataFrame(all_stats)
        st.dataframe(summary_df, use_container_width=True)

        # Combined CSV download
        combined_csv = summary_df.to_csv(index=False).encode()
        st.download_button(
            label="⬇️ Download Combined Summary CSV",
            data=combined_csv,
            file_name=f"ALL_PAIRS_summary_{start_date}_to_{end_date}.csv",
            mime="text/csv",
            key="dl_combined"
        )