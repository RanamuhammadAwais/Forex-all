import streamlit as st
import pandas as pd
from datetime import datetime, time

import dukascopy_python
import dukascopy_python.instruments as instr_module

st.title("ACE Treasury Forex Data Analyzer")
st.markdown(
    'Developed by <a href="https://www.linkedin.com/in/rmuhammadawais/" target="_blank" '
    'style="color:#0077B5; font-weight:bold; text-decoration:none;">RMA</a>',
    unsafe_allow_html=True,
)

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
# Resolve default pairs
# ------------------------------------------------------------------
DEFAULT_PAIRS = [
    "AUDUSD", "CZKUSD", "CHFUSD", "EURUSD", "GBPUSD",
    "SEKUSD", "NOKUSD", "RONUSD", "PLNUSD",
]

verified_pairs = {}
unresolved = []

for p in DEFAULT_PAIRS:
    instr_val, instr_name = find_instrument(p)
    if instr_val is not None:
        verified_pairs[p] = instr_val
    else:
        unresolved.append(p)

if unresolved:
    st.warning(
        f"These pairs could not be resolved and have been skipped: {', '.join(unresolved)}"
    )

# ------------------------------------------------------------------
# UI — Currency Pair Selection
# ------------------------------------------------------------------
st.subheader("Currency Pair Selection")

all_pair_keys = list(verified_pairs.keys())

if "selected_pairs" not in st.session_state:
    st.session_state.selected_pairs = ["EURUSD"] if "EURUSD" in verified_pairs else all_pair_keys[:1]

if st.button("★ Select All Default Currencies"):
    st.session_state.selected_pairs = all_pair_keys
    st.rerun()

selected_pairs = st.multiselect(
    "Select or remove individual pairs",
    options=all_pair_keys,
    default=st.session_state.selected_pairs,
    key="pair_multiselect",
)

st.session_state.selected_pairs = selected_pairs

custom_input = st.text_input(
    "Add custom pairs (comma-separated, e.g. USDJPY, USDCAD)",
    value="",
).strip().upper()

custom_pairs = {}
if custom_input:
    for raw in custom_input.split(","):
        p = raw.strip()
        if len(p) == 6:
            instr_val, instr_name = find_instrument(p)
            if instr_val is not None:
                custom_pairs[p] = instr_val
                st.success(f"'{p}' resolved → `{instr_name}`")
            else:
                st.error(f"Could not resolve '{p}' — check the pair name.")

all_selected = {p: verified_pairs[p] for p in selected_pairs}
all_selected.update(custom_pairs)

if not all_selected:
    st.warning("Please select at least one currency pair.")
    st.stop()

st.info(f"**Selected pairs:** {', '.join(all_selected.keys())}")

# ------------------------------------------------------------------
# UI — Parity Reversal
# ------------------------------------------------------------------
st.subheader("Parity Reversal (Optional)")
pairs_to_reverse = st.multiselect(
    "Select pairs to reverse parity (e.g. SEKUSD → USDSEK)",
    options=list(all_selected.keys()),
    default=[],
    help="Prices will be inverted (1/price) and the display name flipped for selected pairs.",
)

# ------------------------------------------------------------------
# UI — Date & Time
# ------------------------------------------------------------------
st.subheader("Date & Time Settings")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date")
with col2:
    end_date = st.date_input("End Date")

col3, col4 = st.columns(2)
with col3:
    start_time_input = st.time_input("Start Time (GMT)", time(7, 30))
with col4:
    end_time_input = st.time_input("End Time (GMT)", time(12, 0))

# ------------------------------------------------------------------
# UI — Optional Columns Toggle
# ------------------------------------------------------------------
st.subheader("Display Settings")

OPTIONAL_COLS = ["High Time (UTC)", "Low Time (UTC)", "Range (pips)"]

visible_optional_cols = st.multiselect(
    "Optional columns to show (uncheck to hide)",
    options=OPTIONAL_COLS,
    default=OPTIONAL_COLS,
    help="Choose which optional columns to display in the per-pair tables and summaries.",
)

run = st.button("Fetch Data")

# ------------------------------------------------------------------
# Helper: filter optional columns from a daily_df
# ------------------------------------------------------------------
def filter_optional_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [c for c in OPTIONAL_COLS if c not in visible_optional_cols and c in df.columns]
    return df.drop(columns=cols_to_drop)

# ------------------------------------------------------------------
# Helper: invert OHLC for reversed parity
# ------------------------------------------------------------------
def invert_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["open"]  = 1.0 / df["open"]
    df["close"] = 1.0 / df["close"]
    orig_high   = df["high"].copy()
    orig_low    = df["low"].copy()
    df["high"]  = 1.0 / orig_low
    df["low"]   = 1.0 / orig_high
    return df

# ------------------------------------------------------------------
# Helper: fetch one pair → daily breakdown DataFrame
# ------------------------------------------------------------------
def fetch_pair_daily(pair_name, instrument, start, end, s_time, e_time, reverse):
    interval   = dukascopy_python.INTERVAL_HOUR_1
    offer_side = dukascopy_python.OFFER_SIDE_BID

    raw = dukascopy_python.fetch(instrument, interval, offer_side, start, end)
    df  = raw.copy() if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)

    if df.empty:
        return None, None, "No data returned from Dukascopy."

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

    df = df.set_index("time")
    agg_dict = {"open": "first", "high": "max", "low": "min", "close": "last"}
    if "volume" in df.columns:
        agg_dict["volume"] = "sum"
    df_30 = (
        df.resample("30min")
        .agg(agg_dict)
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )

    def to_sec(t):
        return t.hour * 3600 + t.minute * 60 + t.second

    df_30["_secs"] = (
        df_30["time"].dt.hour * 3600
        + df_30["time"].dt.minute * 60
        + df_30["time"].dt.second
    )
    df_30 = df_30[
        (df_30["_secs"] >= to_sec(s_time)) & (df_30["_secs"] <= to_sec(e_time))
    ].drop(columns=["_secs"]).copy()

    if df_30.empty:
        return None, None, "No data in selected session window."

    if reverse:
        df_30 = invert_ohlc(df_30)

    display_name = (pair_name[3:] + pair_name[:3]) if reverse else pair_name
    pip_mult     = 100 if "JPY" in display_name.upper() else 10000

    df_30["date"] = df_30["time"].dt.date
    daily_rows = []
    for day, grp in df_30.groupby("date"):
        high_price = grp["high"].max()
        low_price  = grp["low"].min()
        high_time  = grp.loc[grp["high"] == high_price, "time"].iloc[0]
        low_time   = grp.loc[grp["low"]  == low_price,  "time"].iloc[0]
        range_pips = round(abs(high_price - low_price) * pip_mult, 2)
        daily_rows.append({
            "Date":            str(day),
            "Session High":    f"{high_price:.5f}",
            "Session Low":     f"{low_price:.5f}",
            "High Time (UTC)": str(high_time),
            "Low Time (UTC)":  str(low_time),
            "Range (pips)":    range_pips,
        })

    return pd.DataFrame(daily_rows), display_name, None

# ------------------------------------------------------------------
# Main fetch logic
# ------------------------------------------------------------------
if run:
    start_dt = datetime.combine(start_date, time(0, 0))
    end_dt   = datetime.combine(end_date,   time(23, 59))

    st.write(f"Fetching data for: **{', '.join(all_selected.keys())}** …")

    all_daily_dfs  = {}
    display_names  = {}

    # ----------------------------------------------------------------
    # Per-currency sections — daily high/low table + download
    # ----------------------------------------------------------------
    for pair_name, instrument in all_selected.items():
        reverse = pair_name in pairs_to_reverse

        st.markdown("---")
        with st.spinner(f"Downloading {pair_name}…"):
            try:
                daily_df, display_name, error = fetch_pair_daily(
                    pair_name, instrument, start_dt, end_dt,
                    start_time_input, end_time_input, reverse,
                )

                if error:
                    st.warning(f"{pair_name}: {error}")
                    continue

                label = f"🔹 {display_name}" + (" *(reversed)*" if reverse else "")
                st.subheader(label)

                display_df = filter_optional_cols(daily_df)
                st.dataframe(display_df, use_container_width=True)

                st.download_button(
                    label=f"⬇️ Download {display_name} CSV",
                    data=display_df.to_csv(index=False).encode(),
                    file_name=f"{display_name}_daily_{start_date}_to_{end_date}.csv",
                    mime="text/csv",
                    key=f"dl_{pair_name}",
                )

                all_daily_dfs[pair_name] = daily_df   # store full df for summaries
                display_names[pair_name] = display_name

            except Exception as e:
                st.error(f"{pair_name} — Error: {e}")
                st.exception(e)

    # ----------------------------------------------------------------
    # Currency-wise summary pivot table
    # Rows = currencies, Columns = dates (each date: High | Low)
    # ----------------------------------------------------------------
    if all_daily_dfs:
        st.markdown("---")
        st.subheader("🔹 Currency-wise Summary — All Selected Pairs")

        # Collect all dates across all pairs (sorted)
        all_dates = sorted(
            set(
                date
                for df in all_daily_dfs.values()
                for date in df["Date"].tolist()
            )
        )

        # Build MultiIndex columns: (date, "High") and (date, "Low")
        col_tuples = []
        for date in all_dates:
            col_tuples.append((date, "High"))
            col_tuples.append((date, "Low"))
        multi_cols = pd.MultiIndex.from_tuples(col_tuples)

        pivot_rows = []
        for pair_name, daily_df in all_daily_dfs.items():
            dname = display_names[pair_name]
            date_map = {
                row["Date"]: row
                for _, row in daily_df.iterrows()
            }
            row_data = {"Currency": dname}
            for date in all_dates:
                if date in date_map:
                    row_data[(date, "High")] = date_map[date]["Session High"]
                    row_data[(date, "Low")]  = date_map[date]["Session Low"]
                else:
                    row_data[(date, "High")] = "-"
                    row_data[(date, "Low")]  = "-"
            pivot_rows.append(row_data)

        pivot_df = pd.DataFrame(pivot_rows)
        pivot_df = pivot_df.set_index("Currency")
        pivot_df.columns = pd.MultiIndex.from_tuples(pivot_df.columns)

        st.dataframe(pivot_df, use_container_width=True)

        # Flatten columns for CSV export
        flat_df = pivot_df.copy()
        flat_df.columns = [f"{d} {h}" for d, h in flat_df.columns]
        flat_df = flat_df.reset_index()

        st.download_button(
            label="⬇️ Download Currency-wise Summary CSV",
            data=flat_df.to_csv(index=False).encode(),
            file_name=f"currency_summary_{start_date}_to_{end_date}.csv",
            mime="text/csv",
            key="dl_currency_summary",
        )