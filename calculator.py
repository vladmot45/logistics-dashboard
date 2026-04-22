import streamlit as st
import pandas as pd
import json
import os
import io
import math
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from datetime import date, timedelta

GEOCACHE_FILE = "geocache.json"
ORLEN_URL = "https://tool.orlen.pl/api/wholesalefuelprices/ByProduct"
ORLEN_HEADERS = {
    "Origin": "https://www.orlen.pl",
    "Referer": "https://www.orlen.pl/",
}

# ── Geocache helpers ──────────────────────────────────────────────────────────

def load_geocache():
    if os.path.exists(GEOCACHE_FILE):
        with open(GEOCACHE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_geocache(cache):
    with open(GEOCACHE_FILE, "w") as f:
        json.dump(cache, f)


def geocode_single(address: str, cache: dict) -> list | None:
    if address in cache:
        return cache[address]
    geolocator = Nominatim(user_agent="dap_logistics_calculator")
    try:
        loc = geolocator.geocode(address, timeout=10)
        result = [loc.latitude, loc.longitude] if loc else None
    except Exception:
        result = None
    cache[address] = result
    save_geocache(cache)
    return result


def geocode_batch(addresses: list[str], cache: dict) -> dict:
    new_addresses = [a for a in addresses if a not in cache]
    if not new_addresses:
        return cache

    geolocator = Nominatim(user_agent="dap_logistics_calculator")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1, error_wait_seconds=5)

    bar = st.progress(0, text="Geocoding delivery addresses…")
    for i, addr in enumerate(new_addresses):
        try:
            loc = geocode(addr)
            cache[addr] = [loc.latitude, loc.longitude] if loc else None
        except Exception:
            cache[addr] = None
        bar.progress((i + 1) / len(new_addresses), text=f"Geocoding {i + 1}/{len(new_addresses)}…")

    save_geocache(cache)
    bar.empty()
    return cache


# ── Haversine distance ────────────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── ORLEN Ekodiesel price helpers ─────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_orlen_prices(date_from: str, date_to: str) -> dict[str, float]:
    """Return a {YYYY-MM-DD: price_pln_m3} dict for ORLEN Ekodiesel (productId=43)."""
    try:
        resp = requests.get(
            ORLEN_URL,
            params={"productId": 43, "from": date_from, "to": date_to},
            headers=ORLEN_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        st.session_state["_orlen_error"] = str(e)
        st.session_state["_orlen_raw"] = None
        return {}

    # Unwrap envelope shapes: plain list, or {"data": [...]} / {"items": [...]} etc.
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = (
            data.get("data") or data.get("items") or data.get("prices") or
            data.get("result") or data.get("Results") or []
        )
    else:
        items = []

    # Save raw sample for debug
    st.session_state["_orlen_raw"] = data
    st.session_state["_orlen_error"] = None

    # All known ORLEN field name variants
    DATE_FIELDS  = ["date", "Date", "transactionDate", "priceDate", "validFrom", "from", "effectiveDate"]
    PRICE_FIELDS = ["price", "Price", "wholesalePrice", "wholesaleNetPrice", "netPrice",
                    "value", "Value", "amount", "Amount", "fuelPrice"]

    lookup: dict[str, float] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_date = next((item[f] for f in DATE_FIELDS if f in item and item[f]), None)
        raw_price = next((item[f] for f in PRICE_FIELDS if f in item and item[f] is not None), None)
        if raw_date and raw_price is not None:
            date_key = str(raw_date)[:10]  # keep YYYY-MM-DD, strip time component
            try:
                lookup[date_key] = float(raw_price) / 1000  # API returns PLN/m³; convert to PLN/L
            except (TypeError, ValueError):
                pass

    return lookup


def orlen_on_date(target_date: date, lookup: dict[str, float]) -> float | None:
    """Return the ORLEN price on target_date, or the nearest earlier date."""
    if not lookup:
        return None
    sorted_dates = sorted(lookup.keys())
    target_str = target_date.isoformat()
    # Exact match first
    if target_str in lookup:
        return lookup[target_str]
    # Nearest date on or before target
    before = [d for d in sorted_dates if d <= target_str]
    if before:
        return lookup[before[-1]]
    # Fallback: nearest date after target
    return lookup[sorted_dates[0]]


def fetch_live_orlen(lookup: dict[str, float]) -> float | None:
    """Return the most recent ORLEN price from an already-fetched lookup."""
    if not lookup:
        return None
    latest_key = max(lookup.keys())
    return lookup[latest_key]


# ── Load Excel ────────────────────────────────────────────────────────────────

COLUMN_MAP = {
    "Dostawa.Termin":                    "Data dostawy",
    "Features.PozycjeZeSrednikami":      "Towar",
    "Kontrahent:Kod":                    "Klient",
    "OdbiorcaMiejsceDostawy.Adres":      "Miejscowosc",
    "OdbiorcaMiejsceDostawy.Adres.Kraj": "Kraj",
    "Features.Price":                    "PLN / t",
    "Features.KM":                       "KM",
    "Features.PricePerKM":               "PLN / km",
    "Features.TransportKontrahent:Kod":  "Features.TransportKontrahent:Kod",
    "Features.SUMA_ILOSCI":              "Tony",
}

@st.cache_data(show_spinner="Reading Excel file…")
def load_excel(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0)
    if "Features.PricePerKM.Waluta" in df.columns:
        df = df[df["Features.PricePerKM.Waluta"].astype(str).str.strip().str.upper() != "EUR"]
    if "Features.KM" in df.columns:
        df = df[pd.to_numeric(df["Features.KM"], errors="coerce").fillna(0) != 0]
    df = df.rename(columns=COLUMN_MAP)
    df["Data dostawy"] = pd.to_datetime(df["Data dostawy"], errors="coerce")
    df = df.dropna(subset=["Data dostawy", "Miejscowosc"])
    # Exclude planned / future deliveries — only keep dates strictly before today.
    _cutoff = pd.Timestamp(date.today())
    df = df[df["Data dostawy"] < _cutoff]
    df["Miejscowosc"] = df["Miejscowosc"].str.strip()
    return df


# ── Recommendation formula ────────────────────────────────────────────────────

def recommended_price(past_price, past_diesel, current_diesel, fuel_share):
    if past_diesel and past_diesel > 0 and current_diesel:
        return past_price * (1 + fuel_share * (current_diesel - past_diesel) / past_diesel)
    return past_price


# ── Page ──────────────────────────────────────────────────────────────────────

st.title("💰 Logistics Price Calculator")
st.caption("Enter a destination address")

# ── Fetch ORLEN prices for full range (today needed for live price widget) ────
# We do a broad initial fetch covering a rolling 3-year window to show the live
# price before the file is uploaded. The full range is re-fetched once we know
# the earliest delivery date.
_today = date.today()
_broad_start = (_today - timedelta(days=3 * 365)).isoformat()
_broad_end = _today.isoformat()
orlen_broad = fetch_orlen_prices(_broad_start, _broad_end)
live_diesel = fetch_live_orlen(orlen_broad)

diesel_display = f"{live_diesel:.2f} PLN/L" if live_diesel else "Unavailable"
st.metric("⛽ ORLEN Ekodiesel — Current Wholesale Price (PLN/L)", diesel_display)

# Debug expander — visible only when the API returns no data
if not live_diesel or not orlen_broad:
    with st.expander("⚠️ ORLEN API debug info", expanded=True):
        err = st.session_state.get("_orlen_error")
        raw = st.session_state.get("_orlen_raw")
        if err:
            st.error(f"Request error: {err}")
        elif raw is not None:
            st.warning("Request succeeded but no prices were parsed. Raw response sample:")
            st.json(raw if not isinstance(raw, list) else raw[:3])
        else:
            st.info("No response received yet.")

st.divider()

# ── Step 1: Inputs ────────────────────────────────────────────────────────────

col_a, col_b = st.columns([2, 1])
with col_a:
    dest_address = st.text_input(
        "📍 Destination Address",
        placeholder="e.g. 12345 Berlin, Germany",
    )
with col_b:
    radius_km = st.slider("📏 Search Radius (km)", min_value=5, max_value=150, value=35, step=5)

# ── File: shared session_state, allow re-upload ───────────────────────────────

new_upload = st.file_uploader("📂 Upload Delivery History (.xlsx)", type=["xlsx"])

if new_upload is not None:
    st.session_state["file_bytes"] = new_upload.read()
    st.session_state["file_name"] = new_upload.name

if "file_bytes" not in st.session_state or st.session_state["file_bytes"] is None:
    st.info("Upload your Excel file (or upload it on the Delivery Map page — it will carry over here automatically).")
    st.stop()

if new_upload is None:
    st.caption(f"Using previously uploaded file: **{st.session_state.get('file_name', 'unknown')}**")

if not dest_address:
    st.info("Enter a destination address to continue.")
    st.stop()

# ── Step 2: Geocode destination ───────────────────────────────────────────────

cache = load_geocache()

with st.spinner("Geocoding destination address…"):
    dest_coords = geocode_single(dest_address, cache)

if dest_coords is None:
    st.error(
        f"Could not geocode **{dest_address}**. "
        "Try adding a country name or postal code and try again."
    )
    st.stop()

dest_lat, dest_lon = dest_coords

# ── Step 3: Load data and find nearby deliveries ──────────────────────────────

df = load_excel(st.session_state["file_bytes"])
unique_addrs = df["Miejscowosc"].unique().tolist()
cache = geocode_batch(unique_addrs, cache)

def get_coords(addr):
    c = cache.get(addr)
    return (c[0], c[1]) if c else (None, None)

df[["lat", "lon"]] = df["Miejscowosc"].apply(lambda a: pd.Series(get_coords(a)))
df = df.dropna(subset=["lat", "lon"])

df["dist_km"] = df.apply(
    lambda r: haversine_km(dest_lat, dest_lon, r["lat"], r["lon"]), axis=1
)

nearby = df[df["dist_km"] <= radius_km].copy()

# ── Product filter ────────────────────────────────────────────────────────────

st.divider()
st.subheader(f"📦 Past Deliveries Within {radius_km} km")

available_products = sorted(nearby["Towar"].dropna().unique())
sel_products = st.multiselect(
    "🌾 Filter by Product",
    options=available_products,
    placeholder="Leave empty to show all products",
)
if sel_products:
    nearby = nearby[nearby["Towar"].isin(sel_products)]

n_found = len(nearby)

if n_found == 0:
    st.error(
        "No past deliveries found within the selected radius"
        + (" for the selected product(s)." if sel_products else ". Try increasing the search radius.")
    )
    st.stop()

if n_found < 3:
    st.warning(
        f"Only **{n_found}** past {'delivery' if n_found == 1 else 'deliveries'} found. "
        "Consider increasing the search radius for a more reliable estimate."
    )
else:
    st.success(f"Found **{n_found}** past deliveries within {radius_km} km.")

# Sort all nearby by date descending so "last N" means most recent
nearby = nearby.sort_values("Data dostawy", ascending=False).reset_index(drop=True)

n_calc = st.slider(
    "📅 Number of most recent deliveries to use in price calculation",
    min_value=1,
    max_value=n_found,
    value=min(10, n_found),
    step=1,
    help="Only the N most recent deliveries within the radius are used to calculate the recommended price range.",
)

calc_df = nearby.head(n_calc).copy()

# ── Step 4: Fetch ORLEN prices for the full delivery date range ───────────────

date_min = calc_df["Data dostawy"].min().date()
date_max = calc_df["Data dostawy"].max().date()

orlen_lookup = fetch_orlen_prices(
    date_from=(date_min - timedelta(days=7)).isoformat(),
    date_to=_today.isoformat(),
)

# Refresh live price from the narrower lookup (may have more recent entries)
live_diesel_current = fetch_live_orlen(orlen_lookup) or live_diesel

nearby["diesel_pln_m3"] = nearby["Data dostawy"].apply(
    lambda d: orlen_on_date(d.date(), orlen_lookup) if pd.notna(d) else None
)
calc_df["diesel_pln_m3"] = calc_df["Data dostawy"].apply(
    lambda d: orlen_on_date(d.date(), orlen_lookup) if pd.notna(d) else None
)

# ── Step 5 & 6: Recommended prices (on calc_df only) ─────────────────────────

# Custom fuel-share % — slider + number input kept in sync via callbacks
if "fuel_share_pct" not in st.session_state:
    st.session_state["fuel_share_pct"] = 25

# Re-sync both widgets to the canonical value BEFORE they are re-instantiated.
st.session_state["_fs_slider"] = st.session_state["fuel_share_pct"]
st.session_state["_fs_box"]    = st.session_state["fuel_share_pct"]

def _fs_from_slider():
    st.session_state["fuel_share_pct"] = st.session_state["_fs_slider"]

def _fs_from_box():
    st.session_state["fuel_share_pct"] = st.session_state["_fs_box"]

fs_c1, fs_c2 = st.columns([3, 1])
fs_c1.slider(
    "⛽ Fuel share of freight cost (%)",
    min_value=0, max_value=100, step=1,
    key="_fs_slider", on_change=_fs_from_slider,
    help="Portion of total freight cost attributed to fuel. Typical range: 20–30%.",
)
fs_c2.number_input(
    "Custom %", min_value=0, max_value=100, step=1,
    key="_fs_box", on_change=_fs_from_box,
)

fuel_share = st.session_state["fuel_share_pct"] / 100
fs_label = f"{st.session_state['fuel_share_pct']}%"

calc_df["rec_pln_t"] = calc_df.apply(
    lambda r: recommended_price(
        r["PLN / t"], r["diesel_pln_m3"], live_diesel_current, fuel_share
    ) if pd.notna(r["PLN / t"]) else None,
    axis=1,
)
calc_df["rec_pln_km"] = calc_df.apply(
    lambda r: recommended_price(
        r["PLN / km"], r["diesel_pln_m3"], live_diesel_current, fuel_share
    ) if pd.notna(r["PLN / km"]) else None,
    axis=1,
)

# ── Formula explanation (rendered here so live data is in scope) ──────────────

with st.expander("📐 How is the recommended price calculated?"):
    st.markdown("""
**Formula**

$$
P_{rec} = P_{past} \\times \\left(1 + f_{fuel} \\times \\frac{D_{today} - D_{past}}{D_{past}}\\right)
$$

| Symbol | Meaning |
|--------|---------|
| $P_{rec}$ | Recommended price today (PLN/t or PLN/km) |
| $P_{past}$ | Actual price charged on the past delivery |
| $f_{fuel}$ | Fuel share — fraction of total freight cost attributed to fuel (user-selectable) |
| $D_{today}$ | Today's ORLEN Ekodiesel wholesale price (PLN/L) |
| $D_{past}$ | ORLEN Ekodiesel wholesale price on the date of the past delivery (PLN/L) |

**What it means**

The formula adjusts a historical freight price for the change in diesel costs since that delivery.
Only the fuel portion ($f_{fuel}$) is scaled — driver wages, overhead, and margin stay fixed.
Choose the fuel share % based on haul length:
- **~20%** — conservative; shorter hauls where fuel is a smaller share of cost
- **~30%** — higher; long hauls where fuel dominates
""")

    example_row = calc_df.dropna(subset=["PLN / t", "diesel_pln_m3"]).head(1)
    if not example_row.empty and live_diesel_current:
        er = example_row.iloc[0]
        p_past   = float(er["PLN / t"])
        d_past   = float(er["diesel_pln_m3"])
        d_today  = live_diesel_current
        chg_pct  = (d_today - d_past) / d_past * 100
        fmult    = 1 + fuel_share * (d_today - d_past) / d_past
        adj_fs   = p_past * fmult
        addr     = er["Miejscowosc"]
        del_date = er["Data dostawy"].strftime("%d.%m.%Y")
        st.markdown(f"""
**Worked example — {addr} on {del_date}**

1. Past price: **{p_past:.2f} PLN/t**
2. ORLEN Ekodiesel on delivery date: **{d_past:.3f} PLN/L**
3. ORLEN Ekodiesel today: **{d_today:.3f} PLN/L**
4. Diesel price change: **{chg_pct:+.1f}%**

*At {fs_label} fuel share:*
$$P_{{rec}} = {p_past:.2f} \\times \\left(1 + {fuel_share:.2f} \\times \\frac{{{d_today:.3f} - {d_past:.3f}}}{{{d_past:.3f}}}\\right) = {p_past:.2f} \\times {fmult:.4f} = \\mathbf{{{adj_fs:.2f}}} \\text{{ PLN/t}}$$
""")
    else:
        st.info("No delivery with a matched diesel price found — worked example unavailable.")

# ── Display: past deliveries table (all nearby, most recent first) ────────────

st.caption(f"Showing all {n_found} deliveries within {radius_km} km — top {n_calc} (highlighted) used in price calculation.")

# ── Min / Max / Average summary (from calc_df) ────────────────────────────────

_stats = calc_df.dropna(subset=["PLN / t", "PLN / km", "diesel_pln_m3"])

if not _stats.empty:
    transport_col = "Features.TransportKontrahent:Kod"

    _min_t_row  = _stats.loc[_stats["PLN / t"].idxmin()]
    _max_t_row  = _stats.loc[_stats["PLN / t"].idxmax()]
    _min_km_row = _stats.loc[_stats["PLN / km"].idxmin()]
    _max_km_row = _stats.loc[_stats["PLN / km"].idxmax()]

    def _fmt_row(row, metric):
        return (
            f"{row[metric]:.2f}  \n"
            f"<span style='font-size:12px;color:#555'>"
            f"{row['Data dostawy'].strftime('%d.%m.%Y')} · "
            f"Diesel {row['diesel_pln_m3']:.3f} PLN/L · "
            f"{row[transport_col]}"
            f"</span>"
        )

    st.markdown("**📊 Summary of selected deliveries (PLN / tonne)**")
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"🔽 **Min**  \n{_fmt_row(_min_t_row, 'PLN / t')}", unsafe_allow_html=True)
    c2.markdown(f"🔼 **Max**  \n{_fmt_row(_max_t_row, 'PLN / t')}", unsafe_allow_html=True)
    c3.metric("⚖️ Average", f"{_stats['PLN / t'].mean():.2f} PLN/t")

    st.markdown("**📊 Summary of selected deliveries (PLN / km)**")
    c4, c5, c6 = st.columns(3)
    c4.markdown(f"🔽 **Min**  \n{_fmt_row(_min_km_row, 'PLN / km')}", unsafe_allow_html=True)
    c5.markdown(f"🔼 **Max**  \n{_fmt_row(_max_km_row, 'PLN / km')}", unsafe_allow_html=True)
    c6.metric("⚖️ Average", f"{_stats['PLN / km'].mean():.2f} PLN/km")

    st.divider()

display_cols = {
    "Miejscowosc": "Address",
    "Towar": "Product",
    "Data dostawy": "Delivery Date",
    "PLN / t": "PLN / tonne",
    "PLN / km": "PLN / km",
    "KM": "Distance (km)",
    "Features.TransportKontrahent:Kod": "Transport Companies",
    "Tony": "Tonnes",
    "diesel_pln_m3": "ORLEN Diesel (PLN/L)",
}

table_display = nearby[list(display_cols.keys())].rename(columns=display_cols).copy()
table_display["Delivery Date"] = table_display["Delivery Date"].dt.strftime("%d.%m.%Y")

def highlight_calc_rows(row):
    color = "background-color: #eaf4fb" if row.name < n_calc else ""
    return [color] * len(row)

st.dataframe(
    table_display.style
        .apply(highlight_calc_rows, axis=1)
        .format({
            "PLN / tonne": "{:.2f}",
            "PLN / km": "{:.2f}",
            "Distance (km)": "{:.2f}",
            "Tonnes": "{:.2f}",
            "ORLEN Diesel (PLN/L)": "{:.3f}",
        }, na_rep="—"),
    use_container_width=True,
    hide_index=True,
)

# ── Display: recommendation table (calc_df only) ─────────────────────────────

st.subheader("Maximum Spot Price(diesel-adjusted)")
st.caption(f"Based on the {n_calc} most recent deliveries within {radius_km} km.")

_rec_t_label  = f"Rec. PLN/t @ {fs_label} fuel"
_rec_km_label = f"Rec. PLN/km @ {fs_label} fuel"
rec_cols = {
    "Miejscowosc": "Address",
    "Towar": "Product",
    "Data dostawy": "Delivery Date",
    "diesel_pln_m3": "ORLEN Diesel on Date (PLN/L)",
    "PLN / t": "Past PLN / tonne",
    "rec_pln_t": _rec_t_label,
    "PLN / km": "Past PLN / km",
    "rec_pln_km": _rec_km_label,
}

rec_display = calc_df[list(rec_cols.keys())].rename(columns=rec_cols).copy()
rec_display["Delivery Date"] = rec_display["Delivery Date"].dt.strftime("%d.%m.%Y")

st.dataframe(
    rec_display.style.format({
        "ORLEN Diesel on Date (PLN/L)": "{:.3f}",
        "Past PLN / tonne": "{:.2f}",
        _rec_t_label: "{:.2f}",
        "Past PLN / km": "{:.2f}",
        _rec_km_label: "{:.2f}",
    }, na_rep="—"),
    use_container_width=True,
    hide_index=True,
)

# ── Final recommended range ───────────────────────────────────────────────────

st.subheader("Maximum spot price estimate")

valid_t  = calc_df.dropna(subset=["rec_pln_t"])
valid_km = calc_df.dropna(subset=["rec_pln_km"])

n_valid = min(len(valid_t), len(valid_km)) if not valid_t.empty and not valid_km.empty else max(len(valid_t), len(valid_km))

if n_valid >= 5:
    confidence_color = "green"
    confidence_text = f"High confidence — based on {n_calc} most recent deliveries within {radius_km} km"
elif n_valid >= 2:
    confidence_color = "orange"
    confidence_text = f"Moderate confidence — based on {n_calc} most recent deliveries within {radius_km} km"
else:
    confidence_color = "red"
    confidence_text = f"Low confidence — only {n_valid} {'delivery' if n_valid == 1 else 'deliveries'} had enough data within {radius_km} km"

st.markdown(
    f'<p style="color:{confidence_color}; font-weight:600;">ℹ️ {confidence_text}</p>',
    unsafe_allow_html=True,
)

col1, col2 = st.columns(2)

if not valid_t.empty:
    avg_t = valid_t["rec_pln_t"].mean()
    with col1:
        st.metric(
            "🏋️ Recommended freight — PLN / tonne",
            f"{avg_t:.2f} PLN/t",
        )
        st.caption(f"At {fs_label} fuel share")
else:
    with col1:
        st.warning("Not enough PLN/tonne data to calculate a recommendation.")

if not valid_km.empty:
    avg_km = valid_km["rec_pln_km"].mean()
    with col2:
        st.metric(
            "🛣️ Recommended freight — PLN / km",
            f"{avg_km:.2f} PLN/km",
        )
        st.caption(f"At {fs_label} fuel share")
else:
    with col2:
        st.warning("Not enough PLN/km data to calculate a recommendation.")

# ── Working out ───────────────────────────────────────────────────────────────

with st.expander("🔢 Show working out"):
    _today_str = f"{live_diesel_current:.3f} PLN/L" if live_diesel_current is not None else "unavailable"
    st.markdown(f"""
**Step 1 — Apply the fuel-adjustment formula to each of the {n_calc} most recent deliveries**

For each delivery:
$$P_{{rec}} = P_{{past}} \\times \\left(1 + f_{{fuel}} \\times \\frac{{D_{{today}} - D_{{past}}}}{{D_{{past}}}}\\right)$$

Today's ORLEN Ekodiesel price: **{_today_str}**
""")

    # Build working-out table for PLN/t
    if not valid_t.empty:
        st.markdown("**PLN / tonne — per-delivery breakdown**")
        wo_t = valid_t[["Miejscowosc", "Data dostawy", "diesel_pln_m3", "PLN / t", "rec_pln_t"]].copy()
        wo_t["Data dostawy"] = wo_t["Data dostawy"].dt.strftime("%d.%m.%Y")
        _d = wo_t["diesel_pln_m3"].replace(0, pd.NA)
        wo_t["Diesel Δ%"] = ((live_diesel_current - _d) / _d * 100).round(2) if live_diesel_current is not None else pd.NA
        _rec_col = f"Rec @ {fs_label}"
        wo_t = wo_t.rename(columns={
            "Miejscowosc": "Address",
            "Data dostawy": "Date",
            "diesel_pln_m3": "Diesel on Date (PLN/L)",
            "PLN / t": "Past PLN/t",
            "rec_pln_t": _rec_col,
        })
        st.dataframe(
            wo_t.style.format({
                "Diesel on Date (PLN/L)": "{:.3f}",
                "Past PLN/t": "{:.2f}",
                _rec_col: "{:.2f}",
                "Diesel Δ%": "{:+.2f}%",
            }, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )
        n_t = len(valid_t)
        sum_t = valid_t["rec_pln_t"].sum()
        st.markdown(f"""
**Step 2 — Average across {n_t} deliveries**

$$\\text{{Avg at {fs_label}}} = \\frac{{\\sum P_{{rec}}}}{{{n_t}}} = \\frac{{{sum_t:.2f}}}{{{n_t}}} = \\mathbf{{{avg_t:.2f}}} \\text{{ PLN/t}}$$

**Result: {avg_t:.2f} PLN/t**
""")

    st.divider()

    # Build working-out table for PLN/km
    if not valid_km.empty:
        st.markdown("**PLN / km — per-delivery breakdown**")
        wo_km = valid_km[["Miejscowosc", "Data dostawy", "diesel_pln_m3", "PLN / km", "rec_pln_km"]].copy()
        wo_km["Data dostawy"] = wo_km["Data dostawy"].dt.strftime("%d.%m.%Y")
        _dk = wo_km["diesel_pln_m3"].replace(0, pd.NA)
        wo_km["Diesel Δ%"] = ((live_diesel_current - _dk) / _dk * 100).round(2) if live_diesel_current is not None else pd.NA
        _rec_km_col = f"Rec @ {fs_label}"
        wo_km = wo_km.rename(columns={
            "Miejscowosc": "Address",
            "Data dostawy": "Date",
            "diesel_pln_m3": "Diesel on Date (PLN/L)",
            "PLN / km": "Past PLN/km",
            "rec_pln_km": _rec_km_col,
        })
        st.dataframe(
            wo_km.style.format({
                "Diesel on Date (PLN/L)": "{:.3f}",
                "Past PLN/km": "{:.2f}",
                _rec_km_col: "{:.2f}",
                "Diesel Δ%": "{:+.2f}%",
            }, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )
        n_km = len(valid_km)
        sum_km = valid_km["rec_pln_km"].sum()
        st.markdown(f"""
**Step 2 — Average across {n_km} deliveries**

$$\\text{{Avg at {fs_label}}} = \\frac{{\\sum P_{{rec}}}}{{{n_km}}} = \\frac{{{sum_km:.2f}}}{{{n_km}}} = \\mathbf{{{avg_km:.2f}}} \\text{{ PLN/km}}$$

**Result: {avg_km:.2f} PLN/km**
""")
