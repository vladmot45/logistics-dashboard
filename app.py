import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import plotly.express as px
import io
import requests
from datetime import date, timedelta
import gspread
from google.oauth2.service_account import Credentials

GEOCACHE_SHEET_ID  = "1pMdZMgYq2OaKDJCWko7WlIpfX75EDBq-xNB4H6V7ONo"
GEOCACHE_WORKSHEET = "geocache"
ORLEN_URL = "https://tool.orlen.pl/api/wholesalefuelprices/ByProduct"
ORLEN_HEADERS = {"Origin": "https://www.orlen.pl", "Referer": "https://www.orlen.pl/"}


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_orlen_prices(date_from: str, date_to: str) -> dict:
    try:
        resp = requests.get(
            ORLEN_URL,
            params={"productId": 43, "from": date_from, "to": date_to},
            headers=ORLEN_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return {}
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = (data.get("data") or data.get("items") or data.get("prices") or
                 data.get("result") or data.get("Results") or [])
    else:
        items = []
    DATE_FIELDS = ["date", "Date", "transactionDate", "priceDate", "validFrom", "from", "effectiveDate"]
    PRICE_FIELDS = ["price", "Price", "wholesalePrice", "wholesaleNetPrice", "netPrice",
                    "value", "Value", "amount", "Amount", "fuelPrice"]
    lookup = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_date = next((item[f] for f in DATE_FIELDS if f in item and item[f]), None)
        raw_price = next((item[f] for f in PRICE_FIELDS if f in item and item[f] is not None), None)
        if raw_date and raw_price is not None:
            try:
                lookup[str(raw_date)[:10]] = float(raw_price) / 1000
            except (TypeError, ValueError):
                pass
    return lookup


def orlen_on_date(target_date, lookup: dict):
    if not lookup:
        return None
    sorted_dates = sorted(lookup.keys())
    target_str = target_date.isoformat()
    if target_str in lookup:
        return lookup[target_str]
    before = [d for d in sorted_dates if d <= target_str]
    if before:
        return lookup[before[-1]]
    return lookup[sorted_dates[0]]

COUNTRY_MAP = {
    "Niemcy": "Germany", "Austria": "Austria", "Holandia": "Netherlands",
    "Polska": "Poland", "Słowacja": "Slovakia", "Dania": "Denmark",
    "Szwajcaria": "Switzerland", "Czechy": "Czech Republic",
    "Estonia": "Estonia", "Węgry": "Hungary", "Szwecja": "Sweden",
}

PIN_COLORS = [
    "blue", "green", "purple", "orange", "darkred", "darkblue",
    "darkgreen", "cadetblue", "darkpurple", "pink",
]


def get_sheet():
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        client = gspread.authorize(creds)
        return client.open_by_key(GEOCACHE_SHEET_ID).worksheet(GEOCACHE_WORKSHEET)
    except KeyError:
        st.error("Google Sheets credentials not found. Add [gcp_service_account] to your Streamlit secrets.")
        st.stop()
    except Exception as e:
        st.error(f"Could not connect to geocache Google Sheet: {e}")
        st.stop()


@st.cache_data(show_spinner=False, ttl=3600)
def load_geocache() -> dict:
    sheet = get_sheet()
    records = sheet.get_all_records()
    return {
        r["address"]: [float(r["lat"]), float(r["lon"])]
        for r in records
        if r["lat"] != "" and r["lon"] != ""
    }


def save_to_geocache(address: str, lat: float, lon: float):
    try:
        sheet = get_sheet()
        sheet.append_row([address, lat, lon])
    except Exception as e:
        st.warning(f"Could not save geocode result to sheet: {e}")


def clean_address(raw_addr):
    import re
    result = raw_addr.strip()
    for pl, en in COUNTRY_MAP.items():
        result = result.replace(pl, en)
    result = re.sub(r"\s{2,}", " ", result)
    result = re.sub(r"(?<!\s),(?!\s)", ", ", result)
    return result


def fallback_queries(raw_addr):
    import re
    base = clean_address(raw_addr)
    queries = [base]
    no_street = re.sub(r"^[^,]+,\s*", "", base).strip()
    if no_street and no_street != base:
        queries.append(no_street)
    parts = [p.strip() for p in base.split(",") if p.strip()]
    if len(parts) >= 2:
        queries.append(", ".join(parts[-2:]))
    return queries


def geocode_addresses(addresses, cache):
    new_addresses = [a for a in addresses if a not in cache]
    if not new_addresses:
        return cache

    geolocator = Nominatim(user_agent="delivery_map_dap_trucks_v2")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1, error_wait_seconds=5)

    bar = st.progress(0, text="Geocoding new addresses…")
    for i, addr in enumerate(new_addresses):
        result = None
        for query in fallback_queries(addr):
            try:
                loc = geocode(query)
                if loc:
                    result = [loc.latitude, loc.longitude]
                    break
            except Exception:
                pass
        cache[addr] = result  # Fix 2: update in-memory dict immediately
        if result:
            save_to_geocache(addr, result[0], result[1])
        bar.progress((i + 1) / len(new_addresses), text=f"Geocoding {i+1}/{len(new_addresses)}…")

    load_geocache.clear()  # Fix 3: invalidate so next session reads fresh from Sheets
    bar.empty()
    return cache


# ── Shared file loader (cached on raw bytes so it survives page switches) ─────

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

@st.cache_data(show_spinner="Reading file…")
def load_data(file_bytes: bytes):
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0)
    if "Features.PricePerKM.Waluta" in df.columns:
        df = df[df["Features.PricePerKM.Waluta"].astype(str).str.strip().str.upper() != "EUR"]
    if "Features.KM" in df.columns:
        df = df[pd.to_numeric(df["Features.KM"], errors="coerce").fillna(0) != 0]
    df = df.rename(columns=COLUMN_MAP)
    df["Data dostawy"] = pd.to_datetime(df["Data dostawy"], errors="coerce")
    df = df.dropna(subset=["Data dostawy", "Miejscowosc"])
    df["Miejscowosc"] = df["Miejscowosc"].str.strip()
    return df


# ── UI ────────────────────────────────────────────────────────────────────────

st.title("🚛 DAP Trucks — Delivery Map")
st.caption("Upload your Excel file to visualise delivery destinations.")

# ── File upload with session_state persistence ────────────────────────────────

new_upload = st.file_uploader("📂 Upload Excel file (.xlsx)", type=["xlsx"])

if new_upload is not None:
    st.session_state["file_bytes"] = new_upload.read()
    st.session_state["file_name"] = new_upload.name

if "file_bytes" not in st.session_state or st.session_state["file_bytes"] is None:
    st.info("Upload an Excel file to get started. The app reads the first sheet and expects the standard DAP Trucks column format.")
    st.stop()

if new_upload is None:
    st.caption(f"Using previously uploaded file: **{st.session_state.get('file_name', 'unknown')}**")

# ── Load data ─────────────────────────────────────────────────────────────────

df = load_data(st.session_state["file_bytes"])

# ── Geocode ───────────────────────────────────────────────────────────────────

unique_addrs = df["Miejscowosc"].unique().tolist()

# Fix 4: use the in-memory cache from session_state when available so filter
# reruns never trigger geocoding — only load from Sheets on first run.
if "geocache" not in st.session_state:
    st.session_state["geocache"] = load_geocache()
cache = st.session_state["geocache"]

if not all(a in cache for a in unique_addrs):
    cache = geocode_addresses(unique_addrs, cache)
    st.session_state["geocache"] = cache

def get_lat(addr):
    c = cache.get(addr)
    return c[0] if c else None

def get_lon(addr):
    c = cache.get(addr)
    return c[1] if c else None

df["lat"] = df["Miejscowosc"].map(get_lat)
df["lon"] = df["Miejscowosc"].map(get_lon)
df = df.dropna(subset=["lat", "lon"])

# ── Sidebar filters ───────────────────────────────────────────────────────────

st.sidebar.header("🔍 Filters")
st.sidebar.caption("Leave a filter empty to show all.")

# Compute bounds first so the reset callback can reference them
_km_series = pd.to_numeric(df["KM"], errors="coerce").dropna()
if _km_series.empty:
    km_min, km_max = 0, 0
else:
    km_min, km_max = int(_km_series.min()), int(_km_series.max())
if km_min == km_max:
    km_max = km_min + 1

_addr_totals = df.groupby("Miejscowosc")["Tony"].sum()
if _addr_totals.empty or _addr_totals.isna().all():
    tonnes_min, tonnes_max = 0.0, 0.0
else:
    tonnes_min = float(_addr_totals.min())
    tonnes_max = float(_addr_totals.max())
if tonnes_min == tonnes_max:
    tonnes_max = tonnes_min + 1.0

date_min = df["Data dostawy"].min().date()
from datetime import date as _date_cls
_date_today = _date_cls.today()

def _reset_filters():
    st.session_state["f_countries"]    = []
    st.session_state["f_clients"]      = []
    st.session_state["f_transport"]    = []
    st.session_state["f_products"]     = []
    st.session_state["f_destinations"] = []
    st.session_state["f_dates"]        = (date_min, _date_today)
    st.session_state["km_lo"]          = km_min
    st.session_state["km_hi"]          = km_max
    st.session_state["t_lo"]           = tonnes_min
    st.session_state["t_hi"]           = tonnes_max

st.sidebar.button("🔁 Reset Filters", on_click=_reset_filters, use_container_width=True)
st.sidebar.divider()

countries = sorted(df["Kraj"].dropna().unique())
sel_countries = st.sidebar.multiselect("🌍 Country", countries, key="f_countries")

clients = sorted(df["Klient"].dropna().unique())
sel_clients = st.sidebar.multiselect("🏢 Client", clients, key="f_clients")

transport_cos = sorted(df["Features.TransportKontrahent:Kod"].dropna().unique())
sel_transport = st.sidebar.multiselect("🚛 Transport Company", transport_cos, key="f_transport")

PRODUCT_ALIASES = {
    "Olej sojowy surowy odgumowany, materiał paszowy, bez GMO":      "SBO",
    "Śruta poekstrakcyjna paszowa z nasion soi BEZ GMO 46%":         "SBM46",
    "Śruta poekstrakcyjna paszowa z nasion soi BEZ GMO 48%":         "SBM48",
    "Łuska sojowa granulowana, materiał paszowy, bez GMO":           "SBH",
}
# Reverse map: display label → original column value
_alias_to_orig = {v: k for k, v in PRODUCT_ALIASES.items()}

all_products = df["Towar"].dropna().unique().tolist()
# Aliased products first (sorted by alias), then the rest alphabetically
_aliased   = sorted([p for p in all_products if p in PRODUCT_ALIASES],
                    key=lambda p: PRODUCT_ALIASES[p])
_remaining = sorted([p for p in all_products if p not in PRODUCT_ALIASES])
_product_options = [PRODUCT_ALIASES.get(p, p) for p in _aliased + _remaining]

sel_products_display = st.sidebar.multiselect("📦 Product", _product_options, key="f_products")
# Translate display labels back to original values for filtering
sel_products = [_alias_to_orig.get(p, p) for p in sel_products_display]

destinations = sorted(df["Miejscowosc"].dropna().unique())
sel_destinations = st.sidebar.multiselect("📍 Destination", destinations, key="f_destinations")

# Initialise session state for KM range
if "km_lo" not in st.session_state: st.session_state["km_lo"] = km_min
if "km_hi" not in st.session_state: st.session_state["km_hi"] = km_max
# Clamp stored values to current data bounds (in case a new file is loaded)
st.session_state["km_lo"] = max(km_min, min(st.session_state["km_lo"], km_max))
st.session_state["km_hi"] = max(km_min, min(st.session_state["km_hi"], km_max))

st.sidebar.markdown("**📏 Distance (KM)**")
_km_c1, _km_c2 = st.sidebar.columns(2)
_km_c1.number_input("Min KM", min_value=km_min, max_value=km_max,
    step=1, key="km_lo",
    on_change=lambda: st.session_state.update(
        km_hi=max(st.session_state["km_lo"], st.session_state["km_hi"])
    ))
_km_c2.number_input("Max KM", min_value=km_min, max_value=km_max,
    step=1, key="km_hi",
    on_change=lambda: st.session_state.update(
        km_lo=min(st.session_state["km_lo"], st.session_state["km_hi"])
    ))
st.sidebar.slider("KM range", km_min, km_max,
    (st.session_state["km_lo"], st.session_state["km_hi"]),
    label_visibility="collapsed",
    on_change=lambda: st.session_state.update(
        km_lo=st.session_state["_km_sl"][0], km_hi=st.session_state["_km_sl"][1]
    ), key="_km_sl")
sel_km = (st.session_state["km_lo"], st.session_state["km_hi"])

# Initialise session state for tonnes range
if "t_lo" not in st.session_state: st.session_state["t_lo"] = tonnes_min
if "t_hi" not in st.session_state: st.session_state["t_hi"] = tonnes_max
st.session_state["t_lo"] = max(tonnes_min, min(st.session_state["t_lo"], tonnes_max))
st.session_state["t_hi"] = max(tonnes_min, min(st.session_state["t_hi"], tonnes_max))

st.sidebar.markdown("**⚖️ Total Tonnes per Destination**")
_t_c1, _t_c2 = st.sidebar.columns(2)
_t_c1.number_input("Min t", min_value=tonnes_min, max_value=tonnes_max,
    step=1.0, format="%.1f", key="t_lo",
    on_change=lambda: st.session_state.update(
        t_hi=max(st.session_state["t_lo"], st.session_state["t_hi"])
    ))
_t_c2.number_input("Max t", min_value=tonnes_min, max_value=tonnes_max,
    step=1.0, format="%.1f", key="t_hi",
    on_change=lambda: st.session_state.update(
        t_lo=min(st.session_state["t_lo"], st.session_state["t_hi"])
    ))
st.sidebar.slider("Tonnes range", tonnes_min, tonnes_max,
    (st.session_state["t_lo"], st.session_state["t_hi"]),
    label_visibility="collapsed",
    on_change=lambda: st.session_state.update(
        t_lo=st.session_state["_t_sl"][0], t_hi=st.session_state["_t_sl"][1]
    ), key="_t_sl")
sel_tonnes = (st.session_state["t_lo"], st.session_state["t_hi"])

# Default date range: min from sheet → today
if "f_dates" not in st.session_state:
    st.session_state["f_dates"] = (date_min, _date_today)
sel_dates = st.sidebar.date_input("📅 Delivery Date Range", key="f_dates")

st.sidebar.divider()
map_height = st.sidebar.slider("🗺️ Map Height (px)", 400, 1000, 560, step=50)

st.sidebar.divider()
if st.sidebar.button("🔄 Re-geocode all addresses", help="Clears the geocoding cache sheet and retries all addresses. Use this if pins are missing."):
    try:
        sheet = get_sheet()
        # Keep header row (row 1), delete all data rows
        row_count = len(sheet.get_all_values())
        if row_count > 1:
            sheet.delete_rows(2, row_count)
    except Exception as e:
        st.sidebar.error(f"Could not clear geocache sheet: {e}")
    st.session_state.pop("geocache", None)  # drop in-memory cache so it reloads clean
    load_geocache.clear()
    st.rerun()

# ── Apply filters ─────────────────────────────────────────────────────────────

filtered = df.copy()
if sel_countries:
    filtered = filtered[filtered["Kraj"].isin(sel_countries)]
if sel_clients:
    filtered = filtered[filtered["Klient"].isin(sel_clients)]
if sel_transport:
    filtered = filtered[filtered["Features.TransportKontrahent:Kod"].isin(sel_transport)]
if sel_products:
    filtered = filtered[filtered["Towar"].isin(sel_products)]
if sel_destinations:
    filtered = filtered[filtered["Miejscowosc"].isin(sel_destinations)]
filtered = filtered[(filtered["KM"] >= sel_km[0]) & (filtered["KM"] <= sel_km[1])]
if isinstance(sel_dates, (list, tuple)) and len(sel_dates) == 2:
    filtered = filtered[
        (filtered["Data dostawy"].dt.date >= sel_dates[0]) &
        (filtered["Data dostawy"].dt.date <= sel_dates[1])
    ]
elif isinstance(sel_dates, (list, tuple)) and len(sel_dates) == 1:
    st.sidebar.warning("Pick an end date to apply the date filter.")

# Apply total-tonnes-per-destination filter after all other filters
_filtered_totals = filtered.groupby("Miejscowosc")["Tony"].sum()
_valid_addrs = _filtered_totals[
    (_filtered_totals >= sel_tonnes[0]) & (_filtered_totals <= sel_tonnes[1])
].index
filtered = filtered[filtered["Miejscowosc"].isin(_valid_addrs)]

# ── Aggregate to one row per address ─────────────────────────────────────────

addr_groups = (
    filtered.groupby("Miejscowosc")
    .agg(
        lat=("lat", "first"),
        lon=("lon", "first"),
        kraj=("Kraj", "first"),
        clients=("Klient", lambda x: ", ".join(sorted(set(x.dropna())))),
        products=("Towar", lambda x: "<br>• ".join(sorted(set(x.dropna())))),
        date_first=("Data dostawy", "min"),
        date_last=("Data dostawy", "max"),
        km=("KM", "first"),
        avg_pln_t=("PLN / t", "mean"),
        avg_pln_km=("PLN / km", "mean"),
        delivery_count=("Towar", "count"),
    )
    .reset_index()
)

n_pins = len(addr_groups)

# ── Pin colour logic ──────────────────────────────────────────────────────────

if n_pins == 0:
    color_map = {}
elif n_pins <= 10:
    color_map = {
        row["Miejscowosc"]: PIN_COLORS[i % len(PIN_COLORS)]
        for i, row in addr_groups.iterrows()
    }
else:
    color_map = {row["Miejscowosc"]: "red" for _, row in addr_groups.iterrows()}

# ── Summary metrics ───────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric("📍 Addresses", n_pins)
c2.metric("📦 Deliveries", len(filtered))
c3.metric("🌍 Countries", filtered["Kraj"].nunique() if len(filtered) else 0)
c4.metric("🚛 Transport Cos", filtered["Features.TransportKontrahent:Kod"].nunique() if len(filtered) else 0)

# ── Fuel price summary (date filter only) ────────────────────────────────────
if isinstance(sel_dates, (list, tuple)) and len(sel_dates) == 2:
    _fuel_d_min, _fuel_d_max = sel_dates[0], sel_dates[1]
else:
    _fuel_d_min, _fuel_d_max = date_min, _date_today

_orlen = fetch_orlen_prices(
    (_fuel_d_min - timedelta(days=7)).isoformat(),
    _fuel_d_max.isoformat(),
)
# Build a per-day lookup across the selected date range
_all_days = pd.date_range(_fuel_d_min, _fuel_d_max, freq="D").date
_diesel_by_day = [(d, orlen_on_date(d, _orlen)) for d in _all_days]
_diesel_by_day = [(d, p) for d, p in _diesel_by_day if p is not None]

st.markdown("**⛽ ORLEN Ekodiesel (PLN/L) — for selected date range**")
f1, f2, f3 = st.columns(3)
if _diesel_by_day:
    _prices = [p for _, p in _diesel_by_day]
    _min_d, _min_p = min(_diesel_by_day, key=lambda x: x[1])
    _max_d, _max_p = max(_diesel_by_day, key=lambda x: x[1])
    _avg_p = sum(_prices) / len(_prices)
    f1.metric("🔽 Min", f"{_min_p:.3f}", delta=_min_d.strftime("%d.%m.%Y"), delta_color="off")
    f2.metric("🔼 Max", f"{_max_p:.3f}", delta=_max_d.strftime("%d.%m.%Y"), delta_color="off")
    f3.metric("⚖️ Average", f"{_avg_p:.3f}")
else:
    f1.metric("🔽 Min", "—")
    f2.metric("🔼 Max", "—")
    f3.metric("⚖️ Average", "—")
    st.caption("ORLEN price data unavailable for these dates.")

# ── Map ───────────────────────────────────────────────────────────────────────

m = folium.Map(location=[51.5, 12.0], zoom_start=5, tiles="CartoDB positron")

for _, row in addr_groups.iterrows():
    addr = row["Miejscowosc"]
    d_first = row["date_first"].strftime("%d.%m.%Y") if pd.notna(row["date_first"]) else "—"
    d_last  = row["date_last"].strftime("%d.%m.%Y")  if pd.notna(row["date_last"])  else "—"
    date_str = d_first if d_first == d_last else f"{d_first} – {d_last}"

    # Per-address deliveries for table + stats
    addr_rows = (
        filtered[filtered["Miejscowosc"] == addr]
        .sort_values("Data dostawy", ascending=False)
    )

    # Min / Max rows for PLN/t
    t_valid = addr_rows.dropna(subset=["PLN / t"])
    if not t_valid.empty:
        min_t_r = t_valid.loc[t_valid["PLN / t"].idxmin()]
        max_t_r = t_valid.loc[t_valid["PLN / t"].idxmax()]
        avg_t   = t_valid["PLN / t"].mean()
        stats_t = f"""
        <tr><td style="padding:2px 6px;color:#555">Min PLN/t</td>
            <td style="padding:2px 6px;font-weight:600">{min_t_r['PLN / t']:.2f}</td>
            <td style="padding:2px 6px;color:#555">{min_t_r['Data dostawy'].strftime('%d.%m.%Y')} · {min_t_r['Features.TransportKontrahent:Kod']}</td></tr>
        <tr><td style="padding:2px 6px;color:#555">Max PLN/t</td>
            <td style="padding:2px 6px;font-weight:600">{max_t_r['PLN / t']:.2f}</td>
            <td style="padding:2px 6px;color:#555">{max_t_r['Data dostawy'].strftime('%d.%m.%Y')} · {max_t_r['Features.TransportKontrahent:Kod']}</td></tr>
        <tr><td style="padding:2px 6px;color:#555">Avg PLN/t</td>
            <td style="padding:2px 6px;font-weight:600">{avg_t:.2f}</td><td></td></tr>"""
    else:
        stats_t = "<tr><td colspan='3' style='color:#aaa;padding:2px 6px'>No PLN/t data</td></tr>"

    km_valid = addr_rows.dropna(subset=["PLN / km"])
    if not km_valid.empty:
        min_km_r = km_valid.loc[km_valid["PLN / km"].idxmin()]
        max_km_r = km_valid.loc[km_valid["PLN / km"].idxmax()]
        avg_km   = km_valid["PLN / km"].mean()
        stats_km = f"""
        <tr><td style="padding:2px 6px;color:#555">Min PLN/km</td>
            <td style="padding:2px 6px;font-weight:600">{min_km_r['PLN / km']:.2f}</td>
            <td style="padding:2px 6px;color:#555">{min_km_r['Data dostawy'].strftime('%d.%m.%Y')} · {min_km_r['Features.TransportKontrahent:Kod']}</td></tr>
        <tr><td style="padding:2px 6px;color:#555">Max PLN/km</td>
            <td style="padding:2px 6px;font-weight:600">{max_km_r['PLN / km']:.2f}</td>
            <td style="padding:2px 6px;color:#555">{max_km_r['Data dostawy'].strftime('%d.%m.%Y')} · {max_km_r['Features.TransportKontrahent:Kod']}</td></tr>
        <tr><td style="padding:2px 6px;color:#555">Avg PLN/km</td>
            <td style="padding:2px 6px;font-weight:600">{avg_km:.2f}</td><td></td></tr>"""
    else:
        stats_km = "<tr><td colspan='3' style='color:#aaa;padding:2px 6px'>No PLN/km data</td></tr>"

    # Delivery rows table
    delivery_rows_html = ""
    for _, dr in addr_rows.iterrows():
        delivery_rows_html += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
            <td style="padding:3px 6px">{dr['Data dostawy'].strftime('%d.%m.%Y')}</td>
            <td style="padding:3px 6px;text-align:right">{dr['PLN / t']:.2f}</td>
            <td style="padding:3px 6px;text-align:right">{dr['PLN / km']:.2f}</td>
            <td style="padding:3px 6px">{dr['Features.TransportKontrahent:Kod']}</td>
            <td style="padding:3px 6px;text-align:right">{dr['Tony']:.2f}</td>
        </tr>"""

    popup_html = f"""
    <div style="font-family:Arial,sans-serif;font-size:12px;width:580px;line-height:1.5">
        <div style="font-weight:bold;font-size:13px;margin-bottom:4px">📍 {addr[:80]}</div>
        <div style="color:#555;margin-bottom:2px"><b>Client(s):</b> {row['clients']}</div>
        <div style="color:#555;margin-bottom:6px"><b>Product(s):</b> {row['products'].replace('<br>• ', ', ').lstrip('• ')}</div>
        <div style="color:#555;margin-bottom:6px"><b>Deliveries:</b> {row['delivery_count']} &nbsp;|&nbsp;
            <b>Dates:</b> {date_str} &nbsp;|&nbsp; <b>Distance:</b> {row['km']:.0f} km</div>
        <hr style="margin:4px 0;border-color:#ddd">
        <b style="font-size:11px;text-transform:uppercase;color:#888">Summary</b>
        <table style="width:100%;border-collapse:collapse;margin:4px 0 8px 0">
            {stats_t}
            <tr><td colspan="3" style="padding:2px 0"></td></tr>
            {stats_km}
        </table>
        <hr style="margin:4px 0;border-color:#ddd">
        <b style="font-size:11px;text-transform:uppercase;color:#888">All Deliveries</b>
        <div style="max-height:180px;overflow-y:auto;margin-top:4px">
        <table style="width:100%;border-collapse:collapse;font-size:11px">
            <thead>
                <tr style="background:#f5f5f5;position:sticky;top:0">
                    <th style="padding:3px 6px;text-align:left">Date</th>
                    <th style="padding:3px 6px;text-align:right">PLN/t</th>
                    <th style="padding:3px 6px;text-align:right">PLN/km</th>
                    <th style="padding:3px 6px;text-align:left">Transport Co.</th>
                    <th style="padding:3px 6px;text-align:right">Tonnes</th>
                </tr>
            </thead>
            <tbody>{delivery_rows_html}</tbody>
        </table>
        </div>
    </div>
    """
    folium.Marker(
        location=[row["lat"], row["lon"]],
        popup=folium.Popup(popup_html, max_width=620),
        tooltip=f"{addr[:50]} — {row['delivery_count']} deliveries",
        icon=folium.Icon(color=color_map.get(addr, "red"), icon="circle", prefix="fa"),
    ).add_to(m)

st_folium(m, use_container_width=True, height=map_height, returned_objects=[])

# ── Bar charts ────────────────────────────────────────────────────────────────

st.subheader("📊 Price Analysis")

_PRICE_GROUP_LABELS = {
    "Kraj":                              "Country",
    "Features.TransportKontrahent:Kod":  "Transport Company",
    "Klient":                            "Client",
    "Miejscowosc":                       "Delivery Address",
}

pc1, pc2 = st.columns(2)
with pc1:
    metric_toggle = st.radio(
        "Metric",
        options=["PLN / t", "PLN / km"],
        format_func=lambda x: "Average price per tonne (PLN/t)" if x == "PLN / t" else "Average price per km (PLN/km)",
        horizontal=True,
        label_visibility="collapsed",
    )
with pc2:
    price_group = st.radio(
        "Group by",
        options=list(_PRICE_GROUP_LABELS.keys()),
        format_func=lambda x: _PRICE_GROUP_LABELS[x],
        horizontal=True,
        key="price_group",
    )

metric_label = "Avg PLN / tonne" if metric_toggle == "PLN / t" else "Avg PLN / km"
price_group_label = _PRICE_GROUP_LABELS[price_group]

if len(filtered) == 0:
    st.warning("No data matches the current filters.")
else:
    price_chart = (
        filtered.groupby(price_group)[metric_toggle]
        .mean()
        .reset_index()
        .rename(columns={price_group: price_group_label, metric_toggle: metric_label})
        .sort_values(metric_label, ascending=False)
    )
    fig_price = px.bar(
        price_chart,
        x=price_group_label,
        y=metric_label,
        color=price_group_label,
        text_auto=".2f",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_price.update_layout(showlegend=False, plot_bgcolor="white", yaxis_gridcolor="#eee")
    fig_price.update_traces(textposition="outside")
    st.plotly_chart(fig_price, use_container_width=True)

# ── Tonnes bar chart ──────────────────────────────────────────────────────────

st.subheader("⚖️ Total Tonnes")

_GROUP_LABELS = {
    "Kraj": "Country",
    "Features.TransportKontrahent:Kod": "Transport Company",
    "Klient": "Client",
    "Miejscowosc": "Delivery Address",
}

tonnes_group = st.radio(
    "Group by",
    options=list(_GROUP_LABELS.keys()),
    format_func=lambda x: _GROUP_LABELS[x],
    horizontal=True,
    key="tonnes_group",
)

group_label = _GROUP_LABELS[tonnes_group]

if len(filtered) == 0:
    st.warning("No data matches the current filters.")
else:
    tonnes_chart = (
        filtered.groupby(tonnes_group)["Tony"]
        .sum()
        .reset_index()
        .rename(columns={tonnes_group: group_label, "Tony": "Total Tonnes"})
        .sort_values("Total Tonnes", ascending=False)
    )
    fig_tonnes = px.bar(
        tonnes_chart,
        x=group_label,
        y="Total Tonnes",
        color=group_label,
        text_auto=".2f",
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig_tonnes.update_layout(showlegend=False, plot_bgcolor="white", yaxis_gridcolor="#eee")
    fig_tonnes.update_traces(textposition="outside")
    st.plotly_chart(fig_tonnes, use_container_width=True)

# ── All deliveries table ──────────────────────────────────────────────────────

st.subheader("📋 All Deliveries")

if len(filtered) == 0:
    st.warning("No data matches the current filters.")
else:
    # Attach ORLEN diesel price per delivery date (reuse already-fetched _orlen lookup)
    _tbl = filtered.copy().sort_values("Data dostawy", ascending=False).reset_index(drop=True)
    _tbl["ORLEN Diesel (PLN/L)"] = _tbl["Data dostawy"].apply(
        lambda d: orlen_on_date(d.date(), _orlen) if pd.notna(d) else None
    )

    _tbl_display = _tbl.rename(columns={
        "Miejscowosc":                      "Address",
        "Towar":                            "Product",
        "Data dostawy":                     "Delivery Date",
        "PLN / t":                          "PLN / tonne",
        "PLN / km":                         "PLN / km",
        "KM":                               "Distance (km)",
        "Features.TransportKontrahent:Kod": "Transport Companies",
        "Tony":                             "Tonnes",
    })[[
        "Address", "Product", "Delivery Date",
        "PLN / tonne", "PLN / km", "Distance (km)",
        "Transport Companies", "Tonnes", "ORLEN Diesel (PLN/L)",
    ]].copy()
    _tbl_display["Delivery Date"] = _tbl_display["Delivery Date"].dt.strftime("%d.%m.%Y")

    # ── Min / Max / Avg summary cards ────────────────────────────────────────
    _s = _tbl.copy()  # use pre-rename numeric columns for clean aggregation
    _s_pln_t  = _s["PLN / t"].dropna()
    _s_pln_km = _s["PLN / km"].dropna()
    _s_tonnes = _s["Tony"].dropna()
    _s_diesel = _s["ORLEN Diesel (PLN/L)"].dropna()

    st.markdown("**📊 Summary**")
    _sc1, _sc2, _sc3, _sc4, _sc5 = st.columns(5)

    def _stat_md(series, fmt):
        if series.empty:
            return "—", "—", "—"
        return format(series.min(), fmt), format(series.max(), fmt), format(series.mean(), fmt)

    _t_min, _t_max, _t_avg   = _stat_md(_s_pln_t,  ".2f")
    _km_min, _km_max, _km_avg = _stat_md(_s_pln_km, ".2f")
    _ton_min, _ton_max, _ton_avg = _stat_md(_s_tonnes, ".2f")
    _d_min2, _d_max2, _d_avg2  = _stat_md(_s_diesel, ".3f")

    _sc1.markdown("**PLN / tonne**")
    _sc1.markdown(f"🔽 Min: **{_t_min}**  \n🔼 Max: **{_t_max}**  \n⚖️ Avg: **{_t_avg}**")

    _sc2.markdown("**PLN / km**")
    _sc2.markdown(f"🔽 Min: **{_km_min}**  \n🔼 Max: **{_km_max}**  \n⚖️ Avg: **{_km_avg}**")

    _sc3.markdown("**Tonnes**")
    _sc3.markdown(f"🔽 Min: **{_ton_min}**  \n🔼 Max: **{_ton_max}**  \n⚖️ Avg: **{_ton_avg}**")

    _sc4.markdown("**ORLEN Diesel (PLN/L)**")
    _sc4.markdown(f"🔽 Min: **{_d_min2}**  \n🔼 Max: **{_d_max2}**  \n⚖️ Avg: **{_d_avg2}**")

    _sc5.markdown("**Deliveries**")
    _sc5.markdown(f"📦 Total: **{len(_tbl)}**")

    st.divider()

    st.dataframe(
        _tbl_display.style.format({
            "PLN / tonne":          "{:.2f}",
            "PLN / km":             "{:.2f}",
            "Distance (km)":        "{:.0f}",
            "Tonnes":               "{:.2f}",
            "ORLEN Diesel (PLN/L)": "{:.3f}",
        }, na_rep="—"),
        use_container_width=True,
        hide_index=True,
    )
