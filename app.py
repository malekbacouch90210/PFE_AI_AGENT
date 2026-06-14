"""
app.py — DEX AI Sourcing Agent — FINAL
No sidebar. DEX pink/black navbar. 3 pages (no cron page).
Dark/light mode toggle.

Run: streamlit run app.py
"""
import asyncio
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import streamlit as st

st.set_page_config(
    page_title="DEX AI Sourcing",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

if "page"      not in st.session_state: st.session_state.page      = "sourcing"
if "dark_mode" not in st.session_state: st.session_state.dark_mode = True

dark  = st.session_state.dark_mode
BG    = "#0D0D1A" if dark else "#F5F5F5"
CARD  = "#16213E" if dark else "#FFFFFF"
TEXT  = "#EAEAEA" if dark else "#1A1A2E"
MUTED = "#888888" if dark else "#666666"
BORDER= "#2A2A4A" if dark else "#E0E0E0"

st.markdown(f"""
<style>
[data-testid="collapsedControl"], section[data-testid="stSidebar"] {{ display:none!important; }}
.stApp {{ background:{BG}!important; }}
.block-container {{ padding-top:0!important; max-width:100%!important; }}
*, p, label, .stMarkdown {{ color:{TEXT}!important; }}
.stButton > button[kind="primary"] {{
    background:#E8004D!important; border:none!important;
    border-radius:8px!important; font-weight:700!important; color:#fff!important;
}}
.stButton > button[kind="primary"]:hover {{ background:#c4003e!important; }}
[data-testid="stMetric"] {{
    background:{CARD}; border-radius:10px; padding:12px;
    border:1px solid {BORDER};
}}
[data-testid="stMetricLabel"] p {{ color:{MUTED}!important; font-size:12px!important; }}
hr {{ border-color:{BORDER}!important; }}
.stTextInput input, .stSelectbox select, .stNumberInput input {{
    background:{CARD}!important; color:{TEXT}!important;
    border:1px solid {BORDER}!important; border-radius:8px!important;
}}
.dex-brand {{ color:#E8004D!important; font-size:22px; font-weight:900;
              letter-spacing:1px; }}
</style>
""", unsafe_allow_html=True)


def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed(): raise RuntimeError
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


@st.cache_data(ttl=15)
def get_pending_count():
    try:
        from database import DatabaseManager
        from sqlalchemy import text
        async def _q():
            db = DatabaseManager()
            async with db.engine.connect() as c:
                r = await c.execute(text(
                    "SELECT COUNT(*) FROM produit_matches "
                    "WHERE match_status='NEW_PRODUCT' "
                    "AND validation_decision IS NULL"
                ))
                return r.scalar() or 0
        return run_async(_q())
    except:
        return 0


# ── Navbar ────────────────────────────────────────────────────
pending   = get_pending_count()
mode_icon = "☀️" if dark else "🌙"

st.markdown(
    '<span class="dex-brand">⚡ DEX AI Sourcing</span>',
    unsafe_allow_html=True,
)

nc = st.columns([1.4, 1.4, 1.4, 0.4, 3])
nav_items = [
    ("sourcing",   "🚀 Sourcing Run"),
    ("validation", f"✅ Validation ({pending})" if pending else "✅ Validation"),
    ("history",    "📋 History"),
]
for i, (key, label) in enumerate(nav_items):
    t = "primary" if st.session_state.page == key else "secondary"
    if nc[i].button(label, key=f"nb_{key}", type=t, use_container_width=True):
        st.session_state.page = key
        st.rerun()
if nc[3].button(mode_icon, key="dark_toggle", use_container_width=True):
    st.session_state.dark_mode = not dark
    st.rerun()

st.divider()

# ── Router ────────────────────────────────────────────────────
page = st.session_state.page

if page == "sourcing":
    from pages.page_form import render_form_page
    render_form_page()

elif page == "validation":
    from pages.page_validation import render_validation_page
    render_validation_page()

elif page == "history":
    from pages.page_history import render_history_page
    render_history_page()

st.divider()
