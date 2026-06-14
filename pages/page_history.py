
import asyncio
from typing import List, Dict

import pandas as pd
import streamlit as st

from database import DatabaseManager


# ===== CHANGE: Simplified _run() to use asyncio.run() =====
def _run(coro):

    return asyncio.run(coro)


def render_history_page():
    st.title("📋 History & Results")
    st.caption("Browse sourcing runs, suppliers, and scraped products.")
    st.divider()

    db = DatabaseManager()

    # ── Global stats ─────────────────────────────────────────
    stats = _run(db.get_stats())
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Suppliers",   stats.get("fournisseurs", 0))
    c2.metric("With API / CM",     stats.get("fournisseurs_connectables", 0))
    c3.metric("Raw Products",      stats.get("produits_raw", 0))
    c4.metric("Normalized",        stats.get("produits_normalises", 0))
    c5.metric("Inserted in Odoo",  stats.get("matches_accepted", 0))

    st.divider()

    # ── Suppliers table ───────────────────────────────────────
    st.subheader("🏢 Discovered Suppliers")

    fournisseurs = _run(db.get_all_fournisseurs(limit=500))
    if not fournisseurs:
        st.info("No suppliers yet. Launch a sourcing run from the form.")
        return

    df_sup = pd.DataFrame(fournisseurs)[
        ["id", "nom", "ville", "domain", "score",
         "status", "has_api", "has_channel", "type_connexion"]
    ]
    df_sup.columns = [
        "ID", "Name", "City", "Domain", "Score",
        "Status", "API", "CM", "Connection",
    ]

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.multiselect(
            "Filter by status",
            options=["discovered", "qualified", "scraping_pending", "scraped", "rejected"],
            default=[],
            key="sup_status_filter",
        )
    with col2:
        connection_filter = st.multiselect(
            "Filter by connection type",
            options=["API", "CHANNEL", "BOTH", "NONE"],
            default=[],
            key="sup_conn_filter",
        )

    df_filtered = df_sup.copy()
    if status_filter:
        df_filtered = df_filtered[df_filtered["Status"].isin(status_filter)]
    if connection_filter:
        df_filtered = df_filtered[df_filtered["Connection"].isin(connection_filter)]

    st.dataframe(
        df_filtered,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=100
            ),
            "Domain": st.column_config.LinkColumn("Domain"),
        },
    )
    st.caption(f"Showing {len(df_filtered)} of {len(df_sup)} suppliers")

    st.divider()

    # ── Products table ────────────────────────────────────────
    st.subheader("🎫 Scraped Products")

    products = _run(db.get_produits(limit=1000))
    if not products:
        st.info("No products scraped yet.")
        return

    df_prod = pd.DataFrame(products)

    # Show relevant columns
    cols = [c for c in
            ["id", "nom_produit", "ville", "pays", "prix",
             "devise", "categorie", "source", "created_at"]
            if c in df_prod.columns]
    df_prod = df_prod[cols]
    df_prod.columns = [
        c.replace("_", " ").title() for c in cols
    ]

    # Search
    search = st.text_input(
        "Search products",
        placeholder="Search by name, city...",
        key="prod_search",
    )
    if search:
        mask = df_prod.apply(
            lambda row: row.astype(str).str.contains(search, case=False).any(),
            axis=1,
        )
        df_prod = df_prod[mask]

    st.dataframe(
        df_prod,
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing {len(df_prod)} products")

    st.divider()
    st.subheader("🔍 Matching & Odoo Insertion")
