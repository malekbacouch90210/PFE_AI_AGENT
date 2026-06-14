"""
pages/page_validation.py — Human Validation Page
DEX AI Sourcing Agent — PFE 2026

Shows NEW_PRODUCT matches waiting for review.
Manager clicks Accept → product status becomes APPROVED → pushed to Odoo.
Manager clicks Reject → product status becomes REJECTED → archived.
"""

import asyncio
import sys

import streamlit as st
from loguru import logger

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from database import DatabaseManager
from sqlalchemy import text


# ─────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────

def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


async def load_pending_products(db: DatabaseManager, run_id: str = None) -> list:
    """Load NEW_PRODUCT matches not yet validated."""
    async with db.engine.connect() as conn:
        where_run = "AND pm.run_id = :run_id" if run_id else ""
        params = {"run_id": run_id} if run_id else {}
        result = await conn.execute(text(f"""
            SELECT
                pm.id                          AS match_id,
                pm.scraped_product_id,
                pm.similarity_score,
                pm.ai_reason,
                pm.run_id,
                spn.clean_title                AS title,
                spn.clean_description          AS description,
                spn.normalized_city            AS city,
                spn.normalized_country         AS country,
                spn.canonical_activity_type    AS activity_type,
                spn.normalized_category        AS category,
                spn.estimated_price            AS price,
                spn.price_is_estimated,
                f.nom                          AS supplier_name,
                f.domain                       AS supplier_domain
            FROM produit_matches pm
            JOIN scraped_products_normalized spn
                ON spn.scraped_product_id = pm.scraped_product_id
            LEFT JOIN fournisseurs f
                ON f.id = spn.fournisseur_id
            WHERE pm.match_status = 'NEW_PRODUCT'
              AND pm.validation_decision IS NULL
              {where_run}
            ORDER BY pm.created_at DESC
            LIMIT 200
        """), params)
        return [dict(r._mapping) for r in result.fetchall()]


async def set_validation_decision(db: DatabaseManager, match_id: int, decision: str):
    """Set APPROVED or REJECTED on produit_matches."""
    async with db.engine.begin() as conn:
        await conn.execute(text("""
            UPDATE produit_matches
            SET validation_decision = :decision,
                validated_at        = NOW()
            WHERE id = :mid
        """), {"decision": decision, "mid": match_id})


async def push_to_odoo(db: DatabaseManager, match_id: int) -> bool:
    """Push a single APPROVED product to Odoo immediately."""
    try:
        from services.odoo.odoo_client import odoo_client
        from services.odoo.odoo_sync import build_product_vals

        # Load the full product row
        async with db.engine.connect() as conn:
            result = await conn.execute(text("""
                SELECT
                    spn.scraped_product_id,
                    spn.fournisseur_id,
                    spn.run_id,
                    spn.clean_title,
                    spn.clean_description,
                    spn.normalized_city,
                    spn.normalized_country,
                    spn.canonical_activity_type,
                    spn.normalized_category,
                    spn.estimated_price,
                    spn.price_is_estimated,
                    spn.duration_minutes,
                    spn.image_url,
                    pm.similarity_score,
                    pm.id AS match_id,
                    f.nom    AS supplier_name,
                    f.domain AS supplier_domain,
                    p.url_source,
                    p.source
                FROM produit_matches pm
                JOIN scraped_products_normalized spn
                    ON spn.scraped_product_id = pm.scraped_product_id
                LEFT JOIN produits p
                    ON p.id = spn.scraped_product_id
                LEFT JOIN fournisseurs f
                    ON f.id = spn.fournisseur_id
                WHERE pm.id = :mid
            """), {"mid": match_id})
            row = result.fetchone()
            if not row:
                return False
            row_dict = dict(row._mapping)

        vals = build_product_vals(row_dict)

        # Authenticate + create
        await odoo_client.authenticate()
        odoo_id = await odoo_client.create_product(vals)

        if odoo_id:
            # Save odoo_product_id + mark match_status = ACCEPTED (integrated)
            async with db.engine.begin() as conn:
                await conn.execute(text("""
                    UPDATE produit_matches
                    SET odoo_product_id = :oid,
                        match_status    = 'ACCEPTED'
                    WHERE id = :mid
                """), {"oid": odoo_id, "mid": match_id})
            logger.info(f"Odoo push OK: match_id={match_id} odoo_id={odoo_id}")
            return True

    except Exception as e:
        logger.error(f"push_to_odoo error match_id={match_id}: {e}")
    return False


async def load_validation_stats(db: DatabaseManager) -> dict:
    """Summary counts for the dashboard."""
    async with db.engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT
                COUNT(*) FILTER (
                    WHERE match_status = 'NEW_PRODUCT'
                    AND validation_decision IS NULL
                )                                AS pending,
                COUNT(*) FILTER (
                    WHERE validation_decision = 'APPROVED'
                )                                AS approved,
                COUNT(*) FILTER (
                    WHERE validation_decision = 'REJECTED'
                )                                AS rejected,
                COUNT(*) FILTER (
                    WHERE match_status = 'SIMILAR'
                )                                AS similar,
                COUNT(*) FILTER (
                    WHERE match_status = 'ACCEPTED'
                )                                AS integrated
            FROM produit_matches
        """))
        row = result.fetchone()
        if row:
            return {
                "pending":    row[0] or 0,
                "approved":   row[1] or 0,
                "rejected":   row[2] or 0,
                "similar":    row[3] or 0,
                "integrated": row[4] or 0,
            }
    return {"pending": 0, "approved": 0, "rejected": 0, "similar": 0, "integrated": 0}


# ─────────────────────────────────────────────────────────────
# Main render
# ─────────────────────────────────────────────────────────────

def render_validation_page():
    st.title("✅ Product Validation")
    st.markdown("Review new products discovered by the sourcing pipeline. Accept to push to Odoo, reject to archive.")

    db = DatabaseManager()

    # ── Stats strip ──────────────────────────────────────────
    stats = run_async(load_validation_stats(db))

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Pending Review", stats["pending"])
    col2.metric("Approved", stats["approved"], delta=None)
    col3.metric("Rejected", stats["rejected"])
    col4.metric("Similar (auto-excluded)", stats["similar"])
    col5.metric("Integrated in Odoo", stats["integrated"])

    st.markdown("---")

    # ── Filter bar ───────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns([2, 2, 1])
    with col_f1:
        run_filter = st.text_input("Filter by Run ID (optional)", value="", key="val_run_filter")
    with col_f2:
        atype_filter = st.selectbox(
            "Filter by Activity Type",
            ["All", "EXCURSION", "TICKET", "TRANSFER"],
            key="val_atype_filter",
        )
    with col_f3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Refresh", key="val_refresh"):
            st.rerun()

    # ── Load pending products ────────────────────────────────
    products = run_async(load_pending_products(db, run_filter.strip() or None))

    # Apply activity type filter client-side
    if atype_filter != "All":
        products = [p for p in products if p.get("activity_type") == atype_filter]

    if not products:
        st.info("No products pending validation. Run the matching pipeline first.")
        return

    st.markdown(f"**{len(products)} product(s) pending review**")
    st.markdown("---")

    # ── Bulk actions ─────────────────────────────────────────
    col_b1, col_b2, _ = st.columns([2, 2, 4])
    with col_b1:
        if st.button("Accept All Visible", type="primary", key="val_accept_all"):
            progress = st.progress(0)
            for i, prod in enumerate(products):
                run_async(set_validation_decision(db, prod["match_id"], "APPROVED"))
                ok = run_async(push_to_odoo(db, prod["match_id"]))
                progress.progress((i + 1) / len(products))
            st.success(f"Accepted and pushed {len(products)} products to Odoo.")
            st.rerun()
    with col_b2:
        if st.button("Reject All Visible", key="val_reject_all"):
            for prod in products:
                run_async(set_validation_decision(db, prod["match_id"], "REJECTED"))
            st.warning(f"Rejected {len(products)} products.")
            st.rerun()

    st.markdown("---")

    # ── Product cards ────────────────────────────────────────
    for prod in products:
        match_id = prod["match_id"]
        title    = prod.get("title") or "Untitled"
        city     = prod.get("city") or "Unknown city"
        country  = prod.get("country") or ""
        atype    = prod.get("activity_type") or "EXCURSION"
        category = prod.get("category") or ""
        price    = prod.get("price")
        sim      = prod.get("similarity_score") or 0.0
        reason   = prod.get("ai_reason") or ""
        supplier = prod.get("supplier_name") or prod.get("supplier_domain") or "Unknown"
        desc     = (prod.get("description") or "")[:300]
        is_est   = prod.get("price_is_estimated")

        # Card container
        with st.container(border=True):
            head_col, btn_col = st.columns([7, 3])

            with head_col:
                # Activity type badge color
                badge_color = {
                    "EXCURSION": "#1a7abf",
                    "TICKET":    "#c47a1e",
                    "TRANSFER":  "#1e8c45",
                }.get(atype, "#666")

                st.markdown(
                    f'<span style="background:{badge_color};color:white;padding:2px 8px;'
                    f'border-radius:4px;font-size:12px;font-weight:600;">{atype}</span> '
                    f'<span style="background:#eee;padding:2px 8px;border-radius:4px;'
                    f'font-size:12px;">{category}</span>',
                    unsafe_allow_html=True,
                )
                st.markdown(f"### {title}")
                st.markdown(
                    f"📍 **{city}, {country}**  |  "
                    f"🏢 *{supplier}*  |  "
                    f"Similarity score: `{sim:.2f}`"
                )

                if price:
                    est_label = " *(estimated)*" if is_est else ""
                    st.markdown(f"💶 **{price:.2f} €**{est_label}")
                else:
                    st.markdown("💶 *Price not available*")

                if desc:
                    with st.expander("Description"):
                        st.write(desc)



            with btn_col:
                st.markdown("<br><br>", unsafe_allow_html=True)

                if st.button(
                    "✅ Accept",
                    key=f"accept_{match_id}",
                    type="primary",
                    use_container_width=True,
                ):
                    run_async(set_validation_decision(db, match_id, "APPROVED"))
                    with st.spinner("Pushing to Odoo..."):
                        ok = run_async(push_to_odoo(db, match_id))
                    if ok:
                        st.success("Accepted and pushed to Odoo!")
                    else:
                        st.warning("Accepted but Odoo push failed. Check Odoo connection.")
                    st.rerun()

                if st.button(
                    "❌ Reject",
                    key=f"reject_{match_id}",
                    use_container_width=True,
                ):
                    run_async(set_validation_decision(db, match_id, "REJECTED"))
                    st.rerun()

        st.markdown("")  # spacer between cards