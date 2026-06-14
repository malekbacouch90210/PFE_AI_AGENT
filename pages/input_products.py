"""
Form 2 — Product Scraping + Immediate Normalization Pipeline
(Fixed & Production‑ready with run_id isolation)
"""

import streamlit as st
import asyncio
from datetime import datetime
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import DatabaseManager
from scrapers.product_scraper import ProductScraper, RunScopedNormalizationPipeline
from services.geo.city_fetcher import get_cities_for_country
from utils.country_data import get_all_countries
from models import ScrapingRun


def _run(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def show():
    st.title("🎫 Form 2 — Scraping des Produits + Normalisation")
    st.markdown("---")
    db = DatabaseManager()

    # ═══════════════════════════════════════════════════════════
    # SECTION 1 — COUNTRY
    # ═══════════════════════════════════════════════════════════
    st.subheader("🌍 Sélection du Pays")
    all_countries = sorted(get_all_countries())
    selected_countries = st.multiselect(
        "Choisissez un ou plusieurs pays",
        options=all_countries,
        default=[],
        key="f2_countries",
        placeholder="Sélectionnez un pays...",
    )
    st.markdown("---")

    # ═══════════════════════════════════════════════════════════
    # SECTION 2 — CITIES (empty by default)
    # ═══════════════════════════════════════════════════════════
    st.subheader("🏙️ Sélection des Villes")

    if selected_countries:
        if st.button("🔍 Charger les villes", type="primary", use_container_width=True):
            with st.spinner("Chargement depuis la base..."):
                all_cities_by_country = {}
                for country in selected_countries:
                    cities = _run(get_cities_for_country(country))
                    all_cities_by_country[country] = cities
                st.session_state["f2_all_cities"] = all_cities_by_country
                total = sum(len(c) for c in all_cities_by_country.values())
                st.success(f"✅ {total} villes chargées")

    selected_cities = {}
    total_cities_selected = 0

    if "f2_all_cities" in st.session_state:
        for country, cities in st.session_state["f2_all_cities"].items():
            if cities:
                chosen = st.multiselect(
                    f"Villes de {country} ({len(cities)} disponibles)",
                    options=cities,
                    default=[],   # empty by default
                    key=f"cities_{country}",
                )
                selected_cities[country] = chosen
                total_cities_selected += len(chosen)
            else:
                st.warning(f"Aucune ville trouvée pour {country}")
    else:
        st.info("👆 Sélectionnez un pays, puis chargez les villes")

    st.markdown("---")

    # ═══════════════════════════════════════════════════════════
    # SECTION 3 — SUPPLIERS
    # ═══════════════════════════════════════════════════════════
    st.subheader("🏢 Fournisseurs (depuis la base)")

    if selected_countries:
        if st.button("🔍 Charger les fournisseurs", type="primary", use_container_width=True):
            with st.spinner("Chargement..."):
                all_suppliers = _run(db.get_all_fournisseurs(limit=5000))
                filtered = []
                for s in all_suppliers:
                    sup_pays = (s.get("pays") or "").lower()
                    for country in selected_countries:
                        if (
                            country.lower() in sup_pays
                            or sup_pays in country.lower()
                        ):
                            filtered.append(s)
                            break
                st.session_state["f2_suppliers"] = filtered
                with_domain = sum(1 for s in filtered if s.get("domain"))
                st.success(
                    f"✅ {len(filtered)} fournisseurs "
                    f"({with_domain} avec domaine)"
                )

    suppliers = st.session_state.get("f2_suppliers", [])
    if suppliers:
        with_domain = sum(1 for s in suppliers if s.get("domain"))
        st.info(f"✅ {len(suppliers)} fournisseurs prêts ({with_domain} avec domaine web)")
    else:
        st.warning("⚠️ Chargez les fournisseurs")

    st.markdown("---")

    # Activity types
    st.subheader("🎯 Types d'activité")
    c1, c2, c3 = st.columns(3)
    activity_types = []
    with c1:
        if st.checkbox("🏔️ Excursion", value=True):
            activity_types.append("excursion")
    with c2:
        if st.checkbox("🎫 Ticket", value=True):
            activity_types.append("ticket")
    with c3:
        if st.checkbox("🚐 Transfert", value=True):
            activity_types.append("transfer")

    # Options
    st.subheader("⚙️ Options")
    max_per_supplier = st.slider(
        "Maximum de produits par fournisseur",
        min_value=5, max_value=100, value=25, step=5,
    )

    st.markdown("---")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🌍 Pays", len(selected_countries))
    m2.metric("🏙️ Villes", total_cities_selected)
    m3.metric("🏢 Fournisseurs", len(suppliers))
    m4.metric("🎯 Activités", len(activity_types))

    if st.button("🚀 LANCER SCRAPING + NORMALISATION", type="primary", use_container_width=True):
        if not selected_countries:
            st.error("❌ Veuillez sélectionner au moins un pays")
            return
        if total_cities_selected == 0:
            st.error("❌ Veuillez sélectionner au moins une ville")
            return
        if not suppliers:
            st.error("❌ Veuillez charger les fournisseurs")
            return
        if not activity_types:
            st.error("❌ Veuillez sélectionner au moins un type d'activité")
            return

        _run_pipeline(
            countries=selected_countries,
            selected_cities=selected_cities,
            activity_types=activity_types,
            suppliers=suppliers,
            max_per_supplier=max_per_supplier,
        )


def _run_pipeline(countries, selected_cities, activity_types, suppliers, max_per_supplier):
    progress = st.progress(0, "Initialisation...")
    status = st.empty()

    async def _do():
        db = DatabaseManager()
        await db.create_tables()
        await db.migrate_produits_table()

        run_id = str(uuid.uuid4())
        all_cities = []
        for country, cities in selected_cities.items():
            all_cities.extend(cities)

        # Create ScrapingRun entry
        async with db.AsyncSessionLocal() as session:
            from models import ScrapingRun
            run = ScrapingRun(
                run_id=run_id,
                requested_country=countries[0] if countries else "Unknown",
                requested_cities=all_cities,
                activity_types=activity_types,
                status="RUNNING",
            )
            session.add(run)
            await session.commit()

        scraper = ProductScraper()
        norm_pipeline = RunScopedNormalizationPipeline()

        try:
            if not all_cities:
                return {"scraped": 0, "normalized": 0}

            # ── Step 1: Scraping ─────────────────────────────────
            progress.progress(5, f"🔍 Scraping {len(all_cities)} villes...")
            status.info(f"Run ID: {run_id}")

            result = await scraper.scrape_from_form(
                country=countries[0] if countries else "Unknown",
                cities=all_cities,
                activity_types=activity_types,
                suppliers=suppliers,
                max_per_supplier=max_per_supplier,
                run_id=run_id,
            )
            total_saved = result.get("saved", 0)
            run_product_ids = await db.get_product_ids_by_run_id(run_id)

            if total_saved == 0 or not run_product_ids:
                status.warning("⚠️ Aucun produit scrapé!")
                await db.update_scraping_run(run_id, {"status": "FAILED"})
                return {"scraped": 0, "normalized": 0}

            status.success(f"✅ {total_saved} produits scrapés")
            progress.progress(40, f"✅ {total_saved} produits scrapés")

            # ── Step 2: Direct normalization (NO staging) ────────
            progress.progress(45, "🧠 Normalisation directe (pas de staging)...")
            norm_stats = await norm_pipeline.process_run(
                run_product_ids=run_product_ids
                # NO city_country_map, NO default_country override
            )
            total_normalized = norm_stats.get("processed", 0)

            await db.update_scraping_run(run_id, {
                "total_products": total_saved,
                "total_staged": 0,            # staging deleted
                "total_normalized": total_normalized,
                "status": "DONE",
                "finished_at": datetime.utcnow(),
            })

            progress.progress(100, "✅ Terminé!")
            return {"scraped": total_saved, "normalized": total_normalized}

        finally:
            await scraper.close()
            await norm_pipeline.close()

    try:
        with st.spinner("Pipeline en cours..."):
            stats = _run(_do())
        st.success(
            f"""
            **✅ Pipeline terminé!**

            | Étape | Résultat |
            |-------|----------|
            | 📦 Produits scrapés | **{stats['scraped']}** |
            | 🧠 Produits normalisés | **{stats['normalized']}** |
            """
        )
        if stats["normalized"] > 0:
            st.balloons()
    except Exception as e:
        st.error(f"❌ Erreur pipeline: {e}")
        st.exception(e)