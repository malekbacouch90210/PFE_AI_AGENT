"""
Form 1 — Recherche de Fournisseurs (Min/Max Control)
"""

import streamlit as st
import asyncio
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import DatabaseManager
from scrapers.fournisseur_scraper import FournisseurScraper
from utils.country_data import ZONE_NAMES, ZONE_COLORS, COUNTRIES_BY_ZONE, get_all_countries


def _run(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def show():
    st.title("📝 Form 1 — Recherche de Fournisseurs")
    st.markdown(
        "Sélectionnez zones/pays et types d'activité. "
        "**Tous les fournisseurs tourismes sont collectés (API ou non).**"
    )
    st.markdown("---")

    # ── Zone selection ───────────────────────────────────────
    st.subheader("🌍 Sélection par Zone")
    zone_cols = st.columns(5)
    selected_zones = []
    for i, (zid, zname) in enumerate(ZONE_NAMES.items()):
        with zone_cols[i]:
            if st.checkbox(
                f"Zone {zid} — {zname}",
                key=f"zone_{zid}",
                help=f"Tous les pays de la zone {zname}"
            ):
                selected_zones.append(zid)

    # ── Country selection ────────────────────────────────────
    st.subheader("🗺️ Sélection par Pays")
    col_s, col_m = st.columns([3, 1])
    with col_s:
        search = st.text_input("🔍 Rechercher", placeholder="ex: Morocco, France...")

    all_countries = get_all_countries()
    zone_countries = []
    for zid in selected_zones:
        zone_countries.extend(COUNTRIES_BY_ZONE.get(zid, []))

    filtered = [c for c in all_countries if search.lower() in c.lower()] if search else all_countries

    selected_countries = st.multiselect(
        "Pays (1 à 195)",
        options=sorted(set(filtered)),
        default=sorted(set(zone_countries)),
    )
    final_countries = sorted(set(zone_countries) | set(selected_countries))

    with col_m:
        st.metric("Pays", len(final_countries))

    # ── Activity types (only excursion, ticket, transfer) ──
    st.subheader("🎯 Types d'activité")
    col1, col2, col3 = st.columns(3)
    activity_types = []
    with col1:
        if st.checkbox("🏔️ Excursion", value=True):
            activity_types.append("excursion")
    with col2:
        if st.checkbox("🎫 Ticket", value=True):
            activity_types.append("ticket")
    with col3:
        if st.checkbox("🚐 Transfert", value=True):
            activity_types.append("transfer")

    # ── Options (MIN/MAX supplier control) ──
    st.subheader("⚙️ Options")
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        min_suppliers = st.slider(
            "🎯 Minimum fournisseurs",
            min_value=10, max_value=500, value=40, step=10,
            help="Le scraping s'arrête dès que ce nombre est atteint"
        )
    with col_b:
        max_suppliers = st.slider(
            "🛑 Maximum fournisseurs",
            min_value=50, max_value=2000, value=500, step=50,
            help="Ne pas dépasser ce nombre"
        )
    with col_c:
        skip_existing = st.toggle("⛔ Ignorer domaines déjà en base", value=True)

    # ── Info box ──────────────────────────────────────────────
    st.info(
        "🔍 Logique MODIFIÉE :\n"
        "1. Recherche saisonnière (printemps/été/automne/hiver)\n"
        "2. Tous les fournisseurs tourismes sont collectés\n"
        "3. Le pays (`pays`) et la ville (`ville`) sont renseignés automatiquement\n"
        "4. Conservation des vrais opérateurs même sans API\n"
        "5. Contrôle du nombre de fournisseurs (min/max)"
    )

    # ── Summary ──────────────────────────────────────────────
    st.markdown("---")
    s1, s2, s3 = st.columns(3)
    s1.metric("🌍 Pays", len(final_countries))
    s2.metric("🎯 Activités", len(activity_types))
    s3.metric("📊 Jobs estimés", len(final_countries) * len(activity_types) * 4)

    # ── Launch ───────────────────────────────────────────────
    if st.button("🚀 LANCER LA RECHERCHE", type="primary", width="stretch"):
        if not final_countries:
            st.error("❌ Sélectionnez au moins un pays ou une zone")
            return
        if not activity_types:
            st.error("❌ Sélectionnez au moins un type d'activité")
            return

        _run_scraping(
            countries=final_countries[:50],
            activity_types=activity_types,
            skip_existing=skip_existing,
            min_suppliers=min_suppliers,
            max_suppliers=max_suppliers,
        )


def _run_scraping(countries, activity_types, skip_existing, min_suppliers, max_suppliers):
    progress = st.progress(0, "Recherche des fournisseurs...")
    log_box = st.empty()
    logs = []

    def log(msg):
        logs.append(msg)
        log_box.info("\n".join(logs[-6:]))

    async def _do():
        db = DatabaseManager()
        scraper = FournisseurScraper()
        try:
            existing = await db.get_existing_domains() if skip_existing else []
            log(f"✅ {len(existing)} domaines déjà en base (ignorés)")
            progress.progress(10, "🔎 Recherche des fournisseurs...")

            results = await scraper.scrape_from_form(
                countries, activity_types, existing,
                min_suppliers=min_suppliers,
                max_suppliers=max_suppliers,
            )
            log(f"✅ {len(results)} fournisseurs trouvés")
            progress.progress(70, f"💾 Sauvegarde de {len(results)} fournisseurs...")

            saved = 0
            for i, f in enumerate(results):
                await db.save_fournisseur(f)
                saved += 1

                if (i + 1) % 20 == 0:
                    pct = 70 + int(25 * (i + 1) / max(len(results), 1))
                    progress.progress(pct, f"💾 {saved}/{len(results)} sauvegardés...")

            progress.progress(100, "✅ Terminé!")
            log(f"✅ {saved} fournisseurs sauvegardés en base")

            with_domain = sum(1 for f in results if f.get("domain"))
            without_domain = sum(1 for f in results if not f.get("domain"))
            api_count = sum(1 for f in results if f.get("has_api"))
            cm_count = sum(1 for f in results if f.get("has_channel"))

            log(f"📊 Détails: {with_domain} avec site web, {without_domain} sans site web")
            log(f"📊 {api_count} avec API, {cm_count} avec Channel Manager")

            return results
        finally:
            await scraper.close()

    try:
        with st.spinner(""):
            results = _run(_do())

        st.success(f"✅ {len(results)} fournisseurs trouvés et sauvegardés!")

        if results:
            df = pd.DataFrame(results)

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total", len(results))

            with_domain = sum(1 for r in results if r.get("domain"))
            without_domain = sum(1 for r in results if not r.get("domain"))

            c2.metric("🌐 Avec site web", with_domain)
            c3.metric("📱 Sans site web", without_domain)

            cm_count = sum(1 for r in results if r.get("has_channel"))
            api_count = sum(1 for r in results if r.get("has_api"))

            c4.metric("📡 Channel Manager", cm_count)
            c5.metric("🔌 API Direct", api_count)

            if "channel_name" in df.columns:
                cm_df = df[df["channel_name"].notna()]["channel_name"].value_counts()
                if not cm_df.empty:
                    st.subheader("📊 Channel Managers détectés")
                    st.bar_chart(cm_df)

            show_cols = [c for c in [
                "nom", "pays", "ville", "zone", "activite_type", "source",
                "has_api", "has_channel", "channel_name",
                "type_connexion", "domain", "mapping_raison", "rating", "reviews"
            ] if c in df.columns]
            st.dataframe(df[show_cols], use_container_width=True)

            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Télécharger CSV",
                csv,
                f"fournisseurs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv",
            )

    except Exception as e:
        st.error(f"❌ Erreur: {e}")
        st.exception(e)