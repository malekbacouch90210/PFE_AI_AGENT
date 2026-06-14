"""
pages/page_form.py — FINAL


Changes from original:
  - REMOVED: required cities, recall old suppliers, activity types sections
  - ADDED: APScheduler cron section (start time, end time, interval)
  - ADDED: Live phase progress during execution
  - ADDED: Full pipeline connected (scraping → norm → matching → validation)
  - ADDED: Scraping run saved to DB (history card)
"""
import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, List

import pytz
import streamlit as st
from loguru import logger

from database import DatabaseManager
from services.ai_agent_pipeline import run_post_scraping_pipeline

TZ_PARIS = pytz.timezone("Europe/Paris")


def _run(coro):
    import asyncio as _a
    _a.set_event_loop_policy(_a.WindowsSelectorEventLoopPolicy())
    loop = _a.new_event_loop()
    _a.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@st.cache_data(ttl=3600)
def _load_zones_and_countries():
    db = DatabaseManager()
    zones     = _run(db.get_zones())
    countries = _run(db.get_all_countries())
    return zones, countries


# ── Cron re-run thread ────────────────────────────────────────
import threading
_stop_event   = threading.Event()
_cron_thread  = None


def _cron_loop(form_config: dict, interval_min: int,
               end_dt: datetime, run_number_start: int):
    """Background thread: repeat pipeline every interval_min until end_dt."""
    import time
    run_num = run_number_start
    logger.info(
        f"[Cron] Loop started | every {interval_min}min "
        f"until {end_dt.strftime('%H:%M')} CET"
    )
    while not _stop_event.is_set():
        now_paris = datetime.now(tz=TZ_PARIS)
        if now_paris >= end_dt:
            logger.info("[Cron] End time reached. Stopping.")
            break

        # Wait for next interval
        next_run = datetime.now() + timedelta(minutes=interval_min)
        while datetime.now() < next_run and not _stop_event.is_set():
            if datetime.now(tz=TZ_PARIS) >= end_dt:
                break
            time.sleep(10)

        if _stop_event.is_set() or datetime.now(tz=TZ_PARIS) >= end_dt:
            break

        run_id = f"cron_{run_num}_{int(time.time())}"
        logger.info(f"[Cron] Re-run #{run_num} | run_id={run_id}")

        try:
            results   = run_post_scraping_pipeline(run_id, limit=500)
            new_prods = results.get("new_products", 0)
            # Save to DB
            _save_run_card(run_id, form_config,
                           status="COMPLETE", new_products=new_prods)
            _add_cron_history(run_id, run_num, results)
            logger.info(
                f"[Cron] Re-run #{run_num} done | "
                f"new_products={new_prods}"
            )
        except Exception as e:
            logger.error(f"[Cron] Re-run #{run_num} error: {e}")
            _save_run_card(run_id, form_config, status="FAILED")

        run_num += 1

    st.session_state["cron_running"] = False
    logger.info("[Cron] Loop finished.")


def _save_run_card(run_id: str, config: dict,
                   status: str = "COMPLETE",
                   suppliers: int = 0, products: int = 0,
                   new_products: int = 0):
    """Persist run to scraping_runs table."""
    try:
        from sqlalchemy import text
        async def _ins():
            db = DatabaseManager()
            async with db.engine.begin() as conn:
                await conn.execute(text("""
                    ALTER TABLE scraping_runs
                    ADD COLUMN IF NOT EXISTS new_products_found INTEGER DEFAULT 0
                """))
                await conn.execute(text("""
                    INSERT INTO scraping_runs
                      (run_id, countries, activity_types,
                       min_suppliers, max_suppliers, max_products_per_supplier,
                       status, started_at, finished_at,
                       total_suppliers_found, total_products_scraped,
                       new_products_found)
                    VALUES
                      (:run_id,:countries,:atypes,:min_s,:max_s,:max_p,
                       :status,NOW(),NOW(),:sup,:prod,:new_p)
                    ON CONFLICT (run_id) DO UPDATE
                      SET status               = EXCLUDED.status,
                          finished_at          = NOW(),
                          total_suppliers_found= EXCLUDED.total_suppliers_found,
                          total_products_scraped=EXCLUDED.total_products_scraped,
                          new_products_found   = EXCLUDED.new_products_found
                """), {
                    "run_id":    run_id,
                    "countries": str(config.get("countries", [])),
                    "atypes":    "excursion,ticket,transfer",
                    "min_s":     config.get("min_suppliers", 5),
                    "max_s":     config.get("max_suppliers", 20),
                    "max_p":     config.get("max_products", 20),
                    "status":    status,
                    "sup":       suppliers,
                    "prod":      products,
                    "new_p":     new_products,
                })
        _run(_ins())
    except Exception as e:
        logger.warning(f"[Cron] Could not save run card: {e}")


def _add_cron_history(run_id, run_num, results):
    try:
        if "cron_history" not in st.session_state:
            st.session_state.cron_history = []
        st.session_state.cron_history.insert(0, {
            "run_id":       run_id,
            "run_number":   run_num,
            "new_products": results.get("new_products", 0),
            "elapsed":      results.get("elapsed", 0),
            "time":         datetime.now().strftime("%H:%M:%S"),
        })
        st.session_state.cron_history = st.session_state.cron_history[:20]
    except Exception:
        pass


# ── Main page ─────────────────────────────────────────────────
def render_form_page():
    st.markdown("## 🌍 DEX AI Sourcing Run")
    st.markdown("Configure your sourcing campaign and schedule.")

    zones, countries = _load_zones_and_countries()
    zone_map          = {z["nom"]: z["id"] for z in zones}
    all_country_names = sorted([c["nom"] for c in countries])

    # ── Section 1 — Countries ────────────────────────────────
    st.markdown("### Section 1 — Countries to source")
    c1, c2 = st.columns([1, 2])
    with c1:
        selection_mode = st.radio(
            "Mode", ["By Zone", "Manual selection"], key="sel_mode"
        )
    selected_countries: List[str] = []
    with c2:
        if selection_mode == "By Zone":
            selected_zones = st.multiselect(
                "Select zones",
                options=[z["nom"] for z in zones],
                key="sel_zones",
            )
            if selected_zones:
                zone_ids = {zone_map[z] for z in selected_zones}
                selected_countries = [
                    c["nom"] for c in countries if c["zone_id"] in zone_ids
                ]
                st.info(f"✅ {len(selected_countries)} countries from {len(selected_zones)} zone(s)")
        else:
            selected_countries = st.multiselect(
                "Select countries",
                options=all_country_names,
                key="sel_countries",
            )
    if not selected_countries:
        st.warning("⚠️ Select at least one country or zone.")
        return

    st.divider()

    # ── Section 2 — Supplier limits ──────────────────────────
    st.markdown("### Section 2 — Supplier limits")
    st.caption(
        f"⚠️ Min/Max apply **per country**. "
        f"{len(selected_countries)} countries × max = total suppliers."
    )
    c1, c2 = st.columns(2)
    with c1:
        min_suppliers = st.number_input(
            "Min suppliers per country", min_value=1, value=5, step=1
        )
    with c2:
        max_suppliers = st.number_input(
            "Max suppliers per country", min_value=1, value=30, step=5
        )
    if min_suppliers > max_suppliers:
        st.error("❌ Min must be ≤ Max")
        return
    st.info(
        f"📊 Estimated: "
        f"{int(min_suppliers)*len(selected_countries)} – "
        f"{int(max_suppliers)*len(selected_countries)} suppliers total"
    )

    st.divider()

    # ── Section 3 — Products ─────────────────────────────────
    st.markdown("### Section 3 — Product settings")
    max_products = st.slider("Max products per supplier", 1, 50, 20)

    st.divider()

    # ── Section 4 — Schedule (APScheduler) ───────────────────
    st.markdown("### Section 4 — Schedule (APScheduler)")
    st.markdown(
        "Set a start time and end time. The pipeline will start exactly "
        "at the start time and re-run every N minutes until the end time. "
        "Leave **disabled** to run immediately once."
    )

    use_schedule = st.toggle("Enable Schedule", value=False, key="use_schedule")

    if use_schedule:
        now_paris  = datetime.now(tz=TZ_PARIS)
        sc1, sc2, sc3 = st.columns(3)

        with sc1:
            start_h = st.number_input(
                "Start hour (CET)",   0, 23,
                value=now_paris.hour,
                key="sched_start_h"
            )
            start_m = st.number_input(
                "Start minute", 0, 59,
                value=((now_paris.minute // 5) + 1) * 5 % 60,
                step=5, key="sched_start_m"
            )

        with sc2:
            end_h = st.number_input(
                "End hour (CET)", 0, 23,
                value=(now_paris.hour + 1) % 24,
                key="sched_end_h"
            )
            end_m = st.number_input(
                "End minute", 0, 59,
                value=now_paris.minute,
                step=5, key="sched_end_m"
            )

        with sc3:
            interval = st.selectbox(
                "Re-run every (min)",
                [5, 10, 15, 20, 30, 45, 60],
                index=3,  # default 20 min
                key="sched_interval_sel",
            )
            # Preview
            s_dt = TZ_PARIS.localize(datetime.combine(
                now_paris.date(),
                datetime.min.time()
            ).replace(hour=int(start_h), minute=int(start_m)))
            e_dt = TZ_PARIS.localize(datetime.combine(
                now_paris.date(),
                datetime.min.time()
            ).replace(hour=int(end_h), minute=int(end_m)))
            if e_dt <= s_dt:
                e_dt += timedelta(days=1)
            total_mins = int((e_dt - s_dt).total_seconds() / 60)
            runs_n     = max(1, total_mins // int(interval) + 1)

            st.markdown(
                f'<div style="background:#E8004D22;border:1px solid #E8004D44;'
                f'border-radius:8px;padding:10px;text-align:center;">'
                f'<b style="color:#E8004D">⏱ Schedule Preview</b><br>'
                f'<span style="font-size:13px">'
                f'{int(start_h):02d}:{int(start_m):02d} → '
                f'{int(end_h):02d}:{int(end_m):02d} CET</span><br>'
                f'<span style="font-size:12px;color:#888">every {interval} min</span><br>'
                f'<b style="color:#E8004D">≈ {runs_n} runs</b>'
                f'</div>',
                unsafe_allow_html=True,
            )

        # Countdown to start
        if s_dt > now_paris:
            remaining = int((s_dt - now_paris).total_seconds())
            st.info(
                f"⏳ Launch will wait until "
                f"**{int(start_h):02d}:{int(start_m):02d} CET** "
                f"({remaining // 60}m {remaining % 60}s from now)"
            )
    else:
        start_h = end_h = start_m = end_m = interval = None
        s_dt = e_dt = None

    st.divider()

    # ── Summary ──────────────────────────────────────────────
    mc = st.columns(4)
    mc[0].metric("Countries",         len(selected_countries))
    mc[1].metric("Suppliers/country", f"{int(min_suppliers)}–{int(max_suppliers)}")
    mc[2].metric("Products/supplier", max_products)
    mc[3].metric("Schedule",
                 f"{int(start_h):02d}:{int(start_m):02d} → {int(end_h):02d}:{int(end_m):02d}"
                 if use_schedule else "Immediate")

    # ── Cron status if already running ───────────────────────
    if st.session_state.get("cron_running"):
        end_dt_live = st.session_state.get("cron_end_dt")
        interval_live = st.session_state.get("cron_interval", "?")
        now_p = datetime.now(tz=TZ_PARIS)
        rem_s = int((end_dt_live - now_p).total_seconds()) if end_dt_live and end_dt_live > now_p else 0
        st.markdown(
            f'<div style="background:#00C85A22;border:1px solid #00C85A;'
            f'border-radius:8px;padding:10px;">'
            f'🟢 <b style="color:#00C85A">Cron active</b> — '
            f'every {interval_live} min | '
            f'stops in {rem_s//60}m {rem_s%60}s</div>',
            unsafe_allow_html=True,
        )
        if st.button("⏹ Stop Cron", key="btn_stop_cron_form"):
            _stop_event.set()
            st.session_state["cron_running"] = False
            st.warning("Cron stopped.")
            st.rerun()

    st.divider()

    if st.button("🚀 LAUNCH SOURCING RUN", type="primary", use_container_width=True):
        _launch(
            countries       = selected_countries,
            min_suppliers   = int(min_suppliers),
            max_suppliers   = int(max_suppliers),
            max_products    = max_products,
            use_schedule    = use_schedule,
            start_dt        = s_dt if use_schedule else None,
            end_dt          = e_dt if use_schedule else None,
            interval_min    = int(interval) if use_schedule else 0,
        )


def _launch(countries, min_suppliers, max_suppliers,
            max_products, use_schedule,
            start_dt, end_dt, interval_min):

    # Save config for cron re-runs
    form_config = {
        "countries":     countries,
        "min_suppliers": min_suppliers,
        "max_suppliers": max_suppliers,
        "max_products":  max_products,
        "run_id_prefix": "form",
    }
    st.session_state["last_form_config"] = form_config

    run_id = str(uuid.uuid4())[:8]

    # ── Phase indicators ──────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🔄 Pipeline Execution")

    ph_col = st.columns(4)
    ph_col[0].markdown("📡 **Phase 1**  \nScraping")
    ph_col[1].markdown("🧹 **Phase 2**  \nNormalization")
    ph_col[2].markdown("🔗 **Phase 4**  \nMatching")
    ph_col[3].markdown("✅ **Phase 4b**  \nValidation")

    prog   = st.progress(0, "Initializing...")
    status = st.empty()

    # ── Wait for exact start time if schedule set ─────────────
    if use_schedule and start_dt:
        import time as _t
        now_paris = datetime.now(tz=TZ_PARIS)
        if start_dt > now_paris:
            wait_placeholder = st.empty()
            while True:
                now_p = datetime.now(tz=TZ_PARIS)
                if now_p >= start_dt:
                    break
                rem = int((start_dt - now_p).total_seconds())
                wait_placeholder.info(
                    f"⏳ Waiting for start time "
                    f"**{start_dt.strftime('%H:%M')} CET** — "
                    f"{rem // 60}m {rem % 60}s"
                )
                _t.sleep(5)
                st.rerun()
            wait_placeholder.success("✅ Start time reached. Launching now.")

    # ── Phase 1: Supplier + Product Scraping ──────────────────
    async def _do():
        db = DatabaseManager()
        await db.create_tables()
        await db.save_scraping_run({
            "run_id":                    run_id,
            "countries":                 countries,
            "required_cities":           {},
            "activity_types":            ["excursion", "ticket", "transfer"],
            "min_suppliers":             min_suppliers,
            "max_suppliers":             max_suppliers,
            "max_products_per_supplier": max_products,
            "recall_old_suppliers":      False,
        })

        # ── Phase 1a — Supplier Discovery ────────────────────
        from scrapers.fournisseur_scraper import FournisseurScraper
        scraper = FournisseurScraper()
        prog.progress(5, "📡 Phase 1a — Discovering suppliers...")
        status.info(
            f"Run: {run_id} | {len(countries)} countries | "
            f"{min_suppliers}–{max_suppliers} per country"
        )
        existing_domains = await db.get_existing_domains()
        try:
            suppliers = await scraper.discover(
                countries      = countries,
                activity_types = ["excursion", "ticket", "transfer"],
                existing_domains = existing_domains,
                min_suppliers  = min_suppliers,
                max_suppliers  = max_suppliers,
                run_id         = run_id,
                required_cities= {},
            )
        finally:
            await scraper.close()

        prog.progress(30, f"✅ {len(suppliers)} suppliers found — saving...")

        all_db_c       = await db.get_all_countries()
        country_id_map = {c["nom"].lower(): c["id"] for c in all_db_c}
        saved_suppliers = []
        for s in suppliers:
            detected  = (s.get("_detected_country") or
                         s.get("_target_country") or "").lower()
            s["pays_id"] = country_id_map.get(detected)
            sup_id = await db.save_fournisseur(s)
            if sup_id:
                s["id"] = sup_id
                saved_suppliers.append(s)

        prog.progress(40, f"💾 {len(saved_suppliers)} saved — scraping products...")

        # ── Phase 1b — Product Scraping ───────────────────────
        from scrapers.product_scraper import ProductScraper
        product_scraper = ProductScraper()
        total_extracted = total_saved = total_skipped = 0
        try:
            for i, supplier in enumerate(saved_suppliers):
                domain = supplier.get("domain", "?")
                is_mkt = supplier.get("is_marketplace", False)
                status.info(
                    f"[{i+1}/{len(saved_suppliers)}] {domain} "
                    f"{'🏪 marketplace' if is_mkt else '🕷 scraping'}"
                )
                products = await product_scraper.scrape_supplier(
                    supplier     = supplier,
                    max_products = max_products,
                    run_id       = run_id,
                )
                if products:
                    batch = await db.save_produits_batch(products)
                    total_extracted += batch.get("extracted", 0)
                    total_saved     += batch.get("saved", 0)
                    total_skipped   += batch.get("skipped", 0)
                pct = 40 + int((i + 1) / max(len(saved_suppliers), 1) * 35)
                prog.progress(pct, f"Saved: {total_saved} | Skipped: {total_skipped}")
        finally:
            await product_scraper.close()

        await db.update_scraping_run(run_id, {
            "status":                 "SCRAPING_COMPLETE",
            "finished_at":            datetime.utcnow(),
            "total_suppliers_found":  len(saved_suppliers),
            "total_products_scraped": total_saved,
        })
        prog.progress(75, "✅ Phase 1 done — starting normalization...")
        return {
            "run_id":    run_id,
            "suppliers": len(saved_suppliers),
            "extracted": total_extracted,
            "saved":     total_saved,
            "skipped":   total_skipped,
        }

    try:
        with st.spinner("Phase 1 — Scraping..."):
            stats = _run(_do())

        st.success(
            f"✅ **Phase 1 done** — "
            f"Suppliers: **{stats['suppliers']}** | "
            f"Products: **{stats['saved']}**"
        )

        # ── Phase 2+3+4 ───────────────────────────────────────
        prog.progress(75, "🧹 Phase 2 — Normalization + Embedding...")
        status.info("Running AI normalization and embedding...")

        with st.spinner("Phase 2+3 — Normalization + Embedding..."):
            pipeline_results = run_post_scraping_pipeline(
                stats["run_id"], limit=500
            )

        prog.progress(95, "🔗 Phase 4 — Matching done.")

        new_prods = pipeline_results.get("new_products", 0)
        elapsed   = pipeline_results.get("elapsed", 0)
        norm_err  = pipeline_results.get("phases",{}).get("normalization",{}).get("error")
        match_err = pipeline_results.get("phases",{}).get("matching",{}).get("error")

        if norm_err:
            st.warning(f"⚠️ Normalization: {norm_err}")
        if match_err:
            st.warning(f"⚠️ Matching: {match_err}")

        # Save final card to DB
        _save_run_card(
            run_id, form_config, "COMPLETE",
            suppliers  = stats["suppliers"],
            products   = stats["saved"],
            new_products = new_prods,
        )

        prog.progress(100, "✅ All phases complete!")
        st.success(
            f"✅ **Full pipeline complete** in {elapsed}s  \n"
            f"🆕 **{new_prods}** new products → Validation"
        )

        # ── Start cron if schedule enabled ────────────────────
        if use_schedule and end_dt and interval_min > 0:
            global _cron_thread, _stop_event
            _stop_event.clear()
            _cron_thread = threading.Thread(
                target=_cron_loop,
                args=(form_config, interval_min, end_dt, 2),
                daemon=True,
            )
            _cron_thread.start()
            st.session_state["cron_running"]  = True
            st.session_state["cron_end_dt"]   = end_dt
            st.session_state["cron_interval"] = interval_min
            st.info(
                f"⏱ Cron started: re-runs every **{interval_min} min** "
                f"until **{end_dt.strftime('%H:%M')} CET**"
            )

        # ── Redirect to Validation ────────────────────────────
        st.balloons()
        if new_prods > 0:
            st.success("👉 Redirecting to Validation...")
            st.session_state["page"] = "validation"
            st.rerun()
        else:
            st.info("No new products this run. Check History.")

    except Exception as e:
        st.error(f"❌ Error: {e}")
        st.exception(e)
        _run(DatabaseManager().update_scraping_run(run_id, {"status": "FAILED"}))