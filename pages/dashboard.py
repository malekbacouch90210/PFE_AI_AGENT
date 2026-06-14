# pages/dashboard.py
import streamlit as st
import asyncio
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import DatabaseManager


def show():
    st.title("📈 Dashboard Statistiques")
    st.markdown("---")

    try:
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def get_stats():
            db = DatabaseManager()
            async with db.get_session() as session:
                from sqlalchemy import text

                fournisseurs = (await session.execute(text("SELECT COUNT(*) FROM fournisseurs"))).scalar()
                fournisseurs_ok = (
                    await session.execute(text("SELECT COUNT(*) FROM fournisseurs WHERE mapping = 'OK'"))).scalar()
                produits = (await session.execute(text("SELECT COUNT(*) FROM produits"))).scalar()
                dex = (await session.execute(text("SELECT COUNT(*) FROM produit_dex"))).scalar()
                comparisons = (await session.execute(text("SELECT COUNT(*) FROM produit_dex_comparison"))).scalar()

                # Par zone
                zones = await session.execute(text("""
                                                   SELECT zone, COUNT (*)
                                                   FROM fournisseurs
                                                   WHERE zone IS NOT NULL
                                                   GROUP BY zone
                                                   """))

                return fournisseurs, fournisseurs_ok, produits, dex, comparisons, zones.fetchall()

        fournisseurs, fournisseurs_ok, produits, dex, comparisons, zones = loop.run_until_complete(get_stats())

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric("🏢 Fournisseurs", fournisseurs)
        with col2:
            st.metric("✅ Connectables", fournisseurs_ok)
        with col3:
            st.metric("🎫 Produits", produits)
        with col4:
            st.metric("📚 Produits DEX", dex)
        with col5:
            st.metric("🔄 Comparaisons", comparisons)

        st.markdown("---")

        if zones:
            st.subheader("📍 Répartition par zone")
            df_zones = pd.DataFrame(zones, columns=["Zone", "Nombre"])
            st.bar_chart(df_zones.set_index("Zone"))
            st.dataframe(df_zones)

    except Exception as e:
        st.warning(f"⚠️ Base de données non accessible: {e}")