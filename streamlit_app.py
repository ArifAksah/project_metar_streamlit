import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from datetime import datetime, timedelta
import pandas as pd
import altair as alt
import asyncio
import aiohttp
from collections import defaultdict
import logging

# ===================== KONFIGURASI =====================
st.set_page_config(page_title="Analisis Ketersediaan METAR", layout="wide")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==================== FUNGSI BACKEND ====================
async def login_bmgk():
    url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu/@login"
    
    # Ambil kredensial dengan aman dari st.secrets
    try:
        payload = {
            "username": st.secrets["api_credentials"]["username"],
            "password": st.secrets["api_credentials"]["password"]
        }
    except Exception as e:
        st.error(f"❌ Gagal memuat kredensial API dari secrets.toml: {e}")
        return None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("token")
    except Exception as e:
        st.error(f"❌ Login API gagal: {e}")
        return None

async def fetch_all_stations_info(token, session):
    station_map = {}
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "type_name": "BmkgStation", 
        "_metadata": "station_name,station_operating_hours,station_icao,station_wmo_id,is_metar_half_hourly", 
        "_size": 2000
    }
    url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
    try:
        async with session.get(url, headers=headers, params=params, timeout=30) as response:
            response.raise_for_status()
            items = (await response.json()).get("items", [])
            for item in items:
                icao = item.get("station_icao")
                if not icao: continue
                op_hours = item.get("station_operating_hours", 24)
                if not isinstance(op_hours, int) or not (0 < op_hours <= 24): op_hours = 24
                
                station_map[icao] = {
                    "stasiun": item.get("station_name", "-"), 
                    "wmo_id": item.get("station_wmo_id", "-"), 
                    "jam_operasi": op_hours,
                    "sends_half_hourly": item.get("is_metar_half_hourly", False)
                }
            return station_map
    except Exception as e:
        st.warning(f"Gagal mengambil info stasiun: {e}")
        return {}

async def fetch_all_metar(token, session, tahun, bulan):
    headers = {"Authorization": f"Bearer {token}"}
    start_date = datetime(tahun, bulan, 1)
    end_date = (datetime(tahun, bulan + 1, 1) - timedelta(seconds=1)) if bulan < 12 else datetime(tahun, 12, 31, 23, 59, 59)
    params_base = {
        "type_name": "GTSMessage", "_metadata": "timestamp_data,cccc,station_wmo_id", "type_message": 4,
        "timestamp_data__gte": start_date.strftime("%Y-%m-%dT00:00:00"),
        "timestamp_data__lte": end_date.strftime("%Y-%m-%dT23:59:59"), "_size": 10000
    }
    all_data, offset = [], 0
    while True:
        params = dict(params_base); params["_from"] = offset
        url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
        try:
            async with session.get(url, headers=headers, params=params, timeout=45) as response:
                if response.status == 200:
                    items = (await response.json()).get("items", [])
                    if not items: break
                    all_data.extend(items); offset += len(items)
                else: break
        except Exception: break
    return all_data

# --- FUNGSI ANALISIS DIPERBARUI DENGAN PILIHAN MODE KALKULASI ---
def process_and_analyze_metar(metar_data, station_info_map, tahun, bulan, calculation_mode):
    harian_per_stasiun = defaultdict(lambda: defaultdict(set))
    for item in metar_data:
        cccc, timestamp = item.get("cccc"), item.get("timestamp_data")
        if cccc and timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                harian_per_stasiun[dt.strftime("%Y-%m-%d")][cccc].add(dt.strftime("%H:%M"))
            except ValueError: continue
    
    rows, nomor = [], 1
    semua_cccc = sorted(station_info_map.keys())
    start_date = datetime(tahun, bulan, 1)
    num_days = ((datetime(tahun, bulan + 1, 1) if bulan < 12 else datetime(tahun + 1, 1, 1)) - start_date).days
    
    for day in range(num_days):
        tanggal_str = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
        for cccc in semua_cccc:
            info_stasiun = station_info_map[cccc]
            jam_operasi = info_stasiun.get("jam_operasi", 24)
            waktu_data = harian_per_stasiun[tanggal_str].get(cccc, set())
            sends_half_hourly = info_stasiun.get("sends_half_hourly", False)

            # --- LOGIKA PERHITUNGAN BARU BERDASARKAN MODE YANG DIPILIH ---
            if calculation_mode == "Paksa Interval 1 Jam":
                laporan_per_jam = 1
                jam_unik = {w.split(':')[0] for w in waktu_data}
                jumlah_data = len(jam_unik)
            else: # Mode "Otomatis"
                laporan_per_jam = 2 if sends_half_hourly else 1
                if not waktu_data:
                    jumlah_data = 0
                elif sends_half_hourly:
                    slot_unik = {f"{w.split(':')[0]}:00" if int(w.split(':')[1]) < 30 else f"{w.split(':')[0]}:30" for w in waktu_data}
                    jumlah_data = len(slot_unik)
                else:
                    jam_unik = {w.split(':')[0] for w in waktu_data}
                    jumlah_data = len(jam_unik)

            maksimal_data = jam_operasi * laporan_per_jam
            persentase = round((jumlah_data / maksimal_data) * 100, 2) if maksimal_data else 0
            
            flags = []
            if jumlah_data > maksimal_data and maksimal_data > 0:
                flags.append(f"⚠️ Data anomali, melebihi ekspektasi ({jam_operasi} jam).")
            elif jumlah_data == 0:
                flags.append("❌ Tidak ada data")
            elif jumlah_data < (maksimal_data * 0.5):
                flags.append("⚠️ Kurang dari 50%")
            
            if jam_operasi < 24 and not (jumlah_data > maksimal_data):
                flags.append(f"🕒 Op: {jam_operasi} jam")
            
            rows.append({
                "Nomor": nomor, "WMO ID": info_stasiun.get("wmo_id", "-"), "Tanggal": tanggal_str, 
                "ICAO": cccc, "Nama Stasiun": info_stasiun.get("stasiun", "-"),
                "Jam Operasional": jam_operasi, "Interval Pengiriman": "30 Menit" if sends_half_hourly else "1 Jam",
                "Laporan Diharapkan": maksimal_data, "Laporan Masuk": jumlah_data, 
                "Ketersediaan (%)": persentase, "Catatan": "; ".join(flags) if flags else "✅ Lengkap"
            })
            nomor += 1
    return pd.DataFrame(rows)

async def run_full_analysis(tahun, bulan, calculation_mode):
    token = await login_bmgk()
    if not token: return None
    
    connector = aiohttp.TCPConnector(limit=10) 
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_all_stations_info(token, session),
            fetch_all_metar(token, session, tahun, bulan)
        ]
        results = await asyncio.gather(*tasks)
        station_info_map, metar_data = results

        if not station_info_map:
            st.error("Gagal memuat data stasiun. Proses dibatalkan.")
            return None
    
    df = process_and_analyze_metar(metar_data, station_info_map, tahun, bulan, calculation_mode)
    return df

# ===================== AUTENTIKASI =====================
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
authenticator = stauth.Authenticate(
    config['credentials'], config['cookie']['name'], config['cookie']['key'], config['cookie']['expiry_days']
)
authenticator.login()

if not st.session_state["authentication_status"]:
    if st.session_state["authentication_status"] is False: st.error('Username/password salah')
    elif st.session_state["authentication_status"] is None: st.warning('Masukkan username dan password Anda')
    st.stop()

# ===================== KONTEN UTAMA APLIKASI STREAMLIT =====================
with st.sidebar:
    st.title(f"Selamat Datang, *{st.session_state['name']}*")
    authenticator.logout('Logout', 'main')

st.title("📡 Dashboard Analisis Ketersediaan METAR")
st.markdown("Gunakan ini untuk menganalisis ketersediaan data METAR berdasarkan bulan & tahun.")

with st.form("form_analisis"):
    st.markdown("### 1. Atur Parameter Analisis")
    
    col1, col2 = st.columns(2)
    with col1:
        bulan = st.selectbox("📆 Pilih Bulan", list(range(1, 13)), index=datetime.now().month - 1)
    with col2:
        tahun = st.number_input("📅 Masukkan Tahun", min_value=2000, max_value=2100, value=datetime.now().year)
    
    # --- FITUR BARU: PILIHAN MODE KALKULASI ---
    calculation_mode = st.radio(
        "Pilih Mode Kalkulasi",
        ["Otomatis (berdasarkan interval stasiun)", "Paksa Interval 1 Jam (abaikan interval stasiun)"],
        key="calc_mode"
    )
    
    st.markdown("---")
    st.markdown("### 2. Saring Hasil Tampilan (Opsional)")
    
    fcol1, fcol2 = st.columns(2)
    with fcol1:
        op_hours_option = st.selectbox("Filter Jam Operasional", ["Semua", "24 Jam", "Di Bawah 24 Jam"])
    with fcol2:
        station_type_option = st.selectbox("Filter Tipe Stasiun", ["Semua", "Stasiun", "AWOS"])

    submit = st.form_submit_button("🚀 Jalankan Analisis")

if submit:
    with st.spinner("⏳ Mengambil & memproses data dari API secara paralel..."):
        df = asyncio.run(run_full_analysis(tahun, bulan, calculation_mode))

    if df is not None and not df.empty:
        st.success("✅ Data berhasil dianalisis.")
        
        # Sisa kode untuk filtering tampilan dan visualisasi tidak berubah
        # ... (Kode untuk filter tampilan dan visualisasi tetap sama)
        
        st.markdown("### Saring Hasil Analisis")
        fcol1, fcol2, fcol3 = st.columns(3)
        
        with fcol1:
            op_hours_option_display = st.selectbox("Filter Jam Operasional", ["Semua", "24 Jam", "Di Bawah 24 Jam"], key="op_display", index=["Semua", "24 Jam", "Di Bawah 24 Jam"].index(op_hours_option))
        with fcol2:
            station_type_option_display = st.selectbox("Filter Tipe Stasiun", ["Semua", "Stasiun", "AWOS"], key="type_display", index=["Semua", "Stasiun", "AWOS"].index(station_type_option))
        
        df_filtered = df.copy()
        if op_hours_option_display == "24 Jam":
            df_filtered = df_filtered[df_filtered["Jam Operasional"] == 24]
        elif op_hours_option_display == "Di Bawah 24 Jam":
            df_filtered = df_filtered[df_filtered["Jam Operasional"] < 24]

        if station_type_option_display == "Stasiun":
            df_filtered = df_filtered[df_filtered["Nama Stasiun"].str.contains("stasiun", case=False, na=False)]
        elif station_type_option_display == "AWOS":
            df_filtered = df_filtered[df_filtered["Nama Stasiun"].str.contains("awos", case=False, na=False)]
            
        semua_stasiun = sorted(df_filtered["ICAO"].dropna().unique())
        with fcol3:
            dipilih = st.multiselect("Filter Stasiun (ICAO)", semua_stasiun, default=semua_stasiun)
        
        df_filtered = df_filtered[df_filtered["ICAO"].isin(dipilih)]

        st.markdown("---")
        
        st.markdown("## 📊 Ringkasan Data")
        scol1, scol2 = st.columns(2)
        total_bulanan = df_filtered['Laporan Masuk'].sum()
        scol1.metric("Total Laporan Masuk (Bulanan)", f"{int(total_bulanan):,}")

        df_filtered_copy = df_filtered.copy()
        df_filtered_copy['Tanggal'] = pd.to_datetime(df_filtered_copy['Tanggal']).dt.date
        today_date = datetime.now().date()
        jumlah_hari_ini = df_filtered_copy[df_filtered_copy['Tanggal'] == today_date]['Laporan Masuk'].sum()
        scol2.metric(f"Laporan Masuk Hari Ini ({today_date.strftime('%d %B')})", f"{int(jumlah_hari_ini):,}")
        
        st.markdown("---")
        st.markdown(f"## 🗓️ Data Khusus Hari Ini ({today_date.strftime('%d %B %Y')})")
        df_today = df_filtered_copy[df_filtered_copy['Tanggal'] == today_date]
        if not df_today.empty: st.dataframe(df_today)
        else: st.info(f"Tidak ada data yang ditemukan untuk hari ini dengan filter yang aktif.")
        
        st.markdown("---")
        st.markdown("## 📈 Grafik Analisis")
        
        tab1, tab2, tab3 = st.tabs(["Ketersediaan Harian", "Laporan Masuk Harian", "Ketersediaan per Stasiun"])
        with tab1:
            df_harian_ketersediaan = df_filtered_copy.groupby("Tanggal")["Ketersediaan (%)"].mean().reset_index()
            chart_ketersediaan = alt.Chart(df_harian_ketersediaan).mark_line(point=True, color='#1E90FF').encode(
                x=alt.X("Tanggal:T", title="Tanggal"), y=alt.Y("Ketersediaan (%):Q", title="Rata-rata Ketersediaan (%)"),
                tooltip=["Tanggal", "Ketersediaan (%)"]).properties(title="Rata-rata Ketersediaan Data per Hari")
            st.altair_chart(chart_ketersediaan, use_container_width=True)
        with tab2:
            df_harian_masuk = df_filtered_copy.groupby("Tanggal")["Laporan Masuk"].sum().reset_index()
            chart_masuk = alt.Chart(df_harian_masuk).mark_bar(color='#FF6347').encode(
                x=alt.X("Tanggal:T", title="Tanggal"), y=alt.Y("Laporan Masuk:Q", title="Jumlah Laporan Masuk"),
                tooltip=["Tanggal", "Laporan Masuk"]).properties(title="Total Laporan Masuk per Hari")
            st.altair_chart(chart_masuk, use_container_width=True)
        with tab3:
            df_stasiun = df_filtered.groupby("ICAO")["Ketersediaan (%)"].mean().reset_index()
            bar = alt.Chart(df_stasiun).mark_bar().encode(
                x=alt.X("ICAO:N", sort="-y", title="Stasiun (ICAO)"), y="Ketersediaan (%):Q",
                tooltip=["ICAO", "Ketersediaan (%)"]).properties(title="Rata-rata Ketersediaan per Stasiun")
            st.altair_chart(bar, use_container_width=True)
        
        st.markdown("---")
        st.markdown("## 📋 Data Tabel Lengkap")
        st.dataframe(df_filtered)
        st.download_button("⬇️ Download CSV Data Lengkap", data=df_filtered.to_csv(index=False).encode("utf-8"),
                           file_name=f"ketersediaan_lengkap_{bulan}_{tahun}.csv", mime="text/csv")

    elif submit:
        st.warning("⚠️ Tidak ada data yang tersedia untuk bulan & tahun tersebut.")