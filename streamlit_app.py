import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
from datetime import datetime, timedelta
import pandas as pd
import asyncio
import aiohttp
from collections import defaultdict
import logging
import io

# ===================== KONFIGURASI =====================
st.set_page_config(page_title="Analisis Ketersediaan METAR", layout="wide")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==================== FUNGSI BACKEND ====================
async def login_bmgk():
    url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu/@login"
    try:
        payload = { "username": st.secrets["api_credentials"]["username"], "password": st.secrets["api_credentials"]["password"] }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=15) as response:
                response.raise_for_status()
                return (await response.json()).get("token")
    except Exception as e:
        st.error(f"❌ Login API gagal: {e}")
        return None

async def fetch_station_and_metar_data(tahun, bulan, progress_callback=None):
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        logging.info(msg)
    
    log("🔐 Memulai login ke API BMKG...")
    token = await login_bmgk()
    if not token: 
        log("❌ Login gagal!")
        return None, None
    log("✅ Login berhasil, token diperoleh")
    
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession() as session:
        params_station = {"type_name": "BmkgStation", "_metadata": "station_name,station_operating_hours,station_icao,station_wmo_id,is_metar_half_hourly", "_size": 2000}
        url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
        
        log("📡 Mengambil data stasiun...")
        try:
            async with session.get(url, headers=headers, params=params_station, timeout=30) as response:
                response.raise_for_status()
                items = (await response.json()).get("items", [])
                station_map = {
                    item.get("station_icao"): {
                        "stasiun": item.get("station_name", "-"), "wmo_id": item.get("station_wmo_id", "-"),
                        "jam_operasi": item.get("station_operating_hours", 24),
                        "sends_half_hourly": item.get("is_metar_half_hourly", False)
                    } for item in items if item.get("station_icao")
                }
                log(f"✅ Data {len(station_map)} stasiun berhasil diambil")
        except Exception as e:
            log(f"❌ Gagal mengambil info stasiun: {e}")
            return None, None
        
        start_date, end_date = datetime(tahun, bulan, 1), (datetime(tahun, bulan + 1, 1) - timedelta(seconds=1)) if bulan < 12 else datetime(tahun, 12, 31, 23, 59, 59)
        params_metar = { "type_name": "GTSMessage", "_metadata": "timestamp_data,cccc,station_wmo_id,ttaaii", "type_message": 4, "timestamp_data__gte": start_date.strftime("%Y-%m-%dT00:00:00"), "timestamp_data__lte": end_date.strftime("%Y-%m-%dT23:59:59"), "_size": 10000 }
        
        log(f"📊 Mengambil data METAR periode {bulan}/{tahun}...")
        metar_data, offset = [], 0
        batch_count = 0
        while True:
            params_metar["_from"] = offset
            try:
                async with session.get(url, headers=headers, params=params_metar, timeout=60) as response:
                    response.raise_for_status()
                    items = (await response.json()).get("items", [])
                    if not items: break
                    metar_data.extend(items)
                    offset += len(items)
                    batch_count += 1
                    log(f"📦 Batch {batch_count}: {len(items)} data diambil (Total: {len(metar_data)} data)")
            except Exception as e:
                log(f"❌ Gagal mengambil data METAR: {e}. Data mungkin tidak lengkap.")
                break
        
        log(f"✅ Selesai! Total {len(metar_data)} data METAR berhasil diambil")
        return station_map, metar_data

def process_and_analyze_metar(metar_data, station_info_map, tahun, bulan, calculation_mode):
    harian_per_stasiun = defaultdict(lambda: defaultdict(dict))
    for item in metar_data:
        cccc, timestamp, ttaaii = item.get("cccc"), item.get("timestamp_data"), item.get("ttaaii")
        if cccc and timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                harian_per_stasiun[dt.strftime("%Y-%m-%d")][cccc][dt.strftime("%H:%M")] = ttaaii
            except ValueError: continue
    rows, nomor = [], 1
    start_date = datetime(tahun, bulan, 1)
    num_days = ((datetime(tahun, bulan + 1, 1) if bulan < 12 else datetime(tahun + 1, 1, 1)) - start_date).days
    for day in range(num_days):
        tanggal_str = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
        for cccc in sorted(station_info_map.keys()):
            info_stasiun = station_info_map.get(cccc, {})
            jam_operasi = info_stasiun.get("jam_operasi", 24)
            waktu_data_dict = harian_per_stasiun[tanggal_str].get(cccc, {})
            waktu_data = set(waktu_data_dict.keys())
            sends_half_hourly = info_stasiun.get("sends_half_hourly", False)
            heading_speci = next(iter(waktu_data_dict.values()), "-") if waktu_data_dict else "-"
            if calculation_mode == "Paksa 1 Jam":
                laporan_per_jam, jumlah_data = 1, len({w.split(':')[0] for w in waktu_data})
            else:
                laporan_per_jam = 2 if sends_half_hourly else 1
                if not waktu_data: jumlah_data = 0
                elif sends_half_hourly: jumlah_data = len({f"{w.split(':')[0]}:00" if int(w.split(':')[1]) < 30 else f"{w.split(':')[0]}:30" for w in waktu_data})
                else: jumlah_data = len({w.split(':')[0] for w in waktu_data})
            maksimal_data = jam_operasi * laporan_per_jam
            persentase = round((jumlah_data / maksimal_data) * 100, 2) if maksimal_data else 0
            if persentase > 100: persentase = 100.0
            flags = []
            if jumlah_data > maksimal_data > 0: flags.append(f"⚠️ Anomali")
            elif jumlah_data == 0: flags.append("❌ Nol")
            if jam_operasi < 24: flags.append(f"🕒 Op:{jam_operasi}jam")
            rows.append({ "Nomor": nomor, "WMO ID": info_stasiun.get("wmo_id", "-"), "Tanggal": tanggal_str, "ICAO": cccc, "Nama Stasiun": info_stasiun.get("stasiun", "-"), "Heading Metar": heading_speci, "Jam Operasional": jam_operasi, "Interval Pengiriman": "30 Menit" if sends_half_hourly else "1 Jam", "Laporan Diharapkan": maksimal_data, "Laporan Masuk": jumlah_data, "Ketersediaan (%)": persentase, "Catatan": "; ".join(flags) if flags else "✅ Lengkap" })
            nomor += 1
    return pd.DataFrame(rows)

def create_summary_table(df_daily):
    if df_daily.empty: return pd.DataFrame()
    summary = df_daily.groupby(['ICAO', 'Nama Stasiun', 'Heading Metar']).agg(Total_Laporan_Masuk=('Laporan Masuk', 'sum'), Total_Laporan_Diharapkan=('Laporan Diharapkan', 'sum')).reset_index()
    summary['Rata-Rata_Ketersediaan_Bulanan (%)'] = summary.apply(lambda row: round((row['Total_Laporan_Masuk'] / row['Total_Laporan_Diharapkan']) * 100, 2) if row['Total_Laporan_Diharapkan'] > 0 else 0, axis=1)
    overall_avg = df_daily['Ketersediaan (%)'].mean()
    total_masuk, total_diharapkan = df_daily['Laporan Masuk'].sum(), df_daily['Laporan Diharapkan'].sum()
    overall_row = pd.DataFrame([{'ICAO': '---', 'Nama Stasiun': '**RATA-RATA KESELURUHAN**', 'Heading Metar': '---', 'Total_Laporan_Masuk': total_masuk, 'Total_Laporan_Diharapkan': total_diharapkan, 'Rata-Rata_Ketersediaan_Bulanan (%)': round(overall_avg, 2)}])
    summary_final = pd.concat([summary, overall_row], ignore_index=True)
    return summary_final[['ICAO', 'Nama Stasiun', 'Heading Metar', 'Total_Laporan_Masuk', 'Total_Laporan_Diharapkan', 'Rata-Rata_Ketersediaan_Bulanan (%)']]

def create_multisheet_excel(df_dict):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for sheet_name, df in df_dict.items(): df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

# ===================== AUTENTIKASI STREAMLIT =====================
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
authenticator = stauth.Authenticate(config['credentials'], config['cookie']['name'], config['cookie']['key'], config['cookie']['expiry_days'])
authenticator.login()
if not st.session_state["authentication_status"]:
    if st.session_state.get("authentication_status") is False: st.error('Username/password salah')
    else: st.warning('Masukkan username dan password Anda')
    st.stop()

# ===================== KONTEN UTAMA APLIKASI STREAMLIT =====================
with st.sidebar:
    st.title(f"Selamat Datang, *{st.session_state['name']}*")
    authenticator.logout('Logout', 'main')

st.title("📡 Dashboard Analisis Ketersediaan METAR")

if 'options_loaded' not in st.session_state:
    st.session_state.options_loaded = False
    st.session_state.run_analysis = False
    for key in ['station_map', 'metar_data', 'icao_options', 'ttaaii_options', 'bulan_select', 'tahun_input']:
        if key in st.session_state:
            del st.session_state[key]

def handle_date_change():
    st.session_state.options_loaded = False
    st.session_state.run_analysis = False

st.markdown("### 1. Pilih Periode dan Muat Data")
col1, col2, col3 = st.columns([2, 2, 1])
now = datetime.now()
last_month = now.month - 1 if now.month > 1 else 12
tahun_default = now.year if now.month > 1 else now.year - 1
col1.selectbox("📆 Pilih Bulan", list(range(1, 13)), index=last_month - 1, key='bulan_select', on_change=handle_date_change)
col2.number_input("📅 Masukkan Tahun", min_value=2020, max_value=2100, value=tahun_default, key='tahun_input', on_change=handle_date_change)

# Tombol untuk muat data
tombol_muat_diklik = col3.button("📥 Muat Data", type="primary", use_container_width=True)

# Proses muat data HANYA jika tombol diklik
if tombol_muat_diklik:
    # Buat container untuk log progress
    progress_container = st.empty()
    log_container = st.empty()
    log_messages = []
    
    def update_progress(msg):
        log_messages.append(msg)
        log_container.text_area("📋 Log Progress:", "\n".join(log_messages), height=200)
    
    with st.spinner("⏳ Sedang memproses..."):
        station_map, metar_data = asyncio.run(fetch_station_and_metar_data(
            st.session_state.tahun_input, 
            st.session_state.bulan_select,
            progress_callback=update_progress
        ))
        if station_map and metar_data:
            st.session_state.station_map = station_map
            st.session_state.metar_data = metar_data
            temp_df = pd.DataFrame(metar_data)
            st.session_state.icao_options = sorted(temp_df['cccc'].dropna().unique())
            st.session_state.ttaaii_options = sorted(temp_df['ttaaii'].dropna().unique())
            st.session_state.options_loaded = True
            st.success("🎉 Data berhasil dimuat!")
            st.rerun()
        else:
            st.error("Gagal memuat data. Tidak ada data untuk periode ini atau terjadi error API.")
            st.stop()

# Tampilkan form hanya jika data sudah dimuat
if st.session_state.options_loaded:
    with st.form("form_analisis_lengkap"):
        st.markdown("### 2. Atur Parameter dan Jalankan Analisis")
        st.success(f"✅ Opsi filter untuk **{st.session_state.bulan_select}-{st.session_state.tahun_input}** siap. Atur semua filter di bawah ini.")
        
        calculation_mode = st.radio("Mode Kalkulasi", ["Otomatis", "Paksa 1 Jam"], key="calc_mode", horizontal=True)
        fcol1, fcol2 = st.columns(2)
        fcol3, fcol4 = st.columns(2)
        with fcol1: op_hours_option = st.selectbox("Jam Operasional", ["Semua", "24 Jam", "Di Bawah 24 Jam"])
        with fcol2: station_type_option = st.selectbox("Tipe Stasiun", ["Semua", "Stasiun", "AWOS"])
        with fcol3: stasiun_dipilih = st.multiselect("Stasiun (ICAO)", st.session_state.icao_options, default=st.session_state.icao_options)
        with fcol4: heading_dipilih = st.multiselect("Heading Metar (TTAAII)", st.session_state.ttaaii_options, default=st.session_state.ttaaii_options)
        
        if st.form_submit_button("🚀 Jalankan Analisis", type="primary", use_container_width=True):
            st.session_state.run_analysis = True
else:
    st.info("👆 Klik tombol **'📥 Muat Data'** untuk mengambil data dari API dan memulai analisis.")

if st.session_state.run_analysis:
    with st.spinner("🚀 Menganalisis data..."):
        # Gunakan data dari session state, tidak perlu panggil API lagi
        df = process_and_analyze_metar(st.session_state.metar_data, st.session_state.station_map, st.session_state.tahun_input, st.session_state.bulan_select, calculation_mode)
        
        # Terapkan semua filter dari form
        df_filtered = df.copy()
        if op_hours_option == "24 Jam": df_filtered = df_filtered[df_filtered["Jam Operasional"] == 24]
        elif op_hours_option == "Di Bawah 24 Jam": df_filtered = df_filtered[df_filtered["Jam Operasional"] < 24]
        if station_type_option == "Stasiun": df_filtered = df_filtered[df_filtered["Nama Stasiun"].str.contains("stasiun", case=False, na=False)]
        elif station_type_option == "AWOS": df_filtered = df_filtered[df_filtered["Nama Stasiun"].str.contains("awos", case=False, na=False)]
        df_filtered = df_filtered[df_filtered["ICAO"].isin(stasiun_dipilih) & df_filtered["Heading Metar"].isin(heading_dipilih)]

    if df_filtered.empty:
        st.info("ℹ️ Tidak ada data yang cocok dengan filter yang Anda pilih.")
    else:
        # 1. Siapkan data untuk Laporan Lengkap
        summary_df_full = create_summary_table(df_filtered)
        excel_bytes_lengkap = create_multisheet_excel({
            "Data Harian Rinci": df_filtered, 
            "Rekap Bulanan": summary_df_full.iloc[:-1], 
            "Rekap Keseluruhan": summary_df_full.iloc[-1:]
        })
        
        # 2. Tampilkan hasil Laporan Lengkap
        st.markdown("---")
        st.markdown("## 📊 Hasil Analisis")
        st.dataframe(summary_df_full)
        st.download_button("⬇️ Download Laporan Lengkap (.xlsx)", excel_bytes_lengkap, f"laporan_ketersediaan_{st.session_state.bulan_select}_{st.session_state.tahun_input}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with st.expander("Lihat Data Harian Lengkap"):
            st.dataframe(df_filtered)

        # 3. Siapkan dan Tampilkan Laporan SE_OPMET berdasarkan data yang SUDAH DIFILTER
        st.markdown("---")
        st.markdown("## 📋 Data Khusus SE_OPMET 2025")
        se_opmet_indicators = {
            "WITT", "WIMA", "WITC", "WITN", "WIMM", "WIMB", "WIME", "WIMN", "WIMS", "WIBB", "WIBJ", "WIDD", "WIDN", "WIDS", "WIDT", "WIDO", "WIDM", "WIEE", "WIGG", "WIJJ", "WIJI", "WIPP", "WIKK", "WIKT", "WILL", "WIHH", "WIII", "WIRR", "WICA", "WICC", "WAHL", "WAHS", "WAHQ", "WAHH", "WAHI", "WIOO", "WIOG", "WIOK", "WIOP", "WIOS", "WIOD", "WARR", "WARW", "WART", "WARA", "WADY", "WARD", "WAGG", "WAGI", "WAGB", "WAGS", "WAGM", "WALL", "WALS", "WAQA", "WAQD", "WAQJ", "WAQQ", "WAQT", "WAOO", "WAOK", "WADD", "WATT", "WATC", "WATG", "WATL", "WATM", "WATO", "WATR", "WATS", "WADL", "WADB", "WADS", "WAAA", "WAFB", "WAFM", "WAFJ", "WAWW", "WAWB", "WAWP", "WAFF", "WAFL", "WAFP", "WAFW", "WAMG", "WAMH", "WAMM", "WAEE", "WAEG", "WAEL", "WAES", "WAEW", "WAPP", "WAPU", "WAPN", "WAPA", "WAPC", "WAPS", "WAPF", "WASS", "WASF", "WASK", "WAUU", "WABB", "WABO", "WAJJ", "WAJI", "WABI", "WAYE", "WAKK", "WAKT", "WAVV"
        }
        
        # <<< INI BARIS KUNCI PERBAIKANNYA >>>
        # Kita filter dari df_filtered, bukan dari df awal.
        df_se_opmet = df_filtered[df_filtered['ICAO'].isin(se_opmet_indicators)].copy()
        
        if not df_se_opmet.empty:
            st.dataframe(df_se_opmet)
            
            # Buat summary dan Excel dari data df_se_opmet yang sudah benar-benar terfilter
            summary_df_se_opmet = create_summary_table(df_se_opmet)
            excel_bytes_se_opmet = create_multisheet_excel({
                "Data Harian (SE_OPMET)": df_se_opmet,
                "Rekap Bulanan (SE_OPMET)": summary_df_se_opmet.iloc[:-1],
                "Rekap Keseluruhan (SE_OPMET)": summary_df_se_opmet.iloc[-1:]
            })
            
            st.download_button(
                label="⬇️ Download Laporan SE_OPMET (.xlsx)",
                data=excel_bytes_se_opmet,
                file_name=f"laporan_se_opmet_{st.session_state.bulan_select}_{st.session_state.tahun_input}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_se_opmet_excel"
            )
        else:
            st.info("Tidak ada data SE_OPMET yang cocok dengan filter yang Anda terapkan.")