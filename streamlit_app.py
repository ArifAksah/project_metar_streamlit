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
    url = "https://bmkgsatu.bmkg.go.id/api/v21/user/session/login"
    try:
        payload = { "username": st.secrets["api_credentials"]["username"], "password": st.secrets["api_credentials"]["password"] }
        timeout = aiohttp.ClientTimeout(total=15)
        
        logging.info(f"Attempting login to: {url}")
        logging.info(f"Username: {st.secrets['api_credentials']['username']}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=timeout) as response:
                status_code = response.status
                logging.info(f"Login response status: {status_code}")
                
                result = await response.json()
                logging.info(f"Login response body: {result}")
                
                response.raise_for_status()
                
                # New response structure: {"status": value, "code": value, "data": {"exp": value, "token": value}}
                if result.get("data") and result["data"].get("token"):
                    token = result["data"]["token"]
                    logging.info(f"✅ Login successful! Token received (length: {len(token)})")
                    return token
                else:
                    logging.error(f"❌ Login failed: No token in response. Response: {result}")
                    st.error(f"❌ Login gagal: Response tidak mengandung token. Response: {result}")
                    return None
    except aiohttp.ClientResponseError as e:
        logging.error(f"❌ HTTP Error during login: {e.status} - {e.message}")
        st.error(f"❌ Login API gagal (HTTP {e.status}): {e.message}")
        return None
    except Exception as e:
        logging.error(f"❌ Exception during login: {type(e).__name__} - {str(e)}")
        st.error(f"❌ Login API gagal: {type(e).__name__} - {str(e)}")
        return None

async def fetch_station_and_metar_data(tahun, bulan, progress_callback=None):
    """
    Fetch station and METAR data with progress tracking
    progress_callback: function to update progress (step_name, current, total, message)
    """
    # Step 1: Login
    if progress_callback:
        progress_callback("Login", 0, 4, "🔐 Melakukan login ke API BMKG...")
    
    token = await login_bmgk()
    if not token:
        if progress_callback:
            progress_callback("Login", 0, 4, "❌ Login gagal!")
        return None, None
    
    if progress_callback:
        progress_callback("Login", 1, 4, "✅ Login berhasil!")
    
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession() as session:
        # Step 2: Fetch station info
        if progress_callback:
            progress_callback("Station", 1, 4, "📡 Mengambil informasi stasiun...")
        
        params_station = {"type_name": "BmkgStation", "_metadata": "station_name,station_operating_hours,station_icao,station_wmo_id,is_metar_half_hourly", "_size": 2000}
        url_station = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
        try:
            logging.info(f"Fetching station data from: {url_station}")
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(url_station, headers=headers, params=params_station, timeout=timeout) as response:
                status_code = response.status
                logging.info(f"Station data response status: {status_code}")
                
                response.raise_for_status()
                result = await response.json()
                logging.info(f"Station data response keys: {result.keys() if isinstance(result, dict) else 'not a dict'}")
                
                items = result.get("items", [])
                logging.info(f"Number of station items: {len(items)}")
                
                station_map = {
                    item.get("station_icao"): {
                        "stasiun": item.get("station_name", "-"), "wmo_id": item.get("station_wmo_id", "-"),
                        "jam_operasi": item.get("station_operating_hours", 24),
                        "sends_half_hourly": item.get("is_metar_half_hourly", False)
                    } for item in items if item.get("station_icao")
                }
                
                logging.info(f"✅ Station map created with {len(station_map)} stations")
                
                if progress_callback:
                    progress_callback("Station", 2, 4, f"✅ Berhasil mengambil {len(station_map)} stasiun")
                    
        except Exception as e:
            if progress_callback:
                progress_callback("Station", 2, 4, f"❌ Gagal mengambil info stasiun: {e}")
            logging.error(f"❌ Error fetching station data: {type(e).__name__} - {str(e)}", exc_info=True)
            return None, None
        
        # Step 3: Fetch METAR data using new endpoint
        if progress_callback:
            progress_callback("METAR", 2, 4, "📊 Memulai pengambilan data METAR...")
        
        start_date = datetime(tahun, bulan, 1)
        num_days = ((datetime(tahun, bulan + 1, 1) if bulan < 12 else datetime(tahun + 1, 1, 1)) - start_date).days
        metar_data = []
        logging.info(f"Mulai ambil METAR {tahun}-{bulan:02d} menggunakan endpoint baru...")
        
        # Fetch data for each day in the month
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            date_str = current_date.strftime("%Y-%m-%d")
            
            # Update progress for each day
            if progress_callback:
                progress_callback("METAR", 2, 4, f"📅 Mengambil data tanggal {date_str} ({day+1}/{num_days})")
            
            # New endpoint: GET https://bmkgsatu.bmkg.go.id/api/v21/monitoring/gts/metar/daily/date/2026-06-01
            url_metar = f"https://bmkgsatu.bmkg.go.id/api/v21/monitoring/gts/metar/daily/date/{date_str}"
            
            max_retries = 3
            for retry in range(max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=120)
                    logging.info(f"Fetching METAR for {date_str} from: {url_metar}")
                    
                    async with session.get(url_metar, headers=headers, timeout=timeout) as response:
                        status_code = response.status
                        logging.info(f"METAR {date_str} response status: {status_code}")
                        
                        response.raise_for_status()
                        result = await response.json()
                        
                        logging.info(f"METAR {date_str} response type: {type(result).__name__}")
                        if isinstance(result, dict):
                            logging.info(f"METAR {date_str} response keys: {result.keys()}")
                        
                        # Process the response data
                        daily_data = []
                        
                        # Check if result is a dict with "data" key or directly a list
                        if isinstance(result, dict) and result.get("data"):
                            daily_data = result["data"]
                            logging.info(f"METAR {date_str}: Found data in 'data' key, {len(daily_data)} items")
                        elif isinstance(result, list):
                            # If API returns list directly
                            daily_data = result
                            logging.info(f"METAR {date_str}: Response is list directly, {len(daily_data)} items")
                        else:
                            logging.warning(f"METAR {date_str}: Unexpected response format: {result}")
                        
                        # Convert the new format to match the old format expected by process_and_analyze_metar
                        if daily_data and isinstance(daily_data, list):
                            # Log first item to see structure
                            if len(daily_data) > 0:
                                first_item = daily_data[0]
                                logging.info(f"METAR {date_str}: First item type: {type(first_item)}")
                                logging.info(f"METAR {date_str}: First item sample: {first_item}")
                                
                                if isinstance(first_item, dict):
                                    logging.info(f"METAR {date_str}: First item keys: {first_item.keys()}")
                                elif isinstance(first_item, list):
                                    logging.info(f"METAR {date_str}: First item is list with {len(first_item)} elements")
                            
                            items_added = 0
                            for item in daily_data:
                                if isinstance(item, dict):
                                    # Item is a dictionary
                                    metar_record = {
                                        "cccc": item.get("cccc"),
                                        "timestamp_data": item.get("timestamp_data"),
                                        "ttaaii": item.get("ttaaii"),
                                        "station_wmo_id": item.get("station_wmo_id")
                                    }
                                    metar_data.append(metar_record)
                                    items_added += 1
                                elif isinstance(item, list):
                                    # Item is a list - need to map to dict
                                    # Assuming format: [cccc, timestamp_data, ttaaii, station_wmo_id, ...]
                                    # We need to see the actual data to know the correct mapping
                                    if len(item) >= 4:
                                        metar_record = {
                                            "cccc": item[0] if len(item) > 0 else None,
                                            "timestamp_data": item[1] if len(item) > 1 else None,
                                            "ttaaii": item[2] if len(item) > 2 else None,
                                            "station_wmo_id": item[3] if len(item) > 3 else None
                                        }
                                        metar_data.append(metar_record)
                                        items_added += 1
                                    else:
                                        logging.warning(f"METAR {date_str}: List item has only {len(item)} elements, expected at least 4")
                                else:
                                    logging.warning(f"METAR {date_str}: Item is neither dict nor list: {type(item)}")
                            
                            logging.info(f"✅ Tanggal {date_str}: {len(daily_data)} records processed, {items_added} items added to metar_data")
                            logging.info(f"Current total metar_data length: {len(metar_data)}")
                        else:
                            logging.warning(f"METAR {date_str}: daily_data is empty or not a list")
                        
                        break
                except asyncio.TimeoutError:
                    if retry < max_retries - 1:
                        logging.warning(f"⏳ Timeout untuk tanggal {date_str}, retry {retry+1}/{max_retries}...")
                        if progress_callback:
                            progress_callback("METAR", 2, 4, f"⏳ Timeout {date_str}, retry {retry+1}/{max_retries}...")
                        await asyncio.sleep(5)
                    else:
                        logging.warning(f"⏳ Max retries reached for {date_str}. Melanjutkan ke tanggal berikutnya.")
                        break
                except aiohttp.ClientResponseError as e:
                    logging.error(f"❌ HTTP Error untuk {date_str}: {e.status} - {e.message}")
                    break
                except Exception as e:
                    logging.error(f"❌ Exception untuk {date_str}: {type(e).__name__} - {str(e)}", exc_info=True)
                    break
        
        # Step 4: Complete
        logging.info(f"📊 Final summary: {len(station_map)} stations, {len(metar_data)} METAR records")
        
        if progress_callback:
            progress_callback("Complete", 4, 4, f"✅ Selesai! Total {len(metar_data)} records METAR")
        
        if len(metar_data) == 0:
            logging.warning("⚠️ WARNING: No METAR data collected! Returning empty result.")
        
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
    st.session_state.options_loaded = True  # Skip auto-fetch on first load
    st.session_state.run_analysis = False

def handle_date_change():
    st.session_state.options_loaded = False
    st.session_state.run_analysis = False

st.markdown("### 1. Pilih Periode")
st.info("Ubah bulan atau tahun di bawah ini, lalu klik tombol Refresh untuk mengambil data.")
col1, col2, col3 = st.columns(3)
now = datetime.now()
last_month = now.month - 1 if now.month > 1 else 12
tahun_default = now.year if now.month > 1 else now.year - 1
col1.selectbox("📆 Pilih Bulan", list(range(1, 13)), index=last_month - 1, key='bulan_select')
col2.number_input("📅 Masukkan Tahun", min_value=2020, max_value=2100, value=tahun_default, key='tahun_input')
col3.write("")  # Spacer
col3.write("")  # Spacer
if col3.button("🔄 Refresh Data", type="primary", use_container_width=True):
    st.session_state.options_loaded = False
    st.session_state.run_analysis = False
    st.rerun()

if not st.session_state.options_loaded:
    # Create progress placeholder
    progress_placeholder = st.empty()
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(step_name, current, total, message):
        """Update progress display"""
        progress = int((current / total) * 100)
        progress_bar.progress(progress)
        status_text.info(f"**Step {current}/{total}:** {message}")
    
    try:
        station_map, metar_data = asyncio.run(
            fetch_station_and_metar_data(
                st.session_state.tahun_input,
                st.session_state.bulan_select,
                progress_callback=update_progress
            )
        )
        
        if station_map and metar_data:
            st.session_state.station_map = station_map
            st.session_state.metar_data = metar_data
            
            # Create DataFrame and handle potential nested structures
            temp_df = pd.DataFrame(metar_data)
            
            # Log DataFrame info for debugging
            logging.info(f"DataFrame created with {len(temp_df)} rows")
            logging.info(f"DataFrame columns: {temp_df.columns.tolist()}")
            logging.info(f"Sample row: {temp_df.iloc[0].to_dict() if len(temp_df) > 0 else 'empty'}")
            
            # Flatten any nested dict/list values to strings
            for col in temp_df.columns:
                temp_df[col] = temp_df[col].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)
            
            # Extract unique values safely
            try:
                st.session_state.icao_options = sorted([x for x in temp_df['cccc'].dropna().unique() if x and str(x) != 'None'])
                st.session_state.ttaaii_options = sorted([x for x in temp_df['ttaaii'].dropna().unique() if x and str(x) != 'None'])
                logging.info(f"✅ Extracted {len(st.session_state.icao_options)} ICAO codes and {len(st.session_state.ttaaii_options)} TTAAII codes")
            except Exception as e:
                logging.error(f"Error extracting unique values: {e}", exc_info=True)
                st.error(f"Error processing data: {e}")
                st.stop()
            
            st.session_state.options_loaded = True
            
            # Clear progress indicators
            progress_bar.empty()
            status_text.empty()
            progress_placeholder.empty()
            
            st.success(f"✅ Data berhasil dimuat! {len(metar_data)} records METAR dari {len(station_map)} stasiun")
            st.rerun()
        else:
            # Clear progress indicators
            progress_bar.empty()
            status_text.empty()
            progress_placeholder.empty()
            
            st.error("❌ **Gagal memuat data**")
            st.warning("Kemungkinan penyebab:")
            st.markdown("""
            - Login API gagal (periksa credentials di `.streamlit/secrets.toml`)
            - Tidak ada data untuk periode yang dipilih
            - Koneksi internet bermasalah
            - Endpoint API tidak dapat diakses
            """)
            st.info("💡 **Solusi:** Periksa log di terminal untuk detail error, atau coba periode lain")
            st.stop()
    except Exception as e:
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        progress_placeholder.empty()
        
        st.error(f"❌ **Error saat mengambil data:** {str(e)}")
        logging.error(f"Exception in data fetch: {e}", exc_info=True)
        st.stop()

# Cek apakah data sudah diambil sebelum menampilkan form analisis
if 'icao_options' not in st.session_state or 'ttaaii_options' not in st.session_state:
    st.warning("👆 Klik tombol **Refresh Data** di atas untuk mengambil data terlebih dahulu.")
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
        se_opmet_order = (
            "WAAA", "WAEG", "WAEL", "WAES", "WAFB", "WAFL", "WAFM", "WAFW", "WAMM",
            "WAPC", "WAPF", "WAPN", "WAPP", "WAPS", "WAPU", "WAWB", "WIBB", "WIEE",
            "WIMA", "WITC", "WITN", "WITT", "WIMM", "WIMB", "WIME", "WIMN", "WIMS",
            "WIBJ", "WIDD", "WIDN", "WIDS", "WIDT", "WIDO", "WIDM", "WIGG", "WIJJ",
            "WIJI", "WIPP", "WIKK", "WIKT", "WILL", "WIHH", "WIII", "WIRR", "WICA",
            "WICC", "WAHL", "WAHS", "WAHQ", "WAHH", "WAHI", "WIOO", "WIOG", "WIOK",
            "WIOP", "WIOS", "WIOD", "WARR", "WARW", "WART", "WARA", "WADY", "WARD",
            "WAGG", "WAGI", "WAGB", "WAGS", "WAGM", "WALL", "WALS", "WAQA", "WAQD",
            "WAQJ", "WAQQ", "WAQT", "WAOO", "WAOK", "WADD", "WATT", "WATC", "WATG",
            "WATL", "WATM", "WATO", "WATR", "WATS", "WADL", "WADB", "WADS", "WAFJ",
            "WAWW", "WAWP", "WAFF", "WAFP", "WAEE", "WAEW", "WAPA", "WASS", "WASF",
            "WASK", "WAUU", "WABB", "WABO", "WAJJ", "WAJI", "WABI", "WAYE", "WAKK",
            "WAKT", "WAVV"
        )
        se_opmet_indicators = set(se_opmet_order)
        
        # Filter dari df_filtered, bukan dari df awal.
        # Urutkan sesuai daftar SE_OPMET yang sudah ditetapkan.
        df_se_opmet = df_filtered[df_filtered['ICAO'].isin(se_opmet_indicators)].copy()
        df_se_opmet['ICAO'] = pd.Categorical(df_se_opmet['ICAO'], categories=se_opmet_order, ordered=True)
        df_se_opmet = df_se_opmet.sort_values('ICAO').reset_index(drop=True)
        
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
        
        # 4. Siapkan dan Tampilkan Rekapan Data Metar MDM
        st.markdown("---")
        st.markdown("## 📊 Rekapan Data Metar MDM")
        
        # Urutan stasiun MDM sesuai gambar (22 stasiun)
        mdm_stations_order = [
            "WAAA", "WAEG", "WAEL", "WAES", "WAFB", "WAFL", "WAFM", "WAFW",
            "WAMM", "WAPC", "WAPF", "WAPN", "WAPP", "WAPS", "WAPU", "WAWB",
            "WIBB", "WIEE", "WIMA", "WITC", "WITN", "WITT"
        ]
        
        # Filter data untuk stasiun MDM dari df (data mentah, BUKAN df_filtered)
        # Supaya tidak terpengaruh filter user
        df_mdm = df[df['ICAO'].isin(mdm_stations_order)].copy()
        
        # Debug: tampilkan stasiun yang ditemukan vs tidak ditemukan
        stasiun_ditemukan = set(df_mdm['ICAO'].unique())
        stasiun_tidak_ditemukan = set(mdm_stations_order) - stasiun_ditemukan
        
        st.info(f"📊 Ditemukan {len(stasiun_ditemukan)} dari {len(mdm_stations_order)} stasiun MDM")
        if stasiun_tidak_ditemukan:
            st.warning(f"⚠️ Stasiun yang tidak ditemukan di data API: {', '.join(sorted(stasiun_tidak_ditemukan))}")
        
        if not df_mdm.empty:
            # Urutkan sesuai urutan yang ditentukan
            df_mdm['sort_order'] = df_mdm['ICAO'].map({icao: idx for idx, icao in enumerate(mdm_stations_order)})
            df_mdm = df_mdm.sort_values(['sort_order', 'Tanggal']).drop('sort_order', axis=1).reset_index(drop=True)
            
            st.dataframe(df_mdm)
            
            # Buat summary dan Excel dari data df_mdm
            summary_df_mdm = create_summary_table(df_mdm)
            
            # Urutkan summary juga sesuai urutan MDM
            summary_df_mdm_sorted = summary_df_mdm.iloc[:-1].copy()  # Ambil semua kecuali baris rata-rata
            summary_df_mdm_sorted['sort_order'] = summary_df_mdm_sorted['ICAO'].map({icao: idx for idx, icao in enumerate(mdm_stations_order)})
            summary_df_mdm_sorted = summary_df_mdm_sorted.sort_values('sort_order').drop('sort_order', axis=1)
            
            # Gabungkan kembali dengan baris rata-rata keseluruhan
            summary_df_mdm_final = pd.concat([summary_df_mdm_sorted, summary_df_mdm.iloc[-1:]], ignore_index=True)
            
            st.dataframe(summary_df_mdm_final)
            
            excel_bytes_mdm = create_multisheet_excel({
                "Data Harian (MDM)": df_mdm,
                "Rekap Bulanan (MDM)": summary_df_mdm_final.iloc[:-1],
                "Rekap Keseluruhan (MDM)": summary_df_mdm_final.iloc[-1:]
            })
            
            st.download_button(
                label="⬇️ Download Laporan MDM (.xlsx)",
                data=excel_bytes_mdm,
                file_name=f"laporan_mdm_{st.session_state.bulan_select}_{st.session_state.tahun_input}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_mdm_excel"
            )
        else:
            st.info("Tidak ada data MDM yang cocok dengan filter yang Anda terapkan.")
