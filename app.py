import streamlit as st
import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import re
import datetime
import io

# --- KONFIGURASI HALAMAN WEB ---
st.set_page_config(page_title="Kemendikbud Scraper", page_icon="🏫", layout="centered")

# --- CSS BIAR CANTIK (OPSIONAL) ---
st.markdown("""
<style>
    .stButton>button { width: 100%; background-color: #FF4B4B; color: white; }
    .reportview-container { background: #f0f2f6 }
</style>
""", unsafe_allow_html=True)

# --- CLASS SCRAPER (YANG SUDAH KITA BUAT SEBELUMNYA) ---
MAX_CONCURRENT_REQUESTS = 15
TIMEOUT_SECONDS = 30
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

class SchoolScraper:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_html(self, session, url):
        async with self.semaphore:
            for attempt in range(3):
                try:
                    async with session.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS) as response:
                        if response.status == 200: return await response.text()
                        elif response.status == 429: await asyncio.sleep(5)
                except: await asyncio.sleep(2)
            return None

    async def parse_school_list(self, session, kec_url):
        html = await self.fetch_html(session, kec_url)
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if '/npsn/' in a['href']:
                full = "https://referensi.data.kemendikdasmen.go.id" + a['href'] if a['href'].startswith('/') else a['href']
                links.append(full)
        return list(set(links))

    async def parse_detail(self, session, url):
        html = await self.fetch_html(session, url)
        if not html: return None
        try:
            soup = BeautifulSoup(html, 'html.parser')
            data = {'source_url': url}
            
            # 1. Tabel Scanner
            for row in soup.find_all('tr'):
                texts = [c.get_text(strip=True) for c in row.find_all(['td', 'th']) if c.get_text(strip=True) not in ['', ':']]
                if len(texts) == 2: data[texts[0]] = texts[1]
                elif len(texts) > 2: 
                    if len(texts[-1]) > 1 and texts[-1].lower() != texts[0].lower(): data[texts[0]] = texts[-1]

            # 2. Regex Scanner (Koordinat)
            lat = re.search(r'Lintang\s*[:]\s*([-\d\.]+)', html)
            if lat: data['Lintang'] = lat.group(1)
            lon = re.search(r'Bujur\s*[:]\s*([-\d\.]+)', html)
            if lon: data['Bujur'] = lon.group(1)
            
            return data
        except: return None

# --- LOGIKA UTAMA WEB APP ---
async def run_scraping(urls_input, progress_bar, status_text):
    scraper = SchoolScraper()
    urls = [u.strip() for u in urls_input.split(',')]
    
    async with aiohttp.ClientSession() as session:
        # Phase 1
        status_text.text("🔍 Sedang mencari daftar sekolah...")
        all_schools = []
        for u in urls:
            if 'http' in u: all_schools.extend(await scraper.parse_school_list(session, u))
        
        total = len(all_schools)
        if total == 0:
            return None
            
        status_text.text(f"✅ Ditemukan {total} sekolah. Mengunduh data detail...")
        
        # Phase 2
        tasks = [scraper.parse_detail(session, url) for url in all_schools]
        results = []
        
        # Custom Progress Bar Logic
        completed_count = 0
        for f in asyncio.as_completed(tasks):
            res = await f
            if res and len(res) > 1:
                results.append(res)
            
            completed_count += 1
            progress_perc = int((completed_count / total) * 100)
            progress_bar.progress(progress_perc)
            status_text.text(f"⏳ Mengunduh... ({completed_count}/{total} Sekolah)")
            
        return results

# --- TAMPILAN USER INTERFACE (UI) ---
st.title("🏫 Kemendikbud Data Downloader")
st.markdown("Masukkan Link Kecamatan dari web *referensi.data.kemendikdasmen.go.id* untuk download Excel otomatis.")

# Input Form
with st.form("scraper_form"):
    url_input = st.text_area("Link Kecamatan (Bisa banyak, pisahkan dengan koma)", 
                             placeholder="https://referensi.data.kemendikdasmen.go.id/pendidikan/dikdas/020523/3/jf/6",
                             height=100)
    submitted = st.form_submit_button("🚀 Mulai Scraping")

if submitted and url_input:
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Jalankan Async Loop di dalam Streamlit
    raw_data = asyncio.run(run_scraping(url_input, progress_bar, status_text))
    
    if raw_data:
        # Convert ke Excel di Memory (Tanpa simpan file fisik)
        df = pd.DataFrame(raw_data)
        
        # Rapikan Kolom
        cols = list(df.columns)
        prio = ['Nama', 'NPSN', 'Alamat', 'Lintang', 'Bujur', 'source_url']
        for p in reversed(prio):
            if p in cols: cols.insert(0, cols.pop(cols.index(p)))
        df = df[cols]
        
        # Buffer Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Data Sekolah')
        excel_data = output.getvalue()
        
        # Tampilkan Sukses & Tombol Download
        status_text.success(f"🎉 Selesai! Berhasil mengambil {len(df)} data sekolah.")
        st.download_button(
            label="📥 Download Excel",
            data=excel_data,
            file_name=f"Data_Sekolah_{datetime.datetime.now().strftime('%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        status_text.error("❌ Tidak ada data ditemukan atau Link salah.")