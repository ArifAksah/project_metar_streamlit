# metar_checker.py
import requests
import aiohttp
import asyncio
import os
import pickle
import logging
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd


logging.basicConfig(
    filename="log_metar_checker.log",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# Dictionary untuk provinsi dan balai
daftar_provinsi_per_balai = {
    1: {
        1: "Nanggroe Aceh Darussalam",
        2: "Sumatera Utara",
        3: "Sumatera Barat",
        4: "Riau",
        10: "Kep. Riau"
    },
    2: {
        5: "Jambi",
        6: "Sumatera Selatan",
        7: "Bengkulu",
        8: "Lampung",
        9: "Kep. Bangka Belitung",
        11: "DKI Jakarta",
        12: "Jawa Barat",
        13: "Jawa Tengah",
        14: "DI Yogyakarta",
        16: "Banten",
        20: "Kalimantan Barat"
    },
    3: {
        15: "Jawa Timur",
        17: "Bali",
        18: "Nusa Tenggara Barat",
        19: "Nusa Tenggara Timur",
        21: "Kalimantan Tengah",
        22: "Kalimantan Selatan",
        23: "Kalimantan Timur",
        34: "Kalimantan Utara"
    },
    4: {
        24: "Sulawesi Utara",
        25: "Sulawesi Tengah",
        26: "Sulawesi Selatan",
        27: "Sulawesi Tenggara",
        28: "Gorontalo",
        29: "Sulawesi Barat",
        30: "Maluku",
        31: "Maluku Utara"
    },
    5: {
        32: "Papua",
        33: "Papua Barat"
    }
}

# (Isi stasiun_meteorologi dipotong karena terlalu panjang, kamu sudah punya datanya — tempel ulang di sini)
stasiun_meteorologi = {
    "WITT": {"stasiun": "Sultan Iskandar Muda", "bandara": "Sultan Iskandar Muda", "heading_metar": "SAID32"},
    "WITK": {"stasiun": "Sultan Iskandar Muda", "bandara": "Rembele", "heading_metar": "SAID40"},
    "WITO": {"stasiun": "Sultan Iskandar Muda", "bandara": "Kuala Batu", "heading_metar": "SAID40"},
    "WIML": {"stasiun": "Sultan Iskandar Muda", "bandara": "Lasikin", "heading_metar": "SAID40"},
    "WIMU": {"stasiun": "Sultan Iskandar Muda", "bandara": "Alas Leuser", "heading_metar": "SAID40"},
    "WIMA": {"stasiun": "Malikussaleh", "bandara": "Malikussaleh", "heading_metar": "SAID40"},
    "WITC": {"stasiun": "Cut Nyak Dhien Nagan Raya", "bandara": "Cut Nyak Dhien Nagan Raya", "heading_metar": "SAID40"},
    "WITN": {"stasiun": "Maimun Saleh", "bandara": "Maimun Saleh", "heading_metar": "SAID40"},
    "WIMM": {"stasiun": "Kualanamu", "bandara": "Kualanamu Internasional", "heading_metar": "SAID31"},
    "WIMF": {"stasiun": "Kualanamu", "bandara": "Jenderal Besar Abdul Haris Nasution", "heading_metar": "SAID40"},
    "WIMB": {"stasiun": "Binaka", "bandara": "Binaka", "heading_metar": "SAID40"},
    "WIME": {"stasiun": "Aek Godang", "bandara": "Aek Godang", "heading_metar": "SAID40"},
    "WIMN": {"stasiun": "Silangit", "bandara": "Raja Sisingamangaraja XII", "heading_metar": "SAID33"},
    "WIMP": {"stasiun": "Silangit", "bandara": "Sibisa", "heading_metar": "SAID40"},
    "WIMS": {"stasiun": "FL Tobing", "bandara": "DR Ferdinand Lumban Tobing", "heading_metar": "SAID40"},
    "WIBB": {"stasiun": "Sultan Syarif Kasim II", "bandara": "Sultan Syarif Kasim II", "heading_metar": "SAID32"},
    "WIBD": {"stasiun": "Sultan Syarif Kasim II", "bandara": "Pinang Kampai", "heading_metar": "SAID40"},
    "WIBG": {"stasiun": "Sultan Syarif Kasim II", "bandara": "Tuanku Tambusai", "heading_metar": "SAID40"},
    "WIBJ": {"stasiun": "Japura", "bandara": "Japura", "heading_metar": "SAID40"},
    "WIDD": {"stasiun": "Hang Nadim", "bandara": "Hang Nadim", "heading_metar": "SAID31"},
    "WIDN": {"stasiun": "Raja Hajji Fisabilillah", "bandara": "Raja Hajji Fisabilillah", "heading_metar": "SAID32"},
    "WIDS": {"stasiun": "Dabo", "bandara": "Dabo", "heading_metar": "SAID40"},
    "WIDT": {"stasiun": "Raja Hajji Abdullah", "bandara": "Raja Hajji Abdullah", "heading_metar": "SAID40"},
    "WIDO": {"stasiun": "Lanud Raden Sadjad", "bandara": "Ranai", "heading_metar": "SAID40"},
    "WIDM": {"stasiun": "Tarempa", "bandara": "Matak", "heading_metar": "SAID40"},
    "WIDL": {"stasiun": "Tarempa", "bandara": "Letung", "heading_metar": "SAID40"},
    "WIEE": {"stasiun": "Minangkabau", "bandara": "Minangkabau", "heading_metar": "SAID32"},
    "WIEB": {"stasiun": "Minangkabau", "bandara": "Mentawai", "heading_metar": "SAID40"},
    "WIGG": {"stasiun": "Fatmawati Soekarno", "bandara": "Fatmawati Soekarno", "heading_metar": "SAID40"},
    "WIGM": {"stasiun": "Fatmawati Soekarno", "bandara": "Mukomuko", "heading_metar": "SAID40"},
    "WIGE": {"stasiun": "Fatmawati Soekarno", "bandara": "Enggano", "heading_metar": "SAID40"},
    "WIJJ": {"stasiun": "Sultan Thaha", "bandara": "Sultan Thaha", "heading_metar": "SAID40"},
    "WIJB": {"stasiun": "Sultan Thaha", "bandara": "Muara Bungo", "heading_metar": "SAID40"},
    "WIJI": {"stasiun": "Depati Parbo", "bandara": "Depati Parbo", "heading_metar": "SAID40"},
    "WIPP": {"stasiun": "Sultan Mahmud Badaruddin II", "bandara": "Sultan Mahmud Badaruddin II", "heading_metar": "SAID32"},
    "WIPB": {"stasiun": "Sultan Mahmud Badaruddin II", "bandara": "Silampari", "heading_metar": "SAID40"},
    "WIPY": {"stasiun": "Sultan Mahmud Badaruddin II", "bandara": "Atung Bungsu", "heading_metar": "SAID40"},
    "WIKK": {"stasiun": "Depati Amir", "bandara": "Depati Amir", "heading_metar": "SAID40"},
    "WIKT": {"stasiun": "H.AS. Hanandjoeddin", "bandara": "H.AS. Hanandjoeddin", "heading_metar": "SAID40"},
    "WILL": {"stasiun": "Radin Inten II", "bandara": "Radin Inten II", "heading_metar": "SAID33"},
    "WIPO": {"stasiun": "Radin Inten II", "bandara": "Gatot Soebroto", "heading_metar": "SAID40"},
    "WILP": {"stasiun": "Radin Inten II", "bandara": "Muhammad Taufiq Kiemas", "heading_metar": "SAID40"},
    "WIHH": {"stasiun": "Lanud Halim Perdanakusuma", "bandara": "Halim Perdana Kusuma", "heading_metar": "SAID31"},
    "WIII": {"stasiun": "Soekarno Hatta", "bandara": "Soekarno Hatta", "heading_metar": "SAID31"},
    "WIRR": {"stasiun": "Budiarto", "bandara": "Budiarto", "heading_metar": "SAID40"},
    "WICA": {"stasiun": "Kertajati", "bandara": "Kertajati", "heading_metar": "SAID40"},
    "WICD": {"stasiun": "Kertajati", "bandara": "Cakrabhuwana", "heading_metar": "SAID40"},
    "WICC": {"stasiun": "Lanud Husein Sastranegara", "bandara": "Husein Sastranegara", "heading_metar": "SAID40"},
    "WAHL": {"stasiun": "Tunggul Wulung", "bandara": "Tunggul Wulung", "heading_metar": "SAID40"},
    "WICN": {"stasiun": "Tunggul Wulung", "bandara": "Nusawiru", "heading_metar": "SAID40"},
    "WAHS": {"stasiun": "Ahmad Yani", "bandara": "Jenderal Ahmad Yani", "heading_metar": "SAID33"},
    "WAHU": {"stasiun": "Ahmad Yani", "bandara": "Dewadaru", "heading_metar": "SAID40"},
    "WARC": {"stasiun": "Ahmad Yani", "bandara": "Ngloram", "heading_metar": "SAID40"},
    "WAHQ": {"stasiun": "Lanud Adi Soemarmo", "bandara": "Adi Soemarmo", "heading_metar": "SAID40"},
    "WAHH": {"stasiun": "Lanud Adisutjipto", "bandara": "Adisutjipto", "heading_metar": "SAID40"},
    "WAHI": {"stasiun": "Yogyakarta", "bandara": "Yogyakarta", "heading_metar": "SAID32"},
    "WIOO": {"stasiun": "Supadio", "bandara": "Supadio", "heading_metar": "SAID32"},
    "WIOG": {"stasiun": "Nangapinoh", "bandara": "Nangapinoh", "heading_metar": "SAID40"},
    "WIOK": {"stasiun": "Rahadi Oesman", "bandara": "Rahadi Oesman", "heading_metar": "SAID40"},
    "WIOP": {"stasiun": "Pangsuma", "bandara": "Pangsuma", "heading_metar": "SAID40"},
    "WIOS": {"stasiun": "Tebelian", "bandara": "Tebelian", "heading_metar": "SAID40"},
    "WIOD": {"stasiun": "Singkawang", "bandara": "Singkawang", "heading_metar": "SAID40"},
    "WARR": {"stasiun": "Juanda", "bandara": "Juanda", "heading_metar": "SAID31"},
    "WARW": {"stasiun": "Sangkapura", "bandara": "Harun Thohir", "heading_metar": "SAID40"},
    "WART": {"stasiun": "Trunojoyo", "bandara": "Trunojoyo", "heading_metar": "SAID40"},
    "WARA": {"stasiun": "Lanud Abdulrachman Saleh", "bandara": "Abdulrachman Saleh", "heading_metar": "SAID40"},
    "WADY": {"stasiun": "Banyuwangi", "bandara": "Banyuwangi", "heading_metar": "SAID33"},
    "WARE": {"stasiun": "Banyuwangi", "bandara": "Noto Hadinegoro", "heading_metar": "SAID40"},
    "WARD": {"stasiun": "Dhoho", "bandara": "Dhoho", "heading_metar": "SAID40"},
    "WAGG": {"stasiun": "Tjilik Riwut", "bandara": "Tjilik Riwut", "heading_metar": "SAID40"},
    "WAGA": {"stasiun": "Tjilik Riwut", "bandara": "Kuala Kurun", "heading_metar": "SAID40"},
    "WAGI": {"stasiun": "Iskandar", "bandara": "Iskandar", "heading_metar": "SAID40"},
    "WAGB": {"stasiun": "Beringin", "bandara": "Hajji Muhammad Sidik", "heading_metar": "SAID40"},
    "WAGP": {"stasiun": "Beringin", "bandara": "Purukcahu", "heading_metar": "SAID40"},
    "WAGS": {"stasiun": "H. Asan", "bandara": "H. Asan", "heading_metar": "SAID40"},
    "WAGF": {"stasiun": "H. Asan", "bandara": "Kuala Pembuang", "heading_metar": "SAID40"},
    "WAGM": {"stasiun": "Sanggu", "bandara": "Sanggu", "heading_metar": "SAID40"},
    "WALL": {"stasiun": "Sultan Aji Muhammad Sulaiman Sepinggan", "bandara": "Sultan Aji Muhammad Sulaiman", "heading_metar": "SAID32"},
    "WALE": {"stasiun": "Sultan Aji Muhammad Sulaiman Sepinggan", "bandara": "Melak", "heading_metar": "SAID40"},
    "WALS": {"stasiun": "Ajipangeran Tumenggung Pranoto", "bandara": "Ajipangeran Tumenggung Pranoto", "heading_metar": "SAID40"},
    "WAQA": {"stasiun": "Nunukan", "bandara": "Nunukan", "heading_metar": "SAID40"},
    "WAQD": {"stasiun": "Tanjung Harapan", "bandara": "Tanjung Harapan", "heading_metar": "SAID40"},
    "WAQL": {"stasiun": "Tanjung Harapan", "bandara": "Long Apung", "heading_metar": "SAID40"},
    "WAQM": {"stasiun": "Tanjung Harapan", "bandara": "Kol. Robert Atty Bessing", "heading_metar": "SAID40"},
    "WAQJ": {"stasiun": "Yuvai Semaring", "bandara": "Yuvai Semaring", "heading_metar": "SAID40"},
    "WAQQ": {"stasiun": "Juwata", "bandara": "Juwata", "heading_metar": "SAID33"},
    "WAQT": {"stasiun": "Kalimarau", "bandara": "Kalimarau", "heading_metar": "SAID40"},
    "WAQC": {"stasiun": "Kalimarau", "bandara": "Maratua", "heading_metar": "SAID40"},
    "WAOO": {"stasiun": "Syamsudin Noor", "bandara": "Syamsudin Noor", "heading_metar": "SAID32"},
    "WAON": {"stasiun": "Syamsudin Noor", "bandara": "Tanjung Warukin", "heading_metar": "SAID40"},
    "WAOK": {"stasiun": "Gusti Syamsir Alam", "bandara": "Gusti Syamsir Alam", "heading_metar": "SAID40"},
    "WAOC": {"stasiun": "Gusti Syamsir Alam", "bandara": "Bersujud", "heading_metar": "SAID40"},
    "WADD": {"stasiun": "I Gusti Ngurah Rai", "bandara": "I Gusti Ngurah Rai", "heading_metar": "SAID31"},
    "WATT": {"stasiun": "Eltari", "bandara": "El Tari", "heading_metar": "SAID33"},
    "WATA": {"stasiun": "Eltari", "bandara": "A. A. Bere Tallo", "heading_metar": "SAID40"},
    "WATW": {"stasiun": "Eltari", "bandara": "Wunopito", "heading_metar": "SAID40"},
    "WATC": {"stasiun": "Fransiskus Xaverius Seda", "bandara": "Fransiskus Xaverius Seda", "heading_metar": "SAID40"},
    "WATE": {"stasiun": "Fransiskus Xaverius Seda", "bandara": "H. Hasan Aroeboesman", "heading_metar": "SAID40"},
    "WATG": {"stasiun": "Frans Sales Lega", "bandara": "Frans Sales Lega", "heading_metar": "SAID40"},
    "WATB": {"stasiun": "Frans Sales Lega", "bandara": "Soa", "heading_metar": "SAID40"},
    "WATL": {"stasiun": "Gewayantana", "bandara": "Gewayantana", "heading_metar": "SAID40"},
    "WATM": {"stasiun": "Mali", "bandara": "Mali", "heading_metar": "SAID40"},
    "WATO": {"stasiun": "Komodo", "bandara": "Komodo", "heading_metar": "SAID40"},
    "WATR": {"stasiun": "David Constantijn Saudale", "bandara": "David Constantijn Saudale", "heading_metar": "SAID40"},
    "WATS": {"stasiun": "Tardamu", "bandara": "Tardamu", "heading_metar": "SAID40"},
    "WATU": {"stasiun": "Umbu Mehang Kunda", "bandara": "Umbu Mehang Kunda", "heading_metar": "SAID40"},
    "WATK": {"stasiun": "Umbu Mehang Kunda", "bandara": "Lede Kalumbang", "heading_metar": "SAID40"},
    "WADL": {"stasiun": "Zainuddin Abdul Madjid", "bandara": "Zainuddin Abdul Madjid", "heading_metar": "SAID32"},
    "WADB": {"stasiun": "Sultan Muhammad Salahuddin", "bandara": "Sultan Muhammad Salahuddin", "heading_metar": "SAID40"},
    "WADS": {"stasiun": "Sultan Muhammad Kaharuddin", "bandara": "Sultan Muhammad Kaharuddin", "heading_metar": "SAID40"},
    "WAAA": {"stasiun": "Sultan Hasanuddin", "bandara": "Sultan Hasanuddin", "heading_metar": "SAID31"},
    "WAWH": {"stasiun": "Sultan Hasanuddin", "bandara": "H. Aroeppala", "heading_metar": "SAID40"},
    "WAWN": {"stasiun": "Sultan Hasanuddin", "bandara": "Arung Palakka", "heading_metar": "SAID40"},
    "WAFB": {"stasiun": "Toraja", "bandara": "Toraja", "heading_metar": "SAID40"},
    "WAFM": {"stasiun": "Andi Jemma", "bandara": "Andi Jemma", "heading_metar": "SAID40"},
    "WAFN": {"stasiun": "Andi Jemma", "bandara": "Seko", "heading_metar": "SAID40"},
    "WAFD": {"stasiun": "Andi Jemma", "bandara": "Lagaligo", "heading_metar": "SAID40"},
    "WAFK": {"stasiun": "Andi Jemma", "bandara": "Rampi", "heading_metar": "SAID40"},
    "WAFJ": {"stasiun": "Tampa Padang", "bandara": "Tampa Padang", "heading_metar": "SAID40"},
    "WAWW": {"stasiun": "Lanud Haluoleo", "bandara": "Haluoleo", "heading_metar": "SAID40"},
    "WAWB": {"stasiun": "Beto Ambari", "bandara": "Betoambari", "heading_metar": "SAID40"},
    "WAWD": {"stasiun": "Beto Ambari", "bandara": "Matahora", "heading_metar": "SAID40"},
    "WAWR": {"stasiun": "Beto Ambari", "bandara": "Sugimanuru", "heading_metar": "SAID40"},
    "WAWP": {"stasiun": "Sangia Ni Bandera", "bandara": "Sangia Nibandera", "heading_metar": "SAID40"},
    "WAFF": {"stasiun": "Mutiara Sis-Al Jufri", "bandara": "Mutiara Sis-Al Jufri", "heading_metar": "SAID40"},
    "WAFO": {"stasiun": "Mutiara Sis-Al Jufri", "bandara": "Morowali", "heading_metar": "SAID40"},
    "WAFY": {"stasiun": "Mutiara Sis-Al Jufri", "bandara": "Pogogul", "heading_metar": "SAID40"},
    "WAFL": {"stasiun": "Sultan Bantilan", "bandara": "Sultan Bantilan", "heading_metar": "SAID40"},
    "WAFP": {"stasiun": "Kasiguncu", "bandara": "Kasiguncu", "heading_metar": "SAID40"},
    "WAFW": {"stasiun": "Syukuran Aminuddin Amir", "bandara": "Syukuran Aminuddin Amir", "heading_metar": "SAID40"},
    "WAFU": {"stasiun": "Syukuran Aminuddin Amir", "bandara": "Tanjung Api", "heading_metar": "SAID40"},
    "WAMG": {"stasiun": "Djalaluddin", "bandara": "Djalaluddin", "heading_metar": "SAID40"},
    "WAFZ": {"stasiun": "Djalaluddin", "bandara": "Panua Pohuwato", "heading_metar": "SAID40"},
    "WAMH": {"stasiun": "Naha", "bandara": "Naha", "heading_metar": "SAID40"},
    "WAMM": {"stasiun": "Sam Ratulangi", "bandara": "Sam Ratulangi", "heading_metar": "SAID32"},
    "WAMN": {"stasiun": "Sam Ratulangi", "bandara": "Melonguane", "heading_metar": "SAID40"},
    "WAMS": {"stasiun": "Sam Ratulangi", "bandara": "Miangas", "heading_metar": "SAID40"},
    "WAMI": {"stasiun": "Sam Ratulangi", "bandara": "Bolaang Mongondow", "heading_metar": "SAID40"},
    "WAEE": {"stasiun": "Sultan Babullah", "bandara": "Sultan Babullah", "heading_metar": "SAID40"},
    "WAEK": {"stasiun": "Sultan Babullah", "bandara": "Kuabang", "heading_metar": "SAID40"},
    "WAEJ": {"stasiun": "Sultan Babullah", "bandara": "Gebe", "heading_metar": "SAID40"},
    "WAEM": {"stasiun": "Sultan Babullah", "bandara": "Buli", "heading_metar": "SAID40"},
    "WAEG": {"stasiun": "Gamar Malamo", "bandara": "Gamar Malamo", "heading_metar": "SAID40"},
    "WAEL": {"stasiun": "Oesman Sadik", "bandara": "Oesman Sadik", "heading_metar": "SAID40"},
    "WAES": {"stasiun": "Emalamo", "bandara": "Emalamo", "heading_metar": "SAID40"},
    "WAEW": {"stasiun": "Lanud Leo Wattimena", "bandara": "Pitu", "heading_metar": "SAID40"},
    "WAPP": {"stasiun": "Pattimura", "bandara": "Pattimura", "heading_metar": "SAID33"},
    "WAPM": {"stasiun": "Pattimura", "bandara": "Jos Orno Imsula", "heading_metar": "SAID40"},
    "WAPG": {"stasiun": "Pattimura", "bandara": "Namrole", "heading_metar": "SAID40"},
    "WAPO": {"stasiun": "Pattimura", "bandara": "Larat", "heading_metar": "SAID40"},
    "WAPD": {"stasiun": "Pattimura", "bandara": "Dobo", "heading_metar": "SAID40"},
    "WATQ": {"stasiun": "Pattimura", "bandara": "John Becker", "heading_metar": "SAID40"},
    "WAPU": {"stasiun": "Kuffar", "bandara": "Kuffar", "heading_metar": "SAID40"},
    "WAPN": {"stasiun": "Namlea", "bandara": "Namniwel", "heading_metar": "SAID40"},
    "WAPA": {"stasiun": "Amahai", "bandara": "Amahai", "heading_metar": "SAID40"},
    "WAPV": {"stasiun": "Amahai", "bandara": "Wahai", "heading_metar": "SAID40"},
    "WAPC": {"stasiun": "Bandaneira", "bandara": "Bandaneira", "heading_metar": "SAID40"},
    "WAPS": {"stasiun": "Mathilda Batlayeri", "bandara": "Mathilda Batlayeri", "heading_metar": "SAID40"},
    "WAPF": {"stasiun": "Karel Sadsuitubun", "bandara": "Karel Sadsuitubun", "heading_metar": "SAID40"},
    "WASS": {"stasiun": "Domine Eduard Osok", "bandara": "Domine Eduard Osok", "heading_metar": "SAID40"},
    "WASN": {"stasiun": "Domine Eduard Osok", "bandara": "Marinda", "heading_metar": "SAID40"},
    "WASA": {"stasiun": "Domine Eduard Osok", "bandara": "Ayawasi", "heading_metar": "SAID40"},
    "WAST": {"stasiun": "Domine Eduard Osok", "bandara": "Teminabuan", "heading_metar": "SAID40"},
    "WASI": {"stasiun": "Domine Eduard Osok", "bandara": "Inanwatan", "heading_metar": "SAID40"},
    "WASU": {"stasiun": "Domine Eduard Osok", "bandara": "Kambuaya", "heading_metar": "SAID40"},
    "WASF": {"stasiun": "Torea", "bandara": "Siboru", "heading_metar": "SAID40"},
    "WASK": {"stasiun": "Utarom", "bandara": "Utarom", "heading_metar": "SAID40"},
    "WAUU": {"stasiun": "Rendani", "bandara": "Rendani", "heading_metar": "SAID40"},
    "WAUB": {"stasiun": "Rendani", "bandara": "Bintuni", "heading_metar": "SAID40"},
    "WAUW": {"stasiun": "Rendani", "bandara": "Wasior", "heading_metar": "SAID40"},
    "WASO": {"stasiun": "Rendani", "bandara": "Babo", "heading_metar": "SAID40"},
    "WABB": {"stasiun": "Frans Kaisiepo", "bandara": "Frans Kaisiepo", "heading_metar": "SAID31"},
    "WABF": {"stasiun": "Frans Kaisiepo", "bandara": "Numfoor", "heading_metar": "SAID40"},
    "WABO": {"stasiun": "Sudjarwo Tjondronegoro", "bandara": "Stevanus Rumbewas", "heading_metar": "SAID40"},
    "WAJJ": {"stasiun": "Sentani", "bandara": "Sentani", "heading_metar": "SAID33"},
    "WABG": {"stasiun": "Sentani", "bandara": "Kasonaweja", "heading_metar": "SAID40"},
    "WAJS": {"stasiun": "Sentani", "bandara": "Senggeh", "heading_metar": "SAID40"},
    "WAJI": {"stasiun": "Torea", "bandara": "Mararena", "heading_metar": "SAID40"},
    "WAYY": {"stasiun": "Mozez Kilangin", "bandara": "Mozez Kilangin", "heading_metar": "SAID40"},
    "WAYL": {"stasiun": "Mozez Kilangin", "bandara": "Ilaga", "heading_metar": "SAID40"},
    "WAKG": {"stasiun": "Mozez Kilangin", "bandara": "Ewer", "heading_metar": "SAID40"},
    "WABI": {"stasiun": "Nabire", "bandara": "Douw Aturure", "heading_metar": "SAID40"},
    "WABA": {"stasiun": "Nabire", "bandara": "Waghete", "heading_metar": "SAID40"},
    "WAYE": {"stasiun": "Enarotali", "bandara": "Enarotali", "heading_metar": "SAID40"},
    "WAKK": {"stasiun": "Mopah", "bandara": "Mopah", "heading_metar": "SAID33"},
    "WAKJ": {"stasiun": "Mopah", "bandara": "Kimaam", "heading_metar": "SAID40"},
    "WAKO": {"stasiun": "Mopah", "bandara": "Okaba", "heading_metar": "SAID40"},
    "WAKP": {"stasiun": "Mopah", "bandara": "Kepi", "heading_metar": "SAID40"},
    "WAKT": {"stasiun": "Tanah Merah", "bandara": "Tanah Merah", "heading_metar": "SAID40"},
    "WAVV": {"stasiun": "Wamena", "bandara": "Wamena", "heading_metar": "SAID40"},
    "WAVD": {"stasiun": "Wamena", "bandara": "Nop Goliat Dekai", "heading_metar": "SAID40"},
    "WAJO": {"stasiun": "Wamena", "bandara": "Oksibil", "heading_metar": "SAID40"}
}

# ==================== LOGIN ====================
async def login_bmgk():
    url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu/@login"
    payload = {"username": "pdbshift", "password": "2025Lancar!"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                return data["token"]
            else:
                raise Exception(f"Login gagal: {await response.text()}")

# ==================== FETCH ALL METAR ====================
async def fetch_all_metar(tahun, bulan):
    token = await login_bmgk()
    headers = {"Authorization": f"Bearer {token}"}

    start_date = datetime(tahun, bulan, 1)
    end_date = datetime(tahun, bulan + 1, 1) - timedelta(seconds=1) if bulan < 12 else datetime(tahun, 12, 31, 23, 59, 59)

    params_base = {
        "type_name": "GTSMessage",
        "_metadata": "type_message,timestamp_data,timestamp_sent_data,station_wmo_id,sandi_gts,ttaaii,cccc,need_ftp",
        "type_message": 4,
        "timestamp_data__gte": start_date.strftime("%Y-%m-%dT00:00"),
        "timestamp_data__lte": end_date.strftime("%Y-%m-%dT23:59:59"),
        "_size": 10000
    }

    all_data = []
    offset = 0
    limit = 5

    async with aiohttp.TCPConnector(limit=limit) as connector:
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                params = dict(params_base)
                params["_from"] = offset

                url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
                try:
                    async with session.get(url, headers=headers, params=params, timeout=20) as response:
                        if response.status == 200:
                            json_data = await response.json()
                            items = json_data.get("items", [])
                            all_data.extend(items)
                            print(f"✔️ Offset {offset}: {len(items)} data ditambahkan")
                            if len(items) < 10000:
                                break
                            offset += len(items)
                        else:
                            logging.warning(f"[{start_date.strftime('%Y-%m')}] Error status {response.status}")
                            break
                except Exception as e:
                    logging.warning(f"[{start_date.strftime('%Y-%m')}] Exception saat fetch: {e}")
                    break

    print(f"📦 Total data diterima: {len(all_data)}")
    return all_data

async def analisis_metar_bulanan_async(tahun, bulan):
    metar_data = await fetch_all_metar(tahun, bulan)

    harian_per_stasiun = defaultdict(lambda: defaultdict(set))
    jam_operasional_dict = defaultdict(lambda: 24)
    semua_cccc = set(stasiun_meteorologi.keys())

    # Proses data METAR
    for item in metar_data:
        cccc = item.get("cccc")
        timestamp = item.get("timestamp_data")

        if cccc and timestamp:
            try:
                dt = datetime.fromisoformat(timestamp)
            except ValueError:
                dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

            tanggal = dt.strftime("%Y-%m-%d")
            waktu = dt.strftime("%H:%M")

            harian_per_stasiun[tanggal][cccc].add(waktu)

            # Ambil jam operasional jika tersedia
            jam_operasi = item.get("me48_station_operating_hours")
            if jam_operasi and isinstance(jam_operasi, int):
                jam_operasional_dict[(tanggal, cccc)] = jam_operasi

    # Buat data baris per hari dan per stasiun
    rows = []
    nomor = 1
    for tanggal in sorted(harian_per_stasiun.keys()):
        for cccc in sorted(semua_cccc):
            waktu_data = harian_per_stasiun[tanggal].get(cccc, set())
            jam_operasi = jam_operasional_dict.get((tanggal, cccc), 24)
            maksimal_data = jam_operasi * 2
            jumlah_data = len(waktu_data)
            persentase = round((jumlah_data / maksimal_data) * 100, 2) if maksimal_data else 0

            info_stasiun = stasiun_meteorologi.get(cccc, {"stasiun": "-", "bandara": "-"})
            flags = []

            if jumlah_data == 0:
                flags.append("❌ Tidak ada data")
            elif jumlah_data < (maksimal_data * 0.5):
                flags.append("⚠️ Kurang dari 50%")
            if jam_operasi < 24:
                flags.append(f"🕒 Jam operasional: {jam_operasi} jam")

            rows.append({
                "Nomor": nomor,
                "WMO ID": info_stasiun.get("wmo_id", "-"),
                "Tanggal": tanggal,
                "Location Indicator": cccc,
                "Nama UPT": info_stasiun["stasiun"],
                "Nama Bandara": info_stasiun["bandara"],
                "Jam Operasional (jam)": jam_operasi,
                "Jumlah Data Masuk": jumlah_data,
                "Ketersediaan Data (%)": persentase,
                "Catatan": "; ".join(flags) if flags else "✅ Lengkap"
            })
            nomor += 1

    # Konversi ke DataFrame dan simpan ke CSV
    df = pd.DataFrame(rows)
    output_file = f"ketersediaan_metar_stasiun_{bulan}_{tahun}_async.csv"
    df.to_csv(output_file, index=False)

    return output_file, df








