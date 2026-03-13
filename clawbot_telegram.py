"""
╔══════════════════════════════════════════════════════╗
║   Gümüş Avcısı Elite v5.0 — BIST Intraday Scanner  ║
║   Streamlit + borsapy  |  HuggingFace Space         ║
╚══════════════════════════════════════════════════════╝
v5.0 düzeltmeleri:
  ✅ df_b.sort_index() — ORB hesabı artık garantili doğru
  ✅ hacim format crash fix — string hacim için güvenli format
  ✅ bp.__version__ → getattr — crash riski yok
  ✅ Session state ticker mismatch — eski ticker sonucu görünmez
  ✅ ind_rsi sıfıra bölme koruması
  ✅ analiz_et try/except indikatör koruması
  ✅ Grafik: EMA9 + EMA21 çizgileri eklendi
  ✅ Grafik: RSI alt panel eklendi
  ✅ Tarama: concurrent.futures paralel sorgulama
  ✅ Tarama: CSV export butonu
  ✅ Otomatik yenileme toggle
  ✅ Pozisyon büyüklüğü hesaplayıcı
  ✅ Makro: BIST100 endeksi eklendi
  ✅ Optional typing (Python 3.8+ uyumlu)
"""

import streamlit as st
import borsapy as bp
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, time as dtime
from typing import Optional, List
import concurrent.futures
import pytz
import time
import requests as _requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ─────────────────────────────────────────────────────
# SAYFA AYARLARI
# ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="Gümüş Avcısı | BIST Scanner",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .main,.block-container{background-color:#0d0f14;color:#cdd6f4;padding:1rem 2rem}
  [data-testid="stMetricValue"]{font-size:1.4rem;font-weight:700;color:#00ff88}
  [data-testid="stMetricLabel"]{font-size:0.75rem;color:#8b949e}
  .stMetric{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:12px}
  div[data-testid="stExpander"]{border:1px solid #21262d;border-radius:8px;background:#161b22}
  .stButton>button{background:#161b22;border:1px solid #00ff8844;color:#00ff88;border-radius:6px}
  .stButton>button:hover{background:#00ff8822;border-color:#00ff88}
  .card{background:#161b22;border:1px solid #21262d;border-radius:10px;padding:16px;margin-bottom:10px}
  .badge-guclu{background:#00ff8822;color:#00ff88;padding:3px 12px;border-radius:4px;font-size:13px;font-weight:700}
  .badge-takip{background:#ffd70022;color:#ffd700;padding:3px 12px;border-radius:4px;font-size:13px;font-weight:700}
  .badge-dikkat{background:#ff4d4d22;color:#ff4d4d;padding:3px 12px;border-radius:4px;font-size:13px;font-weight:700}
</style>
""", unsafe_allow_html=True)

TURKEY_TZ = pytz.timezone("Europe/Istanbul")

# ─────────────────────────────────────────────────────
# SABITLER
# ─────────────────────────────────────────────────────
ORB_BARS     = 6
STOP_ATR     = 1.0
HEDEF_ATR    = 2.0
MAX_STOP_PCT = 2.5
MIN_HAREKET  = 0.4
PARALEL_IS   = 8

FALLBACK_TICKERS = sorted(set([
    # Teknoloji & Yazılım
    "LOGO","ARDYZ","KFEIN","HTTBT","MIATK","FORTE","SMART","MTRKS","INDES","REEDR",
    # Savunma & Otomotiv
    "ASELS","KONTR","TTRAK","FROTO","TUSAS","KATMR",
    # Ulaştırma & Havacılık
    "THYAO","PGSUS","TAVHL","CLEBI","PASEU",
    # Enerji
    "ASTOR","ENJSA","GWIND","AKSEN","ENERY","ORGE","CANTE","ALFAS",
    # Bankacılık & Holding
    "ISCTR","AKBNK","GARAN","YKBNK","TSKB","ALBRK","KCHOL","SAHOL","GOZDE",
    "HALKB","VAKBN","EKGYO",
    # Perakende, Gıda & Sağlık
    "MGROS","BIMAS","SOKM","ULKER","CCOLA","ULUUN","MPARK","LKMNH","MAVI","KOTON",
    # Sanayi & Gayrimenkul
    "TUPRS","EREGL","ISDMR","PETKM","SARKY","AKSA","TRGYO","AVPGY",
    # Diğer BIST core
    "SASA","SAHOL","TCELL","TOASO","ARCLK","TTKOM","SISE","ENKAI",
    "KOZAL","KOZA1","DOHOL","VESTL","AEFES","BRISA","GUBRF","OTKAR",
    "AGHOL","ALARK","KRDMD","NETAS","ALKIM","CIMSA","DOAS","HEKTS",
    "KORDS","OYAKC","TKFEN","TRKCM","TURSG","ZOREN","ZORLU","MNDRS","ODAS",
]))


# ─────────────────────────────────────────────────────
# YARDIMCI
# ─────────────────────────────────────────────────────

def simdi() -> str:
    return datetime.now(TURKEY_TZ).strftime("%d.%m.%Y %H:%M")

def seans_durumu():
    now  = datetime.now(TURKEY_TZ)
    t    = now.time()
    if now.weekday() >= 5:
        return False, "🔴 Hafta Sonu — Borsa Kapalı"
    if dtime(10, 0) <= t <= dtime(18, 0):
        return True,  "🟢 Seans Açık"
    if t < dtime(10, 0):
        return False, "🟡 Seans Öncesi"
    return False, "🟠 Seans Sonrası"

# ✅ getattr — crash riski yok
BP_VERSION = getattr(bp, "__version__", getattr(bp, "version", "?"))

# ─────────────────────────────────────────────────────
# TELEGRAM BİLDİRİM
# ─────────────────────────────────────────────────────

def telegram_gonder(token: str, chat_id: str, mesaj: str) -> tuple:
    """
    Mesaj gönder. (basari:bool, detay:str) döndürür.
    HTML parse_mode başarısız olursa düz metin ile tekrar dener.
    """
    if not token or not chat_id:
        return False, "Token veya Chat ID boş."
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for parse_mode in ["HTML", None]:
        try:
            payload = {"chat_id": chat_id, "text": mesaj}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            r = _requests.post(url, json=payload, timeout=10)
            data = r.json()
            if data.get("ok"):
                return True, "OK"
            # Telegram hata mesajı
            err = data.get("description", str(data))
            if parse_mode == "HTML":
                continue   # HTML sorunuysa düz metinle tekrar dene
            return False, err
        except Exception as e:
            return False, str(e)
    return False, "Bilinmeyen hata"

def telegram_test(token: str, chat_id: str) -> tuple:
    """Bot bilgisini çek + test mesajı gönder."""
    # 1. Bot geçerli mi?
    try:
        r = _requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=8)
        bot_data = r.json()
        if not bot_data.get("ok"):
            return False, f"Geçersiz token: {bot_data.get('description','?')}"
        bot_name = bot_data["result"].get("username","?")
    except Exception as e:
        return False, f"Bot bağlantı hatası: {e}"

    # 2. Test mesajı gönder
    ok, detay = telegram_gonder(
        token, chat_id,
        f"🦅 <b>Gümüş Avcısı</b> — Bağlantı testi başarılı!\n"
        f"Bot: @{bot_name}\nZaman: {simdi()}"
    )
    if ok:
        return True, f"✅ Mesaj gönderildi! Bot: @{bot_name}"
    return False, f"Bot OK ama mesaj gönderilemedi: {detay}"

def sinyal_mesaji_kisa(s: dict) -> str:
    ok = "▲" if s["degisim"] >= 0 else "▼"
    return (
        f"🦅 <b>Gümüş Avcısı</b> | {datetime.now(TURKEY_TZ).strftime('%H:%M')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>{s['ticker']}</b>  {ok}%{abs(s['degisim']):.2f}\n"
        f"💰 {s['fiyat']:.2f} TL  |  Skor: {s['puan']}/10\n"
        f"🟢 Giriş: {s['giris']:.2f}  🎯 Hedef: {s['hedef']:.2f}  🛑 Stop: {s['stop']:.2f}\n"
        f"⚖️ R/R: 1:{s['ror']}  |  Olasılık: %{s['olasilik']}\n"
        f"🔊 RVOL: x{s['rvol']}  📊 RSI: {s['rsi']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ Taramadır, yatırım tavsiyesi değildir."
    )

# ─────────────────────────────────────────────────────
# OTOMATİK TARAMA (APScheduler)
# ─────────────────────────────────────────────────────

def zamanli_tarama_yap(token: str, chat_id: str,
                        rvol_esik: float, rsi_alt: int, rsi_ust: int, min_puan: int):
    """Scheduler'ın çağırdığı fonksiyon — arka planda çalışır."""
    tickers  = FALLBACK_TICKERS[:60]  # İlk 60 hisse
    sonuclar = paralel_tara(tickers, rvol_esik, rsi_alt, rsi_ust, min_puan)

    if not sonuclar:
        telegram_gonder(token, chat_id,
            f"🦅 Gümüş Avcısı | {datetime.now(TURKEY_TZ).strftime('%H:%M')}\n"
            f"ℹ️ Bu taramada uygun sinyal bulunamadı.")
        return

    # Özet mesaj
    ozet = (
        f"🦅 <b>Gümüş Avcısı — Otomatik Tarama</b>\n"
        f"⏰ {datetime.now(TURKEY_TZ).strftime('%d.%m.%Y %H:%M')}\n"
        f"✅ {len(sonuclar)} sinyal bulundu\n\n"
    )
    for i, s in enumerate(sonuclar[:5], 1):
        ok = "▲" if s["degisim"] >= 0 else "▼"
        ozet += f"{i}. <b>{s['ticker']}</b> {ok}%{abs(s['degisim']):.2f} — Skor:{s['puan']}/10\n"
    telegram_gonder(token, chat_id, ozet)

    # Her sinyal için detay
    for s in sonuclar[:5]:
        telegram_gonder(token, chat_id, sinyal_mesaji_kisa(s))
        time.sleep(1)

@st.cache_resource
def get_scheduler():
    """Singleton scheduler — Streamlit her rerun'da yeniden yaratmaz."""
    scheduler = BackgroundScheduler(timezone=TURKEY_TZ)
    return scheduler

def scheduler_kur(token: str, chat_id: str,
                  rvol_esik: float, rsi_alt: int, rsi_ust: int, min_puan: int):
    scheduler = get_scheduler()
    # Mevcut job'ları temizle, yeniden ekle
    scheduler.remove_all_jobs()
    kwargs = dict(token=token, chat_id=chat_id, rvol_esik=rvol_esik,
                  rsi_alt=rsi_alt, rsi_ust=rsi_ust, min_puan=min_puan)
    for saat, dakika in [(10,30),(13,30),(17,30)]:
        scheduler.add_job(
            zamanli_tarama_yap,
            CronTrigger(hour=saat, minute=dakika, day_of_week="mon-fri"),
            kwargs=kwargs,
            id=f"tarama_{saat}{dakika:02d}",
            replace_existing=True,
        )
    if not scheduler.running:
        scheduler.start()
    return scheduler

@st.cache_data(ttl=300)
def hisse_listesi_yukle() -> List[str]:
    try:
        df  = bp.companies()
        lst = df["code"].dropna().tolist()
        return lst if lst else FALLBACK_TICKERS
    except Exception:
        try:
            # İkinci yöntem: Index bileşenleri
            return sorted(bp.Index("XU100").component_symbols)
        except Exception:
            return FALLBACK_TICKERS

@st.cache_data(ttl=60)
def veri_cek(ticker: str) -> Optional[pd.DataFrame]:
    try:
        df = bp.Ticker(ticker).history(period="5d", interval="5m")
        if df is None or df.empty:
            return None
        df.columns = [c.lower() for c in df.columns]
        for col in ["open","high","low","close","volume"]:
            if col not in df.columns:
                return None
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(TURKEY_TZ)
        else:
            df.index = df.index.tz_convert(TURKEY_TZ)
        return df.sort_index()  # ✅ Her zaman kronolojik sıra
    except Exception:
        return None

def son_islem_gunu(df: pd.DataFrame):
    return df.index.date[-1]

def fmt_sayi(val, suffix="") -> str:
    """None/NaN/string için güvenli format."""
    try:
        return f"{float(val):,.2f}{suffix}"
    except Exception:
        return "—"

def fmt_hacim(val) -> str:
    """✅ Hacim format crash fix."""
    try:
        return f"{int(float(val)):,}"
    except Exception:
        return str(val) if val else "—"


# ─────────────────────────────────────────────────────
# İNDİKATÖRLER
# ─────────────────────────────────────────────────────

def ind_rsi(close: pd.Series, p: int = 14) -> float:
    try:
        return round(float(bp.calculate_rsi(close, period=p).iloc[-1]), 2)
    except Exception:
        d = close.diff()
        g = d.where(d > 0, 0).rolling(p).mean()
        l = (-d.where(d < 0, 0)).rolling(p).mean()
        # ✅ Sıfıra bölme koruması
        l_last = float(l.iloc[-1])
        g_last = float(g.iloc[-1])
        if l_last == 0:
            return 100.0 if g_last > 0 else 50.0
        return round(100 - 100 / (1 + g_last / l_last), 2)

def ind_rsi_series(close: pd.Series, p: int = 14) -> pd.Series:
    """RSI serisi (grafik için)."""
    try:
        return bp.calculate_rsi(close, period=p)
    except Exception:
        d = close.diff()
        g = d.where(d > 0, 0).rolling(p).mean()
        l = (-d.where(d < 0, 0)).rolling(p).mean()
        rs = g / l.replace(0, float("nan"))
        return (100 - 100 / (1 + rs)).fillna(50)

def ind_ema(close: pd.Series, span: int) -> float:
    try:
        return round(float(bp.calculate_ema(close, period=span).iloc[-1]), 2)
    except Exception:
        return round(float(close.ewm(span=span, adjust=False).mean().iloc[-1]), 2)

def ind_ema_series(close: pd.Series, span: int) -> pd.Series:
    """EMA serisi (grafik için)."""
    try:
        return bp.calculate_ema(close, period=span)
    except Exception:
        return close.ewm(span=span, adjust=False).mean()

def ind_macd(close: pd.Series):
    try:
        dm = bp.calculate_macd(close)
        return round(float(dm["macd"].iloc[-1]), 4), round(float(dm["signal"].iloc[-1]), 4)
    except Exception:
        e12  = close.ewm(span=12, adjust=False).mean()
        e26  = close.ewm(span=26, adjust=False).mean()
        macd = e12 - e26
        sig  = macd.ewm(span=9, adjust=False).mean()
        return round(float(macd.iloc[-1]), 4), round(float(sig.iloc[-1]), 4)

def ind_atr(df: pd.DataFrame, p: int = 14) -> float:
    try:
        return round(float(bp.calculate_atr(df, period=p).iloc[-1]), 4)
    except Exception:
        h, l, c = df["high"], df["low"], df["close"]
        tr = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        return round(float(tr.rolling(p).mean().iloc[-1]), 4)

def ind_vwap_series(df: pd.DataFrame) -> pd.Series:
    try:
        return bp.calculate_vwap(df)
    except Exception:
        tp  = (df["high"] + df["low"] + df["close"]) / 3
        return (tp * df["volume"]).cumsum() / df["volume"].cumsum()

def ind_vwap(df: pd.DataFrame) -> float:
    return round(float(ind_vwap_series(df).iloc[-1]), 2)

def ind_rvol(df_tam: pd.DataFrame, df_bugun: pd.DataFrame) -> float:
    """Aynı saat dilimiyle karşılaştırmalı RVOL."""
    try:
        bugun       = df_bugun.index.date[0]
        n_bar       = len(df_bugun)
        onceki_gunler = sorted({d for d in df_tam.index.date if d < bugun})[-3:]
        if not onceki_gunler:
            return 1.0
        ortalamalar = []
        for g in onceki_gunler:
            d = df_tam[df_tam.index.date == g].sort_index()
            if len(d) >= n_bar:
                ortalamalar.append(float(d["volume"].iloc[:n_bar].sum()))
        if not ortalamalar:
            return 1.0
        bugun_vol = float(df_bugun["volume"].sum())
        ort = sum(ortalamalar) / len(ortalamalar)
        return round(bugun_vol / ort, 2) if ort > 0 else 1.0
    except Exception:
        vol = df_tam["volume"]
        if len(vol) < 27:
            return 1.0
        ort = float(vol.iloc[-27:-1].mean())
        return round(float(vol.iloc[-1]) / ort, 2) if ort > 0 else 1.0

def ind_bollinger(close: pd.Series, p: int = 20):
    """(genislik_pct, squeeze_mi)"""
    try:
        bb  = bp.calculate_bollinger_bands(close, period=p)
        ust = float(bb["upper"].iloc[-1])
        alt = float(bb["lower"].iloc[-1])
        mid = float(bb["middle"].iloc[-1])
        gen = (ust - alt) / mid * 100 if mid else 0
        return round(gen, 2), gen < 3.5
    except Exception:
        sma = close.rolling(p).mean()
        std = close.rolling(p).std()
        ust = sma + 2*std; alt = sma - 2*std
        gen = float(((ust-alt)/sma).iloc[-1]*100)
        return round(gen, 2), gen < 3.5

def aksiyon_etiketi(puan: int, rvol: float):
    if puan >= 8 and rvol >= 1.5:
        return "GÜÇLÜ AL", "badge-guclu"
    elif puan >= 6:
        return "TAKİP ET", "badge-takip"
    return "DİKKAT", "badge-dikkat"


# ─────────────────────────────────────────────────────
# ANA ANALİZ — try/except korumalı indikatörler
# ─────────────────────────────────────────────────────

def analiz_et(
    ticker: str,
    rvol_esik: float = 1.5,
    rsi_alt: int = 40,
    rsi_ust: int = 65,
    min_puan: int = 6,
) -> Optional[dict]:

    df = veri_cek(ticker)
    if df is None or len(df) < 30:
        return None

    gun  = son_islem_gunu(df)
    # ✅ sort_index() garantili — ORB doğru hesaplanır
    df_b = df[df.index.date == gun].sort_index().copy()
    if df_b.empty or len(df_b) < 6:
        return None

    fiyat   = round(float(df_b["close"].iloc[-1]), 2)
    df_d    = df[df.index.date < gun]
    onceki  = float(df_d["close"].iloc[-1]) if not df_d.empty else fiyat
    degisim = round(((fiyat - onceki) / onceki) * 100, 2)

    # ✅ Her indikatör try/except korumalı
    close = df["close"]
    try: rsi    = ind_rsi(close)
    except Exception: return None
    try: ema9   = ind_ema(close, 9)
    except Exception: return None
    try: ema21  = ind_ema(close, 21)
    except Exception: return None
    try: macd_v, macd_s = ind_macd(close)
    except Exception: return None
    try: atr    = ind_atr(df)
    except Exception: return None
    try: vwap   = ind_vwap(df_b)
    except Exception: return None
    try: rvol   = ind_rvol(df, df_b)
    except Exception: rvol = 1.0
    try: bb_gen, squeeze = ind_bollinger(close)
    except Exception: bb_gen, squeeze = 0.0, False

    orb_df = df_b.head(ORB_BARS)
    orb_h  = round(float(orb_df["high"].max()), 2)
    orb_l  = round(float(orb_df["low"].min()),  2)

    # ── Puanlama ──
    puan = 0; yon = "BEKLE"
    if orb_h > 0 and fiyat > orb_h:
        puan += 3; yon = "LONG"
    elif orb_l > 0 and fiyat < orb_l:
        yon = "SHORT"
    if fiyat > vwap:                puan += 2
    if rvol >= 2.0:                 puan += 2
    elif rvol >= rvol_esik:         puan += 1   # ✅ parametre kullanılıyor
    if rsi_alt <= rsi <= rsi_ust:   puan += 1   # ✅ parametre kullanılıyor
    if macd_v > macd_s:             puan += 1
    if ema9 > ema21:                puan += 1
    if squeeze:                     puan = min(puan+1, 10)
    puan = min(puan, 10)

    if yon != "LONG" or puan < min_puan:    # ✅ min_puan parametreden
        return None

    stop  = max(fiyat - atr*STOP_ATR, fiyat*(1 - MAX_STOP_PCT/100))
    hedef = fiyat + atr*HEDEF_ATR
    stop_pct  = round(((fiyat-stop)/fiyat)*100, 2)
    hedef_pct = round(((hedef-fiyat)/fiyat)*100, 2)
    if hedef_pct < MIN_HAREKET:
        return None

    # Olasılık
    baz  = 45 + puan*3.5
    baz += 5 if rvol >= 2.0 else (2 if rvol >= rvol_esik else 0)
    baz += 5   # ORB kırılımı bonusu
    baz += 3 if squeeze else 0
    olasilik = min(int(baz), 90)

    etiket, etiket_cls = aksiyon_etiketi(puan, rvol)

    return dict(
        ticker=ticker, fiyat=fiyat, degisim=degisim,
        rsi=rsi, ema9=ema9, ema21=ema21,
        macd=macd_v, macd_sig=macd_s,
        atr=atr, vwap=vwap, rvol=rvol,
        orb_h=orb_h, orb_l=orb_l,
        bb_gen=bb_gen, squeeze=squeeze,
        puan=puan, yon=yon, giris=fiyat,
        stop=round(stop,2), hedef=round(hedef,2),
        stop_pct=stop_pct, hedef_pct=hedef_pct,
        ror=round(hedef_pct/stop_pct, 2) if stop_pct > 0 else 0,
        olasilik=olasilik,
        etiket=etiket, etiket_cls=etiket_cls,
        df=df, df_bugun=df_b,
    )


# ─────────────────────────────────────────────────────
# PARALEL TARAMA
# ─────────────────────────────────────────────────────

def paralel_tara(tickers: List[str], rvol_esik, rsi_alt, rsi_ust, min_puan,
                 progress_cb=None) -> List[dict]:
    """✅ concurrent.futures ile paralel analiz."""
    sonuclar  = []
    tamamlanan = 0
    toplam    = len(tickers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALEL_IS) as ex:
        futures = {ex.submit(analiz_et, t, rvol_esik, rsi_alt, rsi_ust, min_puan): t
                   for t in tickers}
        for fut in concurrent.futures.as_completed(futures):
            tamamlanan += 1
            s = fut.result()
            if s:
                sonuclar.append(s)
            if progress_cb:
                progress_cb(tamamlanan / toplam, futures[fut], tamamlanan, toplam)

    sonuclar.sort(key=lambda x: (x["puan"], x["olasilik"]), reverse=True)
    return sonuclar


# ─────────────────────────────────────────────────────
# GRAFİK — EMA + RSI alt panel
# ─────────────────────────────────────────────────────

def mum_grafigi(df, ticker, vwap_val, orb_h, orb_l, giris, hedef, stop) -> go.Figure:
    gun  = son_islem_gunu(df)
    df_g = df[df.index.date == gun].sort_index().copy()

    # ✅ 3 satır: Mum | Hacim | RSI
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.60, 0.20, 0.20],
        vertical_spacing=0.03,
        subplot_titles=("", "Hacim", "RSI(14)"),
    )

    # ── Mumlar ──
    fig.add_trace(go.Candlestick(
        x=df_g.index, open=df_g["open"], high=df_g["high"],
        low=df_g["low"],  close=df_g["close"],
        increasing_line_color="#00ff88", increasing_fillcolor="#00ff88",
        decreasing_line_color="#ff4d4d", decreasing_fillcolor="#ff4d4d",
        line=dict(width=1), name=ticker,
    ), row=1, col=1)

    # ✅ EMA9 + EMA21 çizgileri
    ema9_s  = ind_ema_series(df_g["close"], 9)
    ema21_s = ind_ema_series(df_g["close"], 21)
    fig.add_trace(go.Scatter(x=df_g.index, y=ema9_s,  name="EMA9",
                             line=dict(color="#38bdf8", width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_g.index, y=ema21_s, name="EMA21",
                             line=dict(color="#e879f9", width=1)), row=1, col=1)

    # VWAP dinamik
    vwap_s = ind_vwap_series(df_g)
    fig.add_trace(go.Scatter(x=df_g.index, y=vwap_s, name="VWAP",
                             line=dict(color="#ffd700", width=1.5, dash="dot")), row=1, col=1)

    # Seviyeler
    for val, col, lbl in [
        (orb_h,  "#7c9fff", f"ORB H {orb_h:.2f}"),
        (orb_l,  "#7c9fff", f"ORB L {orb_l:.2f}"),
        (hedef,  "#00ff88", f"Hedef {hedef:.2f}"),
        (stop,   "#ff4d4d", f"Stop {stop:.2f}"),
    ]:
        fig.add_hline(y=val, line_color=col, line_dash="dash",
                      annotation_text=lbl, annotation_font_color=col, row=1, col=1)

    # ✅ Bug fix: Plotly 8-digit hex rengi kabul etmez → rgba kullan
    renkler = ["rgba(0,255,136,0.5)" if c >= o else "rgba(255,77,77,0.5)"
               for c, o in zip(df_g["close"], df_g["open"])]
    fig.add_trace(go.Bar(x=df_g.index, y=df_g["volume"],
                         marker_color=renkler, name="Hacim", showlegend=False), row=2, col=1)

    # ✅ RSI alt panel
    rsi_s = ind_rsi_series(df_g["close"])
    fig.add_trace(go.Scatter(x=df_g.index, y=rsi_s, name="RSI",
                             line=dict(color="#a3e635", width=1.2)), row=3, col=1)
    fig.add_hline(y=70, line_color="#ff4d4d", line_dash="dot", line_width=0.8, row=3, col=1)
    fig.add_hline(y=30, line_color="#00ff88", line_dash="dot", line_width=0.8, row=3, col=1)

    fig.update_layout(
        paper_bgcolor="#0d0f14", plot_bgcolor="#0d0f14",
        font_color="#cdd6f4", height=560,
        xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=20, b=10),
        legend=dict(orientation="h", y=1.02, bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(gridcolor="#21262d", zeroline=False)
    fig.update_yaxes(gridcolor="#21262d", zeroline=False)
    fig.update_yaxes(range=[0, 100], row=3, col=1)
    return fig


# ─────────────────────────────────────────────────────
# FX GÜVENLİ ÇEKME
# ─────────────────────────────────────────────────────

def fx_cek(sembol: str) -> str:
    try:
        val = bp.FX(sembol).current
        if val and float(val) > 0:
            return f"{float(val):.2f}"
    except Exception:
        pass
    try:
        df = bp.FX(sembol).history(period="1d")
        if df is not None and not df.empty:
            df.columns = [c.lower() for c in df.columns]
            return f"{float(df['close'].iloc[-1]):.2f}"
    except Exception:
        pass
    return "—"


# ─────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🦅 Gümüş Avcısı")
    st.caption(f"borsapy v{BP_VERSION} | {simdi()}")  # ✅ getattr

    acik, durum_txt = seans_durumu()
    st.markdown(f"**{durum_txt}**")
    st.divider()

    sayfa = st.radio("Sayfa", ["🔍 Hisse Analizi","📊 Piyasa Taraması","⚡ Hızlı Lookup"])
    st.divider()

    st.markdown("**⚙️ Strateji Ayarları**")
    rvol_esik = st.slider("RVOL Eşiği",  1.0, 3.0, 1.5, 0.1)
    rsi_alt   = st.slider("RSI Alt",     30,  55,  40)
    rsi_ust   = st.slider("RSI Üst",     55,  75,  65)
    min_puan  = st.slider("Min. Puan",   4,   9,   6)
    st.divider()

    # ✅ Otomatik sayfa yenileme
    otomatik = st.toggle("🔄 Otomatik Yenile (60s)", value=False)
    st.divider()

    # ── Telegram Bildirimleri ──
    st.markdown("**📲 Telegram Bildirimleri**")
    tg_token   = st.text_input("Bot Token",   type="password",
                                placeholder="123456:ABC-xyz...")
    tg_chat_id = st.text_input("Chat ID",
                                placeholder="-100123456 veya @kullanici")

    bildirim_aktif = st.toggle("🔔 Otomatik Tarama Aktif", value=False,
                                help="10:30 · 13:30 · 17:30 otomatik tarar ve Telegram'a gönderir")

    # ── Bağlantı testi — her zaman görünür ──
    if tg_token and tg_chat_id:
        if st.button("🔌 Bağlantı Test Et", use_container_width=True):
            with st.spinner("Test ediliyor..."):
                ok, mesaj_detay = telegram_test(tg_token, tg_chat_id)
            if ok:
                st.success(mesaj_detay)
            else:
                st.error(f"❌ {mesaj_detay}")
                st.info("💡 Olası nedenler:\n"
                        "• Chat ID yanlış — `/start` attıktan sonra "
                        "@userinfobot'tan ID'ni öğren\n"
                        "• Bot token yanlış — @BotFather'dan kontrol et\n"
                        "• HuggingFace dış HTTP erişimi kısıtlı olabilir")

    if bildirim_aktif and tg_token and tg_chat_id:
        sch = scheduler_kur(tg_token, tg_chat_id, rvol_esik, rsi_alt, rsi_ust, min_puan)
        jobs = sch.get_jobs()
        st.success(f"✅ {len(jobs)} tarama planlandı")
        for j in jobs:
            st.caption(f"⏰ {j.next_run_time.strftime('%H:%M') if j.next_run_time else '—'}")

        if st.button("▶ Şimdi Tara & Gönder", use_container_width=True):
            with st.spinner("Taranıyor..."):
                zamanli_tarama_yap(tg_token, tg_chat_id,
                                   rvol_esik, rsi_alt, rsi_ust, min_puan)
            st.info("Tarama tamamlandı — sonuç yukarıdaki test butonu ile kontrol et.")
    elif bildirim_aktif:
        st.warning("Token ve Chat ID gir.")

    st.divider()
    st.caption("⚠️ Yatırım tavsiyesi değildir.")

if otomatik:
    time.sleep(60)
    st.rerun()


# ─────────────────────────────────────────────────────
# BAŞLIK
# ─────────────────────────────────────────────────────

st.markdown("""
<div style='display:flex;align-items:center;gap:12px;margin-bottom:8px'>
  <span style='font-size:2rem'>🦅</span>
  <div>
    <h1 style='margin:0;font-size:1.6rem;color:#00ff88'>Gümüş Avcısı</h1>
    <p style='margin:0;color:#6e7681;font-size:0.85rem'>
      BIST Intraday Scanner · ORB + VWAP + RVOL + Bollinger · borsapy</p>
  </div>
</div>
""", unsafe_allow_html=True)
st.divider()


# ═══════════════════════════════════════════════════════
# SAYFA 1 — Hisse Analizi
# ═══════════════════════════════════════════════════════

if sayfa == "🔍 Hisse Analizi":
    hisseler = hisse_listesi_yukle()
    idx_def  = hisseler.index("THYAO") if "THYAO" in hisseler else 0

    col1, col2 = st.columns([3, 1])
    with col1:
        secilen = st.selectbox("Hisse Seç", hisseler, index=idx_def)
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        analiz_btn = st.button("▶ Analiz Et", use_container_width=True)

    # ✅ Ticker değişince eski sonucu temizle
    if st.session_state.get("son_secilen") != secilen:
        st.session_state.pop("analiz_sonuc", None)
        st.session_state["son_secilen"] = secilen

    if analiz_btn:
        with st.spinner(f"{secilen} analiz ediliyor..."):
            sonuc = analiz_et(secilen, rvol_esik, rsi_alt, rsi_ust, min_puan)
        st.session_state["analiz_sonuc"] = sonuc

    sonuc = st.session_state.get("analiz_sonuc")

    if sonuc is None and analiz_btn:
        st.warning(f"**{secilen}** için sinyal bulunamadı. ORB kırılımı yok veya puan yetersiz.")
    elif sonuc is not None:
        gun_str = sonuc["df_bugun"].index[0].strftime("%d.%m.%Y")
        st.markdown(
            f"<span class='{sonuc['etiket_cls']}'>{sonuc['etiket']}</span>"
            f"&nbsp; <span style='color:#6e7681;font-size:13px'>Son işlem: {gun_str}</span>"
            + ("&nbsp; 🔴 <b>Bollinger Sıkışması</b>" if sonuc["squeeze"] else ""),
            unsafe_allow_html=True
        )
        st.markdown("<br>", unsafe_allow_html=True)

        c1,c2,c3,c4,c5 = st.columns(5)
        ok = "▲" if sonuc["degisim"] >= 0 else "▼"
        c1.metric("Fiyat",    f"{sonuc['fiyat']:.2f} TL", f"{ok} %{abs(sonuc['degisim']):.2f}")
        c2.metric("Skor",     f"{sonuc['puan']}/10")
        c3.metric("Olasılık", f"%{sonuc['olasilik']}")
        c4.metric("RVOL",     f"x{sonuc['rvol']}")
        c5.metric("RSI",      f"{sonuc['rsi']}")

        c6,c7,c8,c9,c10 = st.columns(5)
        c6.metric("EMA9",    f"{sonuc['ema9']:.2f}")
        c7.metric("EMA21",   f"{sonuc['ema21']:.2f}")
        c8.metric("VWAP",    f"{sonuc['vwap']:.2f}")
        c9.metric("ATR",     f"{sonuc['atr']:.2f}")
        c10.metric("BB Gen.", f"%{sonuc['bb_gen']:.1f}")

        st.divider()

        col_g, col_p = st.columns([2, 1])
        with col_g:
            fig = mum_grafigi(
                sonuc["df"], sonuc["ticker"],
                sonuc["vwap"], sonuc["orb_h"], sonuc["orb_l"],
                sonuc["giris"], sonuc["hedef"], sonuc["stop"]
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_p:
            st.markdown(f"""
<div class='card'>
  <div style='font-size:1.05rem;font-weight:700;color:#00ff88;margin-bottom:10px'>📐 İşlem Planı</div>
  <table style='width:100%;font-size:0.9rem'>
    <tr><td style='color:#8b949e;padding:3px 0'>Yön</td>
        <td style='text-align:right;color:#00ff88;font-weight:700'>LONG ▲</td></tr>
    <tr><td style='color:#8b949e;padding:3px 0'>Giriş</td>
        <td style='text-align:right;color:#e2e8f0;font-weight:700'>{sonuc['giris']:.2f} TL</td></tr>
    <tr><td style='color:#00ff88;padding:3px 0'>🎯 Hedef</td>
        <td style='text-align:right;color:#00ff88;font-weight:700'>
          {sonuc['hedef']:.2f} TL<br>
          <span style='font-size:0.75rem'>+%{sonuc['hedef_pct']:.2f}</span></td></tr>
    <tr><td style='color:#ff4d4d;padding:3px 0'>🛑 Stop</td>
        <td style='text-align:right;color:#ff4d4d;font-weight:700'>
          {sonuc['stop']:.2f} TL<br>
          <span style='font-size:0.75rem'>-%{sonuc['stop_pct']:.2f}</span></td></tr>
    <tr><td style='color:#8b949e;padding:3px 0'>R/R</td>
        <td style='text-align:right;color:#ffd700;font-weight:700'>1:{sonuc['ror']}</td></tr>
    <tr><td style='color:#8b949e;padding:3px 0'>Olasılık</td>
        <td style='text-align:right;color:#ffd700;font-weight:700'>%{sonuc['olasilik']}</td></tr>
  </table>
</div>
""", unsafe_allow_html=True)

            # ✅ Pozisyon büyüklüğü hesaplayıcı
            with st.expander("🧮 Pozisyon Hesapla"):
                sermaye  = st.number_input("Sermaye (TL)", value=100_000, step=10_000)
                risk_pct = st.slider("Risk %", 0.5, 3.0, 1.0, 0.1)
                risk_tl  = sermaye * risk_pct / 100
                fark     = sonuc["giris"] - sonuc["stop"]
                if fark > 0:
                    lot     = int(risk_tl / fark)
                    maliyet = lot * sonuc["giris"]
                    kâr     = lot * (sonuc["hedef"] - sonuc["giris"])
                    zarar   = lot * fark
                    st.metric("Lot",     f"{lot:,}")
                    st.metric("Maliyet", f"{maliyet:,.0f} TL")
                    st.metric("Maks Kâr",  f"+{kâr:,.0f} TL")
                    st.metric("Maks Zarar",f"-{zarar:,.0f} TL")
                else:
                    st.warning("Stop seviyesi hesaplanamadı.")


# ═══════════════════════════════════════════════════════
# SAYFA 2 — Piyasa Taraması
# ═══════════════════════════════════════════════════════

elif sayfa == "📊 Piyasa Taraması":
    st.markdown("### 📊 Piyasa Taraması")
    if not acik:
        st.info("💡 Seans dışı — son kapanış verileri üzerinden analiz yapılır.")

    hisseler = hisse_listesi_yukle()
    col1, col2 = st.columns([3, 1])
    with col1:
        secilen_h = st.multiselect(
            "Hisse seç (boş bırakırsan ilk 50 hisse taranır)",
            options=hisseler, default=[]
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        tara_btn = st.button("⚡ Tara", use_container_width=True)

    if tara_btn:
        taranacak = secilen_h if secilen_h else hisseler[:50]
        progress  = st.progress(0, text="Taranıyor...")
        durum     = st.empty()

        def progress_cb(pct, ticker, done, total):
            durum.caption(f"🔍 {ticker} ({done}/{total})")
            progress.progress(pct)

        # ✅ Paralel tarama
        sonuclar = paralel_tara(taranacak, rvol_esik, rsi_alt, rsi_ust, min_puan, progress_cb)
        progress.empty(); durum.empty()

        if not sonuclar:
            st.info("Sinyal bulunamadı. Sidebar'dan Min. Puan'ı düşürmeyi dene.")
        else:
            st.success(f"✅ {len(sonuclar)} sinyal | {simdi()}")

            tablo = []
            for s in sonuclar:
                ok = "▲" if s["degisim"] >= 0 else "▼"
                tablo.append({
                    "Aksiyon"   : s["etiket"],
                    "Hisse"     : s["ticker"],
                    "Fiyat"     : f"{s['fiyat']:.2f}",
                    "Değişim"   : f"{ok}%{abs(s['degisim']):.2f}",
                    "Skor"      : s["puan"],
                    "Olasılık"  : f"%{s['olasilik']}",
                    "Giriş"     : s["giris"],
                    "Hedef"     : s["hedef"],
                    "Stop"      : s["stop"],
                    "R/R"       : f"1:{s['ror']}",
                    "RSI"       : s["rsi"],
                    "RVOL"      : f"x{s['rvol']}",
                    "BB Sıkışma": "🔴" if s["squeeze"] else "—",
                })

            df_tablo = pd.DataFrame(tablo)
            st.dataframe(
                df_tablo, use_container_width=True, hide_index=True,
                column_config={"Skor": st.column_config.ProgressColumn("Skor", min_value=0, max_value=10)}
            )

            # ✅ CSV export
            csv = df_tablo.drop(columns=["BB Sıkışma"]).to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ CSV İndir", csv,
                file_name=f"bist_tarama_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
            )

            st.markdown("---")
            for s in sonuclar[:5]:
                with st.expander(
                    f"{'🌟' if s['puan']>=8 else '📈'} {s['ticker']} — "
                    f"{s['etiket']} | {s['puan']}/10 | %{s['olasilik']}"
                ):
                    ca,cb,cc,cd = st.columns(4)
                    ca.metric("Giriş", f"{s['giris']:.2f} TL")
                    cb.metric("Hedef", f"{s['hedef']:.2f} TL", f"+%{s['hedef_pct']:.2f}")
                    cc.metric("Stop",  f"{s['stop']:.2f} TL",  f"-%{s['stop_pct']:.2f}")
                    cd.metric("RVOL",  f"x{s['rvol']}")
                    fig = mum_grafigi(s["df"], s["ticker"],
                                      s["vwap"], s["orb_h"], s["orb_l"],
                                      s["giris"], s["hedef"], s["stop"])
                    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════
# SAYFA 3 — Hızlı Lookup
# ═══════════════════════════════════════════════════════

elif sayfa == "⚡ Hızlı Lookup":
    st.markdown("### ⚡ Hızlı Hisse Lookup")

    girdi   = st.text_input("Hisseler (virgülle)", value="THYAO, GARAN, EREGL")
    ara_btn = st.button("🔍 Ara")

    if ara_btn and girdi:
        liste = [h.strip().upper() for h in girdi.split(",") if h.strip()]
        cols  = st.columns(min(len(liste), 3))
        for i, h in enumerate(liste):
            with cols[i % 3]:
                with st.spinner(h):
                    try:
                        info  = bp.Ticker(h).info
                        fiyat = info.get("last") or info.get("close") or "—"
                        deg   = float(info.get("change_percent", 0) or 0)
                        # ✅ Hacim format crash fix
                        hacim = fmt_hacim(info.get("volume"))
                        ok    = "▲" if deg >= 0 else "▼"
                        renk  = "#00ff88" if deg >= 0 else "#ff4d4d"
                        st.markdown(f"""
<div class='card'>
  <div style='font-size:1.2rem;font-weight:700;color:#00ff88'>{h}</div>
  <div style='font-size:1.6rem;font-weight:700;color:#e2e8f0'>{fiyat} TL</div>
  <div style='color:{renk}'>{ok} %{abs(deg):.2f}</div>
  <div style='color:#8b949e;font-size:0.8rem;margin-top:4px'>Hacim: {hacim}</div>
</div>""", unsafe_allow_html=True)
                    except Exception:
                        st.warning(f"{h}: Veri alınamadı")

    st.divider()

    # Döviz / Altın
    st.markdown("#### 💱 Anlık Döviz / Altın")
    fx_listesi = [("USD","USD/TL"),("EUR","EUR/TL"),("gram-altin","Gram Altın"),("ons-altin","Ons Altın")]
    fx_cols = st.columns(4)
    for col, (sembol, etiket) in zip(fx_cols, fx_listesi):
        col.metric(etiket, fx_cek(sembol))

    st.divider()

    # ✅ Makro — BIST100 eklendi
    st.markdown("#### 🏛 Makro")
    m1, m2, m3, m4 = st.columns(4)

    with m1:
        try:
            enf  = bp.Inflation().latest()
            tufe = enf.get("annual") or enf.get("yoy") or "—"
            st.metric("TÜFE (YoY)", f"%{tufe}")
        except Exception:
            st.metric("TÜFE", "—")

    with m2:
        try:
            st.metric("TCMB Faizi", f"%{bp.policy_rate()}")
        except Exception:
            st.metric("TCMB Faizi", "—")

    with m3:
        try:
            st.metric("Risksiz Oran", f"%{bp.risk_free_rate():.2f}")
        except Exception:
            st.metric("Risksiz Oran", "—")

    with m4:
        try:
            xu100 = bp.Ticker("XU100")
            bilgi = xu100.info
            xu_fiyat = bilgi.get("last") or bilgi.get("close") or "—"
            xu_deg   = float(bilgi.get("change_percent", 0) or 0)
            ok = "▲" if xu_deg >= 0 else "▼"
            st.metric("BIST100", f"{xu_fiyat}", f"{ok}%{abs(xu_deg):.2f}")
        except Exception:
            st.metric("BIST100", "—")
