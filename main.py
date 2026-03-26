# main.py - PARÇA 1/5
"""
Gümüş Avcısı Pro - FastAPI Edition
BIST Intraday Scanner API
"""

import os
import json
import sqlite3
import asyncio
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, time as dtime, timedelta, date
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from enum import Enum

import borsapy as bp
import pandas as pd
import numpy as np
import pytz
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import aiohttp
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gumus-avcisi")

# KONFIGÜRASYON
TURKEY_TZ = pytz.timezone("Europe/Istanbul")
DB_PATH = Path("data/gumus_avcisi.db")
DB_PATH.parent.mkdir(exist_ok=True)

# Sabitler
ORB_BARS = 6
STOP_ATR = 1.0
HEDEF_ATR = 2.0
MAX_STOP_PCT = 2.5
MIN_HAREKET = 0.4

FALLBACK_TICKERS = sorted(set([
    "THYAO", "BIMAS", "ASELS", "EREGL", "KCHOL", "GARAN", "ISCTR",
    "AKBNK", "YKBNK", "SASA", "TCELL", "ARCLK", "SISE", "ENKAI",
    "KOZAL", "DOHOL", "VESTL", "TUPRS", "ASTOR", "ENJSA", "GWIND",
    "FROTO", "TTRAK", "KONTR", "TUSAS", "PGSUS", "TAVHL", "MGROS",
    "SOKM", "ULKER", "CCOLA", "MPARK", "PETKM", "EREGL", "ISDMR"
]))
# main.py - PARÇA 2/5

class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._local = threading.local()
        self.init_db()
    
    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                target_price REAL NOT NULL,
                score INTEGER NOT NULL,
                rvol REAL, rsi REAL, atr REAL, vwap REAL,
                orb_high REAL, orb_low REAL, ema9 REAL, ema21 REAL,
                status TEXT DEFAULT 'ACTIVE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                triggered_at TIMESTAMP, result TEXT, pnl_realized REAL
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT UNIQUE NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                alert_enabled BOOLEAN DEFAULT TRUE,
                target_score INTEGER DEFAULT 6,
                notes TEXT
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                ticker TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                current_price REAL,
                stop_price REAL NOT NULL,
                target_price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                exit_time TIMESTAMP,
                exit_price REAL,
                pnl_unrealized REAL,
                pnl_realized REAL,
                status TEXT DEFAULT 'OPEN',
                FOREIGN KEY (signal_id) REFERENCES signals(id)
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS backtests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                strategy TEXT NOT NULL DEFAULT 'ORB_VWAP',
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                total_trades INTEGER,
                winning_trades INTEGER,
                losing_trades INTEGER,
                win_rate REAL,
                profit_factor REAL,
                net_pnl REAL,
                max_drawdown REAL,
                avg_trade_pnl REAL,
                sharpe_ratio REAL,
                params TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS price_cache (
                ticker TEXT PRIMARY KEY,
                price REAL,
                change_pct REAL,
                volume REAL,
                high_24h REAL,
                low_24h REAL,
                updated_at TIMESTAMP
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                ticker TEXT,
                message TEXT NOT NULL,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'PENDING',
                error_msg TEXT
            )
        ''')
        
        c.execute('CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized")
    
    def execute(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        conn = self._get_conn()
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()
        return c.fetchall()
    
    def fetchone(self, query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        conn = self._get_conn()
        c = conn.cursor()
        c.execute(query, params)
        return c.fetchone()
    
    def fetchall(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        conn = self._get_conn()
        c = conn.cursor()
        c.execute(query, params)
        return c.fetchall()

db = Database(DB_PATH)
# main.py - PARÇA 3/5

class TechnicalIndicators:
    @staticmethod
    def rsi(close: pd.Series, period: int = 14) -> float:
        try:
            return round(float(bp.calculate_rsi(close, period=period).iloc[-1]), 2)
        except Exception:
            d = close.diff()
            g = d.where(d > 0, 0).rolling(period).mean()
            l = (-d.where(d < 0, 0)).rolling(period).mean()
            l_last = float(l.iloc[-1])
            g_last = float(g.iloc[-1])
            if l_last == 0:
                return 100.0 if g_last > 0 else 50.0
            return round(100 - 100 / (1 + g_last / l_last), 2)
    
    @staticmethod
    def ema(close: pd.Series, span: int) -> float:
        try:
            return round(float(bp.calculate_ema(close, period=span).iloc[-1]), 2)
        except Exception:
            return round(float(close.ewm(span=span, adjust=False).mean().iloc[-1]), 2)
    
    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> float:
        try:
            return round(float(bp.calculate_atr(df, period=period).iloc[-1]), 4)
        except Exception:
            h, l, c = df["high"], df["low"], df["close"]
            tr = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
            return round(float(tr.rolling(period).mean().iloc[-1]), 4)
    
    @staticmethod
    def vwap(df: pd.DataFrame) -> float:
        try:
            return round(float(bp.calculate_vwap(df).iloc[-1]), 2)
        except Exception:
            tp = (df["high"] + df["low"] + df["close"]) / 3
            return round(float((tp * df["volume"]).cumsum().iloc[-1] / df["volume"].cumsum().iloc[-1]), 2)
    
    @staticmethod
    def rvol(df_full: pd.DataFrame, df_today: pd.DataFrame) -> float:
        try:
            today = df_today.index.date[0]
            n_bars = len(df_today)
            prev_days = sorted({d for d in df_full.index.date if d < today})[-3:]
            if not prev_days:
                return 1.0
            averages = []
            for d in prev_days:
                day_data = df_full[df_full.index.date == d].sort_index()
                if len(day_data) >= n_bars:
                    averages.append(float(day_data["volume"].iloc[:n_bars].sum()))
            if not averages:
                return 1.0
            today_vol = float(df_today["volume"].sum())
            avg = sum(averages) / len(averages)
            return round(today_vol / avg, 2) if avg > 0 else 1.0
        except Exception:
            vol = df_full["volume"]
            if len(vol) < 27:
                return 1.0
            avg = float(vol.iloc[-27:-1].mean())
            return round(float(vol.iloc[-1]) / avg, 2) if avg > 0 else 1.0


class DataFetcher:
    def __init__(self):
        self.tz = TURKEY_TZ
        self._cache: Dict[str, tuple] = {}
        self._cache_ttl = 30
    
    def get_data(self, ticker: str, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        if not force_refresh and ticker in self._cache:
            data, timestamp = self._cache[ticker]
            if (datetime.now() - timestamp).seconds < self._cache_ttl:
                return data
        
        try:
            df = bp.Ticker(ticker).history(period="5d", interval="5m")
            if df is None or df.empty:
                return None
            
            df.columns = [c.lower() for c in df.columns]
            for col in ["open", "high", "low", "close", "volume"]:
                if col not in df.columns:
                    return None
            
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC").tz_convert(self.tz)
            else:
                df.index = df.index.tz_convert(self.tz)
            
            df = df.sort_index()
            self._cache[ticker] = (df, datetime.now())
            return df
        except Exception as e:
            logger.error(f"Data fetch error for {ticker}: {e}")
            return None
    
    def get_current_price(self, ticker: str) -> Optional[float]:
        df = self.get_data(ticker)
        if df is not None and not df.empty:
            return float(df["close"].iloc[-1])
        return None

data_fetcher = DataFetcher()


class StrategyEngine:
    def __init__(self):
        self.indicators = TechnicalIndicators()
    
    def analyze(self, ticker: str, rvol_threshold: float = 1.5,
                rsi_low: int = 40, rsi_high: int = 65, min_score: int = 6) -> Optional[Dict]:
        df = data_fetcher.get_data(ticker)
        if df is None or len(df) < 30:
            return None
        
        today = df.index.date[-1]
        df_today = df[df.index.date == today].sort_index().copy()
        if df_today.empty or len(df_today) < 6:
            return None
        
        current_price = round(float(df_today["close"].iloc[-1]), 2)
        
        df_prev = df[df.index.date < today]
        prev_close = float(df_prev["close"].iloc[-1]) if not df_prev.empty else current_price
        change_pct = round(((current_price - prev_close) / prev_close) * 100, 2)
        
        close = df["close"]
        try:
            rsi = self.indicators.rsi(close)
            ema9 = self.indicators.ema(close, 9)
            ema21 = self.indicators.ema(close, 21)
            atr = self.indicators.atr(df)
            vwap = self.indicators.vwap(df_today)
            rvol = self.indicators.rvol(df, df_today)
        except Exception as e:
            logger.error(f"Indicator error for {ticker}: {e}")
            return None
        
        orb_data = df_today.head(ORB_BARS)
        orb_high = round(float(orb_data["high"].max()), 2)
        orb_low = round(float(orb_data["low"].min()), 2)
        
        score = 0
        direction = "NEUTRAL"
        
        if orb_high > 0 and current_price > orb_high:
            score += 3
            direction = "LONG"
        elif orb_low > 0 and current_price < orb_low:
            score += 3
            direction = "SHORT"
        
        if current_price > vwap:
            score += 2
        if rvol >= 2.0:
            score += 2
        elif rvol >= rvol_threshold:
            score += 1
        if rsi_low <= rsi <= rsi_high:
            score += 1
        if ema9 > ema21:
            score += 1
        
        score = min(score, 10)
        
        if direction == "NEUTRAL" or score < min_score:
            return None
        
        if direction == "LONG":
            stop = max(current_price - atr * STOP_ATR, current_price * (1 - MAX_STOP_PCT / 100))
            target = current_price + atr * HEDEF_ATR
        else:
            stop = min(current_price + atr * STOP_ATR, current_price * (1 + MAX_STOP_PCT / 100))
            target = current_price - atr * HEDEF_ATR
        
        stop = round(stop, 2)
        target = round(target, 2)
        
        stop_pct = abs(round(((current_price - stop) / current_price) * 100, 2))
        target_pct = abs(round(((target - current_price) / current_price) * 100, 2))
        
        if target_pct < MIN_HAREKET:
            return None
        
        rr = round(target_pct / stop_pct, 2) if stop_pct > 0 else 0
        
        return {
            "ticker": ticker,
            "direction": direction,
            "price": current_price,
            "change_pct": change_pct,
            "entry": current_price,
            "stop": stop,
            "target": target,
            "stop_pct": stop_pct,
            "target_pct": target_pct,
            "rr": rr,
            "score": score,
            "indicators": {
                "rsi": rsi,
                "ema9": ema9,
                "ema21": ema21,
                "atr": atr,
                "vwap": vwap,
                "rvol": rvol,
                "orb_high": orb_high,
                "orb_low": orb_low
            }
        }

strategy_engine = StrategyEngine()
  # main.py - PARÇA 4/5

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.subscriptions: Dict[str, Set[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        for ticker, subs in self.subscriptions.items():
            subs.discard(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")
    
    async def subscribe(self, websocket: WebSocket, ticker: str):
        if ticker not in self.subscriptions:
            self.subscriptions[ticker] = set()
        self.subscriptions[ticker].add(websocket)
        logger.info(f"Subscribed to {ticker}")
    
    async def broadcast(self, message: Dict, ticker: Optional[str] = None):
        if ticker and ticker in self.subscriptions:
            disconnected = []
            for conn in self.subscriptions[ticker]:
                try:
                    await conn.send_json(message)
                except:
                    disconnected.append(conn)
            for conn in disconnected:
                self.disconnect(conn)
        else:
            disconnected = []
            for conn in self.active_connections:
                try:
                    await conn.send_json(message)
                except:
                    disconnected.append(conn)
            for conn in disconnected:
                self.disconnect(conn)

ws_manager = ConnectionManager()


async def live_price_feed():
    while True:
        try:
            tickers = list(ws_manager.subscriptions.keys())
            if not tickers:
                await asyncio.sleep(5)
                continue
            
            for ticker in tickers:
                price = data_fetcher.get_current_price(ticker)
                if price:
                    await ws_manager.broadcast({
                        "type": "price",
                        "ticker": ticker,
                        "price": price,
                        "timestamp": datetime.now(TURKEY_TZ).isoformat()
                    }, ticker)
            
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Live feed error: {e}")
            await asyncio.sleep(5)


async def send_telegram_alert(signal: Dict):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        return
    
    message = f"""
🦅 <b>Gümüş Avcısı Sinyal</b>

📈 <b>{signal['ticker']}</b> | {signal['direction']}
💰 Giriş: {signal['entry']:.2f} ₺
🎯 Hedef: {signal['target']:.2f} ₺  
🛑 Stop: {signal['stop']:.2f} ₺
⚖️ R/R: 1:{signal['rr']}
⭐ Skor: {signal['score']}/10

📊 RVOL: {signal['indicators']['rvol']}x | RSI: {signal['indicators']['rsi']}
    """
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            await session.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            })
        db.execute("INSERT INTO alerts (type, ticker, message, status) VALUES (?, ?, ?, ?)",
                   ("TELEGRAM", signal["ticker"], message, "SENT"))
    except Exception as e:
        logger.error(f"Telegram error: {e}")
        db.execute("INSERT INTO alerts (type, ticker, message, status, error_msg) VALUES (?, ?, ?, ?, ?)",
                   ("TELEGRAM", signal["ticker"], message, "FAILED", str(e)))


async def auto_scan():
    logger.info("Otomatik tarama başladı")
    
    watchlist = db.fetchall("SELECT ticker FROM watchlist WHERE alert_enabled = 1")
    tickers = [w["ticker"] for w in watchlist] or FALLBACK_TICKERS[:20]
    
    signals_found = []
    
    for ticker in tickers:
        try:
            result = strategy_engine.analyze(ticker)
            if result:
                db.execute('''
                    INSERT INTO signals 
                    (ticker, direction, entry_price, stop_price, target_price,
                     score, rvol, rsi, atr, vwap, orb_high, orb_low, ema9, ema21)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    result["ticker"], result["direction"], result["entry"],
                    result["stop"], result["target"], result["score"],
                    result["indicators"]["rvol"], result["indicators"]["rsi"],
                    result["indicators"]["atr"], result["indicators"]["vwap"],
                    result["indicators"]["orb_high"], result["indicators"]["orb_low"],
                    result["indicators"]["ema9"], result["indicators"]["ema21"]
                ))
                
                signals_found.append(result)
                
                await ws_manager.broadcast({
                    "type": "new_signal",
                    "signal": {
                        "ticker": result["ticker"],
                        "direction": result["direction"],
                        "entry": result["entry"],
                        "score": result["score"]
                    }
                })
                
                await send_telegram_alert(result)
        except Exception as e:
            logger.error(f"Scan error for {ticker}: {e}")
        
        await asyncio.sleep(0.5)
    
    logger.info(f"Tarama tamamlandı: {len(signals_found)} sinyal")
    return signals_found
              # main.py - PARÇA 5/5

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler(timezone=TURKEY_TZ)
    
    scheduler.add_job(
        lambda: asyncio.create_task(auto_scan()),
        CronTrigger(hour="10,13,17", minute="30", day_of_week="mon-fri"),
        id="auto_scan",
        replace_existing=True
    )
    
    scheduler.add_job(
        lambda: asyncio.create_task(update_price_cache()),
        "interval", minutes=1,
        id="price_update"
    )
    
    scheduler.start()
    feed_task = asyncio.create_task(live_price_feed())
    
    logger.info("🚀 Gümüş Avcısı Pro başlatıldı")
    
    yield
    
    scheduler.shutdown()
    feed_task.cancel()
    logger.info("👋 Gümüş Avcısı Pro durduruldu")

app = FastAPI(
    title="Gümüş Avcısı Pro",
    description="BIST Intraday Scanner API",
    version="6.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def update_price_cache():
    try:
        tickers = FALLBACK_TICKERS[:10]
        for ticker in tickers:
            price = data_fetcher.get_current_price(ticker)
            if price:
                db.execute('''
                    INSERT OR REPLACE INTO price_cache 
                    (ticker, price, updated_at) VALUES (?, ?, ?)
                ''', (ticker, price, datetime.now()))
    except Exception as e:
        logger.error(f"Price cache error: {e}")


@app.get("/api/signals")
async def get_signals(limit: int = 50, status: Optional[str] = None):
    query = 'SELECT * FROM signals WHERE 1=1'
    params = []
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    
    rows = db.fetchall(query, tuple(params))
    return [{
        "id": r["id"],
        "ticker": r["ticker"],
        "direction": r["direction"],
        "entry": r["entry_price"],
        "stop": r["stop_price"],
        "target": r["target_price"],
        "score": r["score"],
        "rr": f"1:{round((r['target_price'] - r['entry_price']) / (r['entry_price'] - r['stop_price']), 2)}" if r["entry_price"] != r["stop_price"] else "N/A",
        "indicators": {"rvol": r["rvol"], "rsi": r["rsi"], "atr": r["atr"], "vwap": r["vwap"]},
        "status": r["status"],
        "created_at": r["created_at"]
    } for r in rows]


@app.get("/api/watchlist")
async def get_watchlist():
    rows = db.fetchall('''
        SELECT w.*, pc.price, pc.change_pct 
        FROM watchlist w
        LEFT JOIN price_cache pc ON w.ticker = pc.ticker
        ORDER BY w.added_at DESC
    ''')
    return [{
        "id": r["id"],
        "ticker": r["ticker"],
        "added_at": r["added_at"],
        "alert_enabled": bool(r["alert_enabled"]),
        "target_score": r["target_score"],
        "current_price": r["price"],
        "change_pct": r["change_pct"],
        "notes": r["notes"]
    } for r in rows]


@app.post("/api/watchlist/{ticker}")
async def add_to_watchlist(ticker: str, target_score: int = 6, notes: str = ""):
    try:
        db.execute('''
            INSERT OR REPLACE INTO watchlist (ticker, target_score, notes, alert_enabled)
            VALUES (?, ?, ?, 1)
        ''', (ticker.upper(), target_score, notes))
        return {"success": True, "ticker": ticker.upper()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/portfolio")
async def get_portfolio():
    positions_rows = db.fetchall('''
        SELECT p.*, s.score as signal_score
        FROM positions p
        LEFT JOIN signals s ON p.signal_id = s.id
        WHERE p.status = 'OPEN'
        ORDER BY p.entry_time DESC
    ''')
    
    positions = []
    total_pnl = 0
    
    for r in positions_rows:
        current = data_fetcher.get_current_price(r["ticker"]) or r["entry_price"]
        
        if r["direction"] == "LONG":
            pnl_unrealized = (current - r["entry_price"]) * r["quantity"]
            pnl_pct = ((current - r["entry_price"]) / r["entry_price"]) * 100
        else:
            pnl_unrealized = (r["entry_price"] - current) * r["quantity"]
            pnl_pct = ((r["entry_price"] - current) / r["entry_price"]) * 100
        
        total_pnl += pnl_unrealized
        
        positions.append({
            "id": r["id"],
            "ticker": r["ticker"],
            "direction": r["direction"],
            "entry_price": r["entry_price"],
            "current_price": round(current, 2),
            "stop_price": r["stop_price"],
            "target_price": r["target_price"],
            "quantity": r["quantity"],
            "pnl_unrealized": round(pnl_unrealized, 2),
            "pnl_pct": round(pnl_pct, 2),
            "entry_time": r["entry_time"],
            "signal_score": r["signal_score"]
        })
    
    closed_stats = db.fetchone('''
        SELECT COUNT(*) as count, SUM(pnl_realized) as total_pnl
        FROM positions WHERE status = 'CLOSED'
    ''')
    
    return {
        "open_positions": positions,
        "summary": {
            "open_count": len(positions),
            "open_pnl": round(total_pnl, 2),
            "closed_count": closed_stats["count"] if closed_stats else 0,
            "closed_pnl": round(closed_stats["total_pnl"] or 0, 2),
            "total_pnl": round(total_pnl + (closed_stats["total_pnl"] or 0), 2)
        }
    }


@app.post("/api/portfolio/position")
async def open_position(signal_id: int, quantity: int):
    signal = db.fetchone("SELECT * FROM signals WHERE id = ?", (signal_id,))
    if not signal:
        raise HTTPException(status_code=404, detail="Sinyal bulunamadı")
    
    db.execute('''
        INSERT INTO positions 
        (signal_id, ticker, direction, entry_price, stop_price, target_price, quantity)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        signal_id, signal["ticker"], signal["direction"],
        signal["entry_price"], signal["stop_price"], 
        signal["target_price"], quantity
    ))
    
    db.execute("UPDATE signals SET status = 'TRIGGERED', triggered_at = ? WHERE id = ?",
               (datetime.now(), signal_id))
    
    return {"success": True, "ticker": signal["ticker"], "quantity": quantity}


@app.get("/api/backtest/{ticker}")
async def get_backtest(ticker: str, days: int = 30):
    cached = db.fetchone('''
        SELECT * FROM backtests 
        WHERE ticker = ? AND end_date = ?
        ORDER BY created_at DESC LIMIT 1
    ''', (ticker.upper(), str(date.today())))
    
    if cached and (datetime.now() - datetime.fromisoformat(cached["created_at"])).days < 1:
        return {
            "cached": True,
            "ticker": cached["ticker"],
            "strategy": cached["strategy"],
            "period": f"{cached['start_date']} to {cached['end_date']}",
            "metrics": {
                "total_trades": cached["total_trades"],
                "win_rate": cached["win_rate"],
                "profit_factor": cached["profit_factor"],
                "net_pnl": cached["net_pnl"],
                "max_drawdown": cached["max_drawdown"]
            }
        }
    
    return {"message": "Backtest çalıştırılıyor", "use_force": True}


@app.post("/api/alert")
async def send_alert(type: str, message: str, ticker: Optional[str] = None):
    alert_id = db.execute(
        "INSERT INTO alerts (type, ticker, message, status) VALUES (?, ?, ?, ?)",
        (type, ticker, message, "PENDING")
    )
    
    if type == "TELEGRAM":
        bot_token = os.getenv("8727724989:AAGoSlqnsv7MOGrR_uHypmMzVEnu8zKKbl8")
        chat_id = os.getenv("5023593717")
        
        if bot_token and chat_id:
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    await session.post(url, json={
                        "chat_id": chat_id,
                        "text": f"🦅 Gümüş Avcısı\n\n{message}",
                        "parse_mode": "HTML"
                    })
                db.execute("UPDATE alerts SET status = 'SENT' WHERE id = ?", (alert_id,))
                return {"success": True, "channel": "telegram"}
            except Exception as e:
                db.execute("UPDATE alerts SET status = 'FAILED', error_msg = ? WHERE id = ?",
                          (str(e), alert_id))
                raise HTTPException(status_code=500, detail=str(e))
    
    return {"success": True, "alert_id": alert_id, "type": type}


@app.post("/api/scan")
async def manual_scan(background_tasks: BackgroundTasks):
    background_tasks.add_task(auto_scan)
    return {"message": "Tarama başlatıldı"}


@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            
            if data.get("action") == "subscribe":
                ticker = data.get("ticker", "THYAO")
                await ws_manager.subscribe(websocket, ticker.upper())
                price = data_fetcher.get_current_price(ticker)
                await websocket.send_json({
                    "type": "subscribed",
                    "ticker": ticker,
                    "price": price
                })
            elif data.get("action") == "ping":
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket)


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(TURKEY_TZ).isoformat(),
        "version": "6.0.0",
        "connections": len(ws_manager.active_connections)
    }


@app.get("/")
async def root():
    return {
        "name": "Gümüş Avcısı Pro",
        "version": "6.0.0",
        "endpoints": ["/api/signals", "/api/watchlist", "/api/portfolio", 
                     "/api/backtest/{ticker}", "/api/alert", "/ws/live"]
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
