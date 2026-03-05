"""
PolyEdge v2.1 — EV+ Signal Engine (Fix: odds-api.io compatibility)
Polymarket Gamma API  ↔  odds-api.io (Superbet, Betclic PL)
"""

import os
import re
import uuid
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from rapidfuzz import fuzz, process
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ODDS_API_KEY: str = os.environ.get("ODDS_API_KEY", "")
ODDS_API_BASE: str = "https://api.odds-api.io/v3"

# Wybrani bukmacherzy (Darmowy plan: max 2)
BOOKMAKERS: list[str] = ["Superbet", "Betclic PL"]

# Formaty dla odds-api.io: (sport, league)
ACTIVE_LEAGUES = [
    # PIŁKA NOŻNA
    ("football", "england-premier-league"),
    ("football", "germany-bundesliga"),
    ("football", "spain-laliga"),
    ("football", "italy-serie-a"),
    ("football", "france-ligue-1"),
    ("football", "poland-ekstraklasa"),
    ("football", "international-clubs-uefa-champions-league"),
    # TENIS
    ("tennis", "atp-atp-indian-wells-usa-men-singles"),
    ("tennis", "wta-wta-indian-wells-usa-women-singles"),
    # KOSZYKÓWKA
    ("basketball", "usa-nba"),
    ("basketball", "poland-plk"),
    ("basketball", "international-euroleague"),
    ("basketball", "spain-liga-acb"),
]
]

POLYMARKET_GAMMA_URL: str = "https://gamma-api.polymarket.com/markets"

# Parametry podatkowe 2026
STAKE_TAX_STANDARD: float = 0.12       # 12% standard
PIT_THRESHOLD_PLN: float = 2280.0      # Próg 10% PIT
PIT_RATE: float = 0.10

BANKROLL_PLN: float = 2500.0           
KELLY_FRACTION: float = 0.25           
MIN_EV_PERCENT: float = 2.0            
REFRESH_MINUTES: int = 10              

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-7s │ %(message)s")
log = logging.getLogger("polyedge")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MODELS & LOGIC
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PolyOutcome(BaseModel):
    event_title: str
    outcome_label: str
    poly_prob: float

class BookieOutcome(BaseModel):
    sport: str
    event_id: str
    event_name: str
    selection: str
    decimal_odds: float
    bookmaker: str

class EVSignal(BaseModel):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:8])
    ts: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%H:%M"))
    poly_event: str
    poly_outcome: str
    poly_prob: float
    bookmaker: str
    bookie_event: str
    bookie_selection: str
    bookie_odds: float
    ev_pct: float
    kelly_stake: float
    status: str = "shadow"

def calc_ev_pl(poly_prob: float, odds: float, bookmaker: str, stake: float = 100.0) -> tuple[float, float]:
    """Oblicza EV uwzględniając, że Betclic PL często ma 0% podatku od stawki."""
    # Betclic w 2026 często utrzymuje "bez podatku" (tax 0%). Superbet ma 12%.
    current_stake_tax = 0.0 if "Betclic" in bookmaker else STAKE_TAX_STANDARD
    mult = 1.0 - current_stake_tax
    
    eff_stake = stake * mult
    gross_return = eff_stake * odds
    gross_profit = gross_return - stake

    if gross_return > PIT_THRESHOLD_PLN:
        net_profit = gross_profit - (gross_return * PIT_RATE)
    else:
        net_profit = gross_profit

    ev = (poly_prob * net_profit) - ((1.0 - poly_prob) * stake)
    return round((ev / stake) * 100, 2), round(net_profit, 2)

def get_kelly(poly_prob: float, odds: float, bookmaker: str) -> float:
    current_stake_tax = 0.0 if "Betclic" in bookmaker else STAKE_TAX_STANDARD
    eff_odds = odds * (1.0 - current_stake_tax)
    b = eff_odds - 1.0
    if b <= 0: return 0.0
    
    f_star = (poly_prob * b - (1 - poly_prob)) / b
    if f_star <= 0: return 0.0
    
    stake = BANKROLL_PLN * f_star * KELLY_FRACTION
    # Optymalizacja pod próg PIT
    if (stake * (1.0 - current_stake_tax) * odds) > PIT_THRESHOLD_PLN:
        stake = PIT_THRESHOLD_PLN / ((1.0 - current_stake_tax) * odds)
    
    return round(max(0, min(stake, BANKROLL_PLN * 0.15)), 2)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API FETCHERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_polymarket() -> list[PolyOutcome]:
    results = []
    tags = ["sports", "nfl", "nba", "soccer", "ufc"]
    async with httpx.AsyncClient(timeout=20.0) as client:
        for tag in tags:
            try:
                r = await client.get(POLYMARKET_GAMMA_URL, params={"tag": tag, "closed": "false", "limit": 50})
                if r.status_code == 200:
                    for m in r.json():
                        prices = json.loads(m.get("outcomePrices", "[]"))
                        outcomes = json.loads(m.get("outcomes", "[]"))
                        for i, label in enumerate(outcomes):
                            if i < len(prices) and 0.1 < float(prices[i]) < 0.9:
                                results.append(PolyOutcome(
                                    event_title=m.get("question", ""),
                                    outcome_label=label,
                                    poly_prob=float(prices[i])
                                ))
            except: continue
    return results

async def fetch_odds_api() -> list[BookieOutcome]:
    if not ODDS_API_KEY: return []
    all_outcomes = []
    async with httpx.AsyncClient(timeout=25.0) as client:
        for sport, league in ACTIVE_LEAGUES:
            try:
                # 1. Pobierz listę meczów
                ev_r = await client.get(f"{ODDS_API_BASE}/events", params={
                    "apiKey": ODDS_API_KEY, "sport": sport, "league": league
                })
                if ev_r.status_code != 200: continue
                
                events = ev_r.json()
                if not isinstance(events, list): continue

                for ev in events[:10]: # Bierzemy 10 najświeższych meczów
                    eid = ev.get("id")
                    if not eid: continue

                    # 2. Pobierz kursy
                    o_r = await client.get(f"{ODDS_API_BASE}/odds", params={
                        "apiKey": ODDS_API_KEY, "eventId": eid, "bookmakers": ",".join(BOOKMAKERS)
                    })
                    if o_r.status_code != 200: continue
                    
                    # Krytyczna poprawka formatu danych
                    try:
                        raw_text = o_r.text
                        data = json.loads(raw_text) if isinstance(raw_text, str) else o_r.json()
                    except: continue
                    
                    # Wyciąganie bukmacherów (Superbet, Betclic PL)
                    bk_list = data.get("bookmakers", []) if isinstance(data, dict) else data
                    if not isinstance(bk_list, list): continue

                    for bk in bk_list:
                        bk_name = bk.get("name", "Unknown")
                        for mkt in bk.get("markets", []):
                            # Pobieramy wyniki (zwycięzca meczu)
                            outcomes = mkt.get("outcomes", mkt.get("selections", []))
                            for oc in outcomes:
                                try:
                                    all_outcomes.append(BookieOutcome(
                                        sport=sport, event_id=str(eid),
                                        event_name=f"{ev.get('home')} vs {ev.get('away')}",
                                        selection=oc.get("name"),
                                        decimal_odds=float(oc.get("price", 0)),
                                        bookmaker=bk_name
                                    ))
                                except: continue
            except Exception as e:
                log.warning(f"Błąd {league}: {str(e)}")
    log.info(f"Pipeline: Pobrano {len(all_outcomes)} kursów.")
    return all_outcomes

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CORE PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

signals = []
shadow_bets = []

async def run_pipeline():
    global signals, shadow_bets
    log.info("--- Pipeline Start ---")
    poly = await fetch_polymarket()
    bookie = await fetch_odds_api()
    
    new_signals = []
    if poly and bookie:
        bk_strings = [f"{b.event_name} {b.selection}".lower() for b in bookie]
        for p in poly:
            p_str = f"{p.event_title} {p.outcome_label}".lower()
            match = process.extractOne(p_str, bk_strings, scorer=fuzz.token_set_ratio, score_cutoff=65)
            if match:
                idx = match[2]
                target = bookie[idx]
                ev, _ = calc_ev_pl(p.poly_prob, target.decimal_odds, target.bookmaker)
                
                if ev >= MIN_EV_PERCENT:
                    sig = EVSignal(
                        poly_event=p.event_title, poly_outcome=p.outcome_label,
                        poly_prob=p.poly_prob, bookmaker=target.bookmaker,
                        bookie_event=target.event_name, bookie_selection=target.selection,
                        bookie_odds=target.decimal_odds, ev_pct=ev,
                        kelly_stake=get_kelly(p.poly_prob, target.decimal_odds, target.bookmaker)
                    )
                    new_signals.append(sig)
                    
                    # Auto-shadow
                    if not any(sb.poly_event == sig.poly_event for sb in shadow_bets):
                        shadow_bets.append(sig.model_copy())

    signals = sorted(new_signals, key=lambda x: x.ev_pct, reverse=True)
    log.info(f"Pipeline End: Found {len(signals)} signals")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FASTAPI APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_pipeline, "interval", minutes=REFRESH_MINUTES)
    scheduler.start()
    await run_pipeline()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request, "signals": signals, "shadow_bets": shadow_bets
    })

@app.get("/api/signals")
async def get_api_signals():
    return {"signals": signals}

@app.post("/api/refresh")
async def manual_refresh():
    await run_pipeline()
    return {"status": "ok"}
