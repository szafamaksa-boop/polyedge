import os
import uuid
import logging
import json
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from rapidfuzz import fuzz, process
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Logowanie
logging.basicConfig(level=logging.INFO, format='%(asctime)s │ %(levelname)-7s │ %(message)s')
log = logging.getLogger("PolyEdge")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ODDS_API_KEY: str = os.environ.get("ODDS_API_KEY", "")
ODDS_API_BASE: str = "https://api.odds-api.io/v3"

BOOKMAKERS: list[str] = ["Superbet", "Betclic PL"]

ACTIVE_LEAGUES = [
    ("football", "england-premier-league"),
    ("football", "germany-bundesliga"),
    ("football", "spain-laliga"),
    ("football", "italy-serie-a"),
    ("football", "france-ligue-1"),
    ("football", "poland-ekstraklasa"),
    ("basketball", "usa-nba"),
    ("basketball", "international-euroleague"),
]

MIN_EV_PERCENT: float = -5.0  # Na razie zostawiamy ujemne EV do testów

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EVSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
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

class PolyMarket(BaseModel):
    event_title: str
    outcome_label: str
    poly_prob: float

class BookieOdds(BaseModel):
    event_name: str
    selection: str
    decimal_odds: float
    bookmaker: str

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

signals: list[EVSignal] = []
last_refresh: str = "Nigdy"

def calc_ev_pl(prob: float, odds: float, bookmaker: str) -> tuple[float, float]:
    tax = 0.88 if bookmaker.lower() in ["superbet", "betclic pl", "sts", "fortuna"] else 1.0
    net_odds = odds * tax
    ev = (prob * net_odds) - (1 - prob)
    return round(ev * 100, 2), net_odds

def get_kelly(prob: float, odds: float, bookmaker: str) -> float:
    _, net_odds = calc_ev_pl(prob, odds, bookmaker)
    if net_odds <= 1: return 0.0
    b = net_odds - 1
    k = (b * prob - (1 - prob)) / b
    return round(max(0, k * 0.1) * 100, 2)

async def fetch_polymarket() -> list[PolyMarket]:
    results = []
    
    # 1. Automatyczne generowanie tagów na podstawie ACTIVE_LEAGUES
    slugs_to_check = set()
    for _, odds_league in ACTIVE_LEAGUES:
        parts = odds_league.split('-')
        slugs_to_check.add(parts[-1]) 
        if len(parts) > 1:
            slugs_to_check.add(f"{parts[-2]}-{parts[-1]}")
            
    slugs_to_check.update(["soccer", "basketball", "tennis", "nhl", "mma"])

    async with httpx.AsyncClient(timeout=20.0) as client:
        for slug in slugs_to_check:
            try:
                url = f"https://gamma-api.polymarket.com/markets?tag={slug}&active=true&closed=false&limit=100"
                resp = await client.get(url)
                
                if resp.status_code == 200:
                    data = resp.json()
                    for m in data:
                        q = m.get('question', '').lower()
                        
                        # 2. FILTR: Wykluczamy długoterminówki! Szukamy tylko meczów.
                        banned_words = ["cup", "championship", "qualify", "series", "draft", "award", "season", "winner", "returns", "convicted", "before"]
                        
                        if not any(bw in q for bw in banned_words):
                            if m.get('outcomes') and m.get('outcomePrices'):
                                outcomes = json.loads(m['outcomes'])
                                prices = json.loads(m['outcomePrices'])
                                
                                for i, label in enumerate(outcomes):
                                    prob = float(prices[i])
                                    if 0.02 < prob < 0.98:
                                        results.append(PolyMarket(
                                            event_title=m['question'],
                                            outcome_label=label,
                                            poly_prob=prob
                                        ))
            except Exception:
                continue
                
    log.info(f"Poly: Znaleziono {len(results)} rynków pojedynczych meczów (bez długoterminówek).")
    return results

async def fetch_odds_api() -> list[BookieOdds]:
    all_odds = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        for sport, league in ACTIVE_LEAGUES:
            try:
                url = f"{ODDS_API_BASE}/events?apiKey={ODDS_API_KEY}&sport={sport}&league={league}"
                r = await client.get(url)
                
                if r.status_code == 429:
                    log.error("LIMIT ODDS-API PRZEKROCZONY! Musisz wygenerować nowy darmowy klucz na nowy email.")
                    return []
                    
                if r.status_code == 200:
                    events = r.json()
                    for ev in events[:10]:
                        ev_id = ev['id']
                        o_url = f"{ODDS_API_BASE}/odds?apiKey={ODDS_API_KEY}&eventId={ev_id}&bookmakers={','.join(BOOKMAKERS).replace(' ', '+')}"
                        o_r = await client.get(o_url)
                        if o_r.status_code == 200:
                            data = o_r.json()
                            for bkm in data:
                                b_name = bkm['name']
                                for out in bkm['outcomes']:
                                    all_odds.append(BookieOdds(
                                        event_name=f"{ev['homeTeam']} vs {ev['awayTeam']}",
                                        selection=out['name'],
                                        decimal_odds=float(out['price']),
                                        bookmaker=b_name
                                    ))
            except Exception:
                continue
    return all_odds

async def run_pipeline():
    global signals, last_refresh
    log.info("--- Pipeline Start ---")
    
    poly = await fetch_polymarket()
    bookie = await fetch_odds_api()
    
    # DOWÓD W LOGACH: Wypisanie do 5 meczów z Polymarket
    if poly:
        log.info("--- DOWÓD: Próbka 5 pobranych meczów z Poly ---")
        unique_titles = list(set([p.event_title for p in poly]))
        for title in unique_titles[:5]:
            log.info(f"Złapałem: {title}")
            
    if not bookie:
        log.warning("Pipeline End: Brak danych bukmacherskich (prawdopodobnie limit). Zmien klucz Odds API.")
        return

    new_signals = []
    bk_strings = [f"{b.event_name} {b.selection}".lower() for b in bookie]
    
    for p in poly:
        # Bardziej agresywne czyszczenie pytań z Polymarket do dopasowania
        clean_q = p.event_title.lower()
        for word in ["will", "win", "against", "?", "the", "match", "game", "beat"]:
            clean_q = clean_q.replace(word, "")
        
        p_str = f"{clean_q.strip()} {p.outcome_label.lower()}".strip()
        
        # Matcher (szukamy dopasowania)
        match = process.extractOne(p_str, bk_strings, scorer=fuzz.token_set_ratio, score_cutoff=40)
        
        if match:
            idx = match[2]
            score = match[1]
            target = bookie[idx]
            
            # Wypisze w logach kiedy uda się połączyć mecze
            log.info(f"MATCH! Poly: '{p_str}' <-> Bookie: '{bk_strings[idx]}' | Score: {score}")
            
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

    signals = sorted(new_signals, key=lambda x: x.ev_pct, reverse=True)
    last_refresh = datetime.now().strftime("%H:%M:%S")
    log.info(f"Pipeline End: Found {len(signals)} signals")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WEB APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_pipeline, 'interval', minutes=15)
    scheduler.start()
    await run_pipeline()
    yield

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "signals": signals, 
        "last_refresh": last_refresh
    })

@app.get("/api/signals")
async def api_signals():
    return {"count": len(signals), "signals": [s.model_dump() for s in signals]}

# NAPRAWIONY PRZYCISK: ten endpoint pozwala przyciskowi odświeżać skaner na życzenie
@app.post("/api/refresh")
async def force_refresh():
    await run_pipeline()
    return {"status": "ok"}
