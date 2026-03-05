"""
PolyEdge v2 — EV+ Signal Engine
Polymarket Gamma API  ↔  odds-api.io (LV BET, Superbet)
Polish Tax Engine · Kelly Criterion · Shadow Bets
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

# Bukmacherzy do śledzenia (nazwy jak w odds-api.io)
BOOKMAKERS: list[str] = ["LV BET", "Superbet"]

# Sporty do skanowania — klucze odds-api.io
SPORTS: list[str] = [
    "soccer_epl",
    "soccer_germany_bundesliga",
    "soccer_spain_la_liga",
    "soccer_italy_serie_a",
    "soccer_france_ligue_one",
    "soccer_uefa_champs_league",
    "basketball_nba",
    "basketball_euroleague",
    "mma_mixed_martial_arts",
    "tennis_atp_french_open",
    "americanfootball_nfl",
    "icehockey_nhl",
]

# Polymarket Gamma API
POLYMARKET_GAMMA_URL: str = "https://gamma-api.polymarket.com/markets"

# ── Polski Tax Engine 2026 ──────────────────────────────────────────────────
# Podatek od stawki: 12% (operator potrąca, gracz stawia netto ×0.88)
# Dotyczy: LV BET, Superbet (i wszyscy PL-licencjonowani)
#
# Podatek od wygranej: 10% PIT od nadwyżki ponad 2 280 PLN jednorazowej wygranej
# Wygrana ≤ 2 280 PLN → 0% PIT
# Wygrana > 2 280 PLN → 10% PIT od CAŁEJ wygranej (nie tylko nadwyżki)
# ────────────────────────────────────────────────────────────────────────────

STAKE_TAX_RATE: float = 0.12           # 12% podatek od stawki
STAKE_TAX_MULT: float = 1.0 - STAKE_TAX_RATE  # 0.88 — mnożnik efektywnej stawki
PIT_THRESHOLD_PLN: float = 2280.0      # próg PIT od wygranej
PIT_RATE: float = 0.10                 # 10% PIT od wygranej > progu

BANKROLL_PLN: float = 2280.0           # domyślny bankroll
KELLY_FRACTION: float = 0.25           # Quarter-Kelly
MIN_EV_PERCENT: float = 3.0            # min EV% żeby auto-shadow
REFRESH_MINUTES: int = 10              # pipeline interval

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("polyedge")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PolyOutcome(BaseModel):
    event_title: str
    outcome_label: str
    poly_prob: float  # 0‒1

class BookieOutcome(BaseModel):
    sport: str
    event_id: str
    event_name: str
    selection: str
    decimal_odds: float
    bookmaker: str

class EVSignal(BaseModel):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:8])
    ts: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    poly_event: str
    poly_outcome: str
    poly_prob: float
    bookmaker: str
    bookie_event: str
    bookie_selection: str
    bookie_odds: float
    implied_prob_bookie: float
    ev_pct: float
    kelly_stake: float
    kelly_raw_frac: float
    net_payout_example: float   # zysk netto na 100 PLN stawki (po podatkach)
    status: str = "shadow"      # shadow | real

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# IN-MEMORY STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

signals: list[EVSignal] = []
shadow_bets: list[EVSignal] = []
last_refresh: Optional[str] = None
pipeline_errors: list[str] = []

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1) POLYMARKET — Gamma API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

POLY_SPORT_TAGS = [
    "sports", "nba", "nfl", "mma", "ufc", "football", "soccer",
    "tennis", "boxing", "mlb", "nhl", "f1", "champions-league",
    "epl", "bundesliga", "la-liga", "serie-a", "ligue-1",
    "euroleague", "basketball",
]

async def fetch_polymarket() -> list[PolyOutcome]:
    """Fetch sport markets from Polymarket Gamma /markets endpoint."""
    results: list[PolyOutcome] = []
    seen: set[str] = set()

    async with httpx.AsyncClient(timeout=25.0) as c:
        for tag in POLY_SPORT_TAGS:
            try:
                r = await c.get(POLYMARKET_GAMMA_URL, params={
                    "tag": tag, "closed": "false", "limit": 80,
                })
                if r.status_code != 200:
                    continue
                data = r.json()
                items = data if isinstance(data, list) else data.get("data", [])
                for m in items:
                    question = m.get("question", m.get("title", ""))
                    slug = m.get("slug", question)

                    # Parse outcomes + prices
                    outcomes_raw = m.get("outcomes", "[]")
                    prices_raw = m.get("outcomePrices", "[]")
                    if isinstance(outcomes_raw, str):
                        try: outcomes_raw = json.loads(outcomes_raw)
                        except Exception: continue
                    if isinstance(prices_raw, str):
                        try: prices_raw = json.loads(prices_raw)
                        except Exception: prices_raw = []

                    for i, label in enumerate(outcomes_raw):
                        prob = 0.5
                        if i < len(prices_raw):
                            try: prob = float(prices_raw[i])
                            except (ValueError, TypeError): pass
                        if not (0.05 < prob < 0.95):
                            continue
                        key = f"{slug}||{label}"
                        if key in seen:
                            continue
                        seen.add(key)
                        results.append(PolyOutcome(
                            event_title=question,
                            outcome_label=str(label),
                            poly_prob=prob,
                        ))
            except Exception as e:
                log.warning(f"Polymarket [{tag}]: {e}")

    log.info(f"Polymarket: {len(results)} outcomes z {len(POLY_SPORT_TAGS)} tagów")
    return results

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2) ODDS-API.IO — LV BET + Superbet
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_odds_api() -> list[BookieOutcome]:
    """
    Fetch odds from odds-api.io for LV BET and Superbet.
    Step 1: GET /v3/events   → list of events per sport
    Step 2: GET /v3/odds     → odds per event + bookmaker
    """
    if not ODDS_API_KEY:
        log.error("ODDS_API_KEY not set — skipping odds-api.io")
        return []

    all_outcomes: list[BookieOutcome] = []
    bookmakers_param = ",".join(BOOKMAKERS)  # "LV BET,Superbet"

    async with httpx.AsyncClient(timeout=25.0) as c:
        for sport in SPORTS:
            try:
                # ── Step 1: Get events ──────────────────────────────────
                events_url = f"{ODDS_API_BASE}/events"
                ev_resp = await c.get(events_url, params={
                    "apiKey": ODDS_API_KEY,
                    "sport": sport,
                    "bookmaker": BOOKMAKERS[0],  # potrzebny do filtra
                })
                if ev_resp.status_code != 200:
                    log.warning(f"odds-api events [{sport}] HTTP {ev_resp.status_code}")
                    continue

                events = ev_resp.json()
                if not isinstance(events, list):
                    events = events.get("data", events.get("events", []))

                # ── Step 2: Get odds for each event ─────────────────────
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    event_id = str(event.get("id", event.get("event_id", "")))
                    event_name = event.get("name", event.get("title", ""))
                    home = event.get("home_team", event.get("home", ""))
                    away = event.get("away_team", event.get("away", ""))
                    if not event_name and home and away:
                        event_name = f"{home} vs {away}"
                    if not event_id:
                        continue

                    try:
                        odds_url = f"{ODDS_API_BASE}/odds"
                        odds_resp = await c.get(odds_url, params={
                            "apiKey": ODDS_API_KEY,
                            "eventId": event_id,
                            "bookmakers": bookmakers_param,
                        })
                        if odds_resp.status_code != 200:
                            continue
                        odds_data = odds_resp.json()
                    except Exception:
                        continue

                    # Parse bookmakers → markets → outcomes
                    bookmaker_list = []
                    if isinstance(odds_data, dict):
                        bookmaker_list = odds_data.get("bookmakers", [])
                        if not bookmaker_list:
                            bookmaker_list = odds_data.get("data", {}).get("bookmakers", [])
                    elif isinstance(odds_data, list):
                        # some APIs wrap differently
                        for item in odds_data:
                            if isinstance(item, dict) and "bookmakers" in item:
                                bookmaker_list.extend(item["bookmakers"])

                    for bk in bookmaker_list:
                        if not isinstance(bk, dict):
                            continue
                        bk_name = bk.get("title", bk.get("name", bk.get("key", "")))
                        # Normalize bookmaker name check
                        bk_upper = bk_name.upper().replace("-", " ").replace("_", " ")
                        matched_bk = None
                        for target in BOOKMAKERS:
                            if target.upper() in bk_upper or bk_upper in target.upper():
                                matched_bk = target
                                break
                        if not matched_bk:
                            continue

                        markets = bk.get("markets", [])
                        for market in markets:
                            if not isinstance(market, dict):
                                continue
                            outcomes = market.get("outcomes", market.get("selections", []))
                            for oc in outcomes:
                                if not isinstance(oc, dict):
                                    continue
                                sel_name = oc.get("name", oc.get("label", ""))
                                price = oc.get("price", oc.get("odds", oc.get("decimal_odds", 0)))
                                try:
                                    odds_f = float(price)
                                except (ValueError, TypeError):
                                    continue
                                if odds_f < 1.01:
                                    continue
                                all_outcomes.append(BookieOutcome(
                                    sport=sport,
                                    event_id=event_id,
                                    event_name=str(event_name),
                                    selection=str(sel_name),
                                    decimal_odds=odds_f,
                                    bookmaker=matched_bk,
                                ))

            except httpx.HTTPError as e:
                log.warning(f"odds-api HTTP [{sport}]: {e}")
            except Exception as e:
                log.warning(f"odds-api parse [{sport}]: {e}")

    log.info(f"odds-api.io: {len(all_outcomes)} selections ({', '.join(BOOKMAKERS)})")
    return all_outcomes

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3) FUZZY MATCHING — Polymarket ↔ Bookmaker events
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _norm(text: str) -> str:
    """Lowercase, strip accents/punctuation, collapse whitespace."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def fuzzy_match(
    poly: list[PolyOutcome],
    bookie: list[BookieOutcome],
    threshold: int = 55,
) -> list[tuple[PolyOutcome, BookieOutcome, int]]:
    """Match Polymarket outcomes to bookmaker selections via fuzzy string matching."""
    if not bookie:
        return []

    bk_keys = [f"{_norm(b.event_name)} {_norm(b.selection)}" for b in bookie]
    matches: list[tuple[PolyOutcome, BookieOutcome, int]] = []

    for po in poly:
        pk = f"{_norm(po.event_title)} {_norm(po.outcome_label)}"
        result = process.extractOne(pk, bk_keys, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
        if result:
            _, score, idx = result
            matches.append((po, bookie[idx], int(score)))

    log.info(f"Fuzzy match: {len(matches)} par (próg={threshold})")
    return matches

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4) POLISH TAX ENGINE + EV + KELLY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def net_payout(gross_win: float) -> float:
    """
    Oblicz wypłatę netto po polskich podatkach 2026.
    gross_win = (decimal_odds × stawka_netto) − stawka_netto
    Jeśli wygrana > 2 280 PLN → 10% PIT od CAŁEJ wygranej.
    """
    if gross_win <= 0:
        return 0.0
    if gross_win > PIT_THRESHOLD_PLN:
        return gross_win * (1.0 - PIT_RATE)
    return gross_win


def calc_ev(poly_prob: float, decimal_odds: float, stake: float = 100.0) -> tuple[float, float]:
    """
    Oblicz EV po polskich podatkach.
    - Stawka efektywna = stake × 0.88 (12% tax od stawki)
    - Wygrana brutto = effective_stake × decimal_odds
    - Zysk brutto = wygrana_brutto − stake (pełna kwota z kieszeni)
    - Zysk netto = z uwzględnieniem PIT jeśli > 2 280

    Returns: (ev_percent, net_profit_on_stake)
    """
    effective_stake = stake * STAKE_TAX_MULT        # np. 100 × 0.88 = 88 PLN na zakład
    gross_return = effective_stake * decimal_odds    # np. 88 × 2.50 = 220 PLN
    gross_profit = gross_return - stake              # 220 − 100 = 120 PLN (z kieszeni wychodzi 100)

    # Podatek PIT od wygranej (wypłata = gross_return)
    if gross_return > PIT_THRESHOLD_PLN:
        pit = gross_return * PIT_RATE
        net_profit = gross_profit - pit
    else:
        net_profit = gross_profit

    # EV = p(win) × net_profit − p(lose) × stake
    ev = (poly_prob * net_profit) - ((1.0 - poly_prob) * stake)
    ev_pct = (ev / stake) * 100.0

    return ev_pct, net_profit


def kelly_stake(
    poly_prob: float,
    decimal_odds: float,
    bankroll: float = BANKROLL_PLN,
    fraction: float = KELLY_FRACTION,
) -> tuple[float, float]:
    """
    Quarter-Kelly z uwzględnieniem polskich podatków.
    Optymalizacja progu 2 280 PLN — jeśli kelly chce postawić dużo,
    sprawdza czy wygrana nie przekroczy progu PIT i koryguje.

    Returns: (stake_pln, raw_kelly_fraction)
    """
    # Efektywne odds po podatku od stawki
    eff_odds = decimal_odds * STAKE_TAX_MULT  # np. 2.50 × 0.88 = 2.20
    b = eff_odds - 1.0  # net odds
    if b <= 0:
        return 0.0, 0.0

    q = 1.0 - poly_prob
    f_star = (poly_prob * b - q) / b
    if f_star <= 0:
        return 0.0, 0.0

    raw_frac = f_star

    # Quarter-Kelly
    f_adj = f_star * fraction
    stake = bankroll * f_adj

    # ── Optymalizacja progu 2 280 PLN ──
    # Sprawdź czy wygrana > próg → PIT zjada zysk
    # Jeśli tak, ogranicz stawkę żeby wygrana ≈ 2 280
    potential_return = (stake * STAKE_TAX_MULT) * decimal_odds
    if potential_return > PIT_THRESHOLD_PLN:
        # Max stawka żeby return ≤ 2280
        max_stake_under = PIT_THRESHOLD_PLN / (STAKE_TAX_MULT * decimal_odds)
        # Ale sprawdź czy EV jest na tyle dobre, że warto przekroczyć
        ev_under, _ = calc_ev(poly_prob, decimal_odds, max_stake_under)
        ev_over, _ = calc_ev(poly_prob, decimal_odds, stake)
        # Jeśli EV jest lepsze pod progiem, ogranicz
        if ev_under > ev_over * 0.9:
            stake = max_stake_under

    # Granice: min 5 PLN, max 20% bankrollu
    stake = max(5.0, min(stake, bankroll * 0.20))
    return round(stake, 2), round(raw_frac, 4)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5) PIPELINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def run_pipeline():
    """Main pipeline: fetch → match → score → signal."""
    global signals, last_refresh, pipeline_errors
    pipeline_errors = []
    log.info("══════ Pipeline START ══════")

    # 1. Fetch sources
    poly_outcomes = await fetch_polymarket()
    if not poly_outcomes:
        pipeline_errors.append("Polymarket: brak danych — API może być niedostępne")

    bookie_outcomes = await fetch_odds_api()
    if not bookie_outcomes:
        msg = "odds-api.io: brak danych"
        if not ODDS_API_KEY:
            msg += " — ODDS_API_KEY nie ustawiony w env vars!"
        pipeline_errors.append(msg)

    # 2. Fuzzy match
    matched = fuzzy_match(poly_outcomes, bookie_outcomes)

    # 3. Score & filter
    new_signals: list[EVSignal] = []
    for po, bo, score in matched:
        ev_pct, net_prof = calc_ev(po.poly_prob, bo.decimal_odds)
        if ev_pct < MIN_EV_PERCENT:
            continue

        impl_prob = 1.0 / bo.decimal_odds if bo.decimal_odds > 0 else 0
        k_stake, k_frac = kelly_stake(po.poly_prob, bo.decimal_odds)

        sig = EVSignal(
            poly_event=po.event_title,
            poly_outcome=po.outcome_label,
            poly_prob=round(po.poly_prob, 4),
            bookmaker=bo.bookmaker,
            bookie_event=bo.event_name,
            bookie_selection=bo.selection,
            bookie_odds=bo.decimal_odds,
            implied_prob_bookie=round(impl_prob, 4),
            ev_pct=round(ev_pct, 2),
            kelly_stake=k_stake,
            kelly_raw_frac=k_frac,
            net_payout_example=round(net_prof, 2),
        )
        new_signals.append(sig)

    signals = sorted(new_signals, key=lambda s: s.ev_pct, reverse=True)

    # 4. Auto-shadow (EV > 3%)
    existing_ids = {sb.poly_event + sb.poly_outcome + sb.bookmaker for sb in shadow_bets}
    for sig in signals:
        key = sig.poly_event + sig.poly_outcome + sig.bookmaker
        if key not in existing_ids:
            shadow = sig.model_copy()
            shadow.status = "shadow"
            shadow_bets.append(shadow)
            existing_ids.add(key)
            log.info(f"Auto-shadow: {sig.id} │ EV={sig.ev_pct}% │ {sig.poly_outcome}")

    last_refresh = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log.info(f"Pipeline OK: {len(signals)} signals │ {len(shadow_bets)} shadow bets")
    log.info("══════ Pipeline END ══════")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6) SCHEDULER + APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("PolyEdge v2 starting…")
    await run_pipeline()
    scheduler.add_job(run_pipeline, "interval", minutes=REFRESH_MINUTES, id="pipeline")
    scheduler.start()
    log.info(f"Scheduler: co {REFRESH_MINUTES} min")
    yield
    scheduler.shutdown()
    log.info("PolyEdge shutdown")

app = FastAPI(title="PolyEdge", version="2.0.0", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# ── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    total_kelly = sum(s.kelly_stake for s in signals)
    stats = {
        "total_signals": len(signals),
        "shadow_count": len(shadow_bets),
        "real_count": sum(1 for s in shadow_bets if s.status == "real"),
        "avg_ev": round(sum(s.ev_pct for s in signals) / max(len(signals), 1), 2),
        "best_ev": round(max((s.ev_pct for s in signals), default=0), 2),
        "total_kelly": round(total_kelly, 2),
        "last_refresh": last_refresh or "—",
        "errors": pipeline_errors,
        "api_key_set": bool(ODDS_API_KEY),
    }
    return templates.TemplateResponse("index.html", {
        "request": request,
        "signals": signals,
        "shadow_bets": shadow_bets,
        "stats": stats,
    })


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "2.0.0",
        "signals": len(signals),
        "shadow_bets": len(shadow_bets),
        "last_refresh": last_refresh,
        "api_key_set": bool(ODDS_API_KEY),
    }


@app.get("/api/signals")
async def api_signals():
    return {
        "count": len(signals),
        "last_refresh": last_refresh,
        "signals": [s.model_dump() for s in signals],
    }


@app.get("/api/shadow-bets")
async def api_shadow_bets():
    return {
        "count": len(shadow_bets),
        "shadow": sum(1 for s in shadow_bets if s.status == "shadow"),
        "real": sum(1 for s in shadow_bets if s.status == "real"),
        "bets": [s.model_dump() for s in shadow_bets],
    }


@app.post("/api/shadow-bets/{bet_id}/promote")
async def promote_shadow_bet(bet_id: str):
    """Promote a shadow bet to REAL status."""
    for sb in shadow_bets:
        if sb.id == bet_id and sb.status == "shadow":
            sb.status = "real"
            log.info(f"PROMOTED to REAL: {sb.id} │ {sb.poly_event}")
            return {"ok": True, "message": f"Bet {bet_id} → REAL"}
    return {"ok": False, "message": "Shadow bet not found or already real"}


@app.post("/api/refresh")
async def manual_refresh():
    await run_pipeline()
    return {"ok": True, "signals": len(signals), "last_refresh": last_refresh}
