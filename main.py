import os
import json
import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from rapidfuzz import fuzz
from apscheduler.schedulers.asyncio import AsyncIOScheduler

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
FOOTBALL_API_KEY = os.getenv("FOOTBALL_API_KEY", "")
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
MIN_EV_THRESHOLD = float(os.getenv("MIN_EV_THRESHOLD", "0.03"))
DEFAULT_BANKROLL = float(os.getenv("DEFAULT_BANKROLL", "5000"))

signals_db = []
shadow_bets_db = []
last_update = None

TAX_MODELS = {
    "betclic": {"name": "Betclic", "stake_tax_rate": 0.0, "stake_multiplier": 1.0, "win_tax_rate": 0.10, "win_tax_threshold": 2280},
    "superbet": {"name": "Superbet", "stake_tax_rate": 0.12, "stake_multiplier": 0.88, "win_tax_rate": 0.10, "win_tax_threshold": 2280},
    "sts": {"name": "STS", "stake_tax_rate": 0.12, "stake_multiplier": 0.88, "win_tax_rate": 0.10, "win_tax_threshold": 2280},
}

def optimize_stake(fair_prob, bookie_odds, bookmaker_key, bankroll=None):
    bankroll = bankroll or DEFAULT_BANKROLL
    model = TAX_MODELS.get(bookmaker_key, TAX_MODELS["sts"])
    tax_mult = model["stake_multiplier"]
    b = bookie_odds * tax_mult - 1
    q = 1 - fair_prob
    if b <= 0:
        return {"stake": 0, "strategy": "NO_EDGE", "net_ev": 0}
    kelly_full = (b * fair_prob - q) / b
    kelly_adj = max(0, kelly_full * KELLY_FRACTION)
    raw_stake = kelly_adj * bankroll
    if raw_stake <= 0:
        return {"stake": 0, "strategy": "NO_EDGE", "net_ev": 0}
    potential_win = raw_stake * bookie_odds * tax_mult
    threshold = model["win_tax_threshold"]
    if potential_win > threshold:
        max_stake_under = threshold / (bookie_odds * tax_mult)
        ev_under = fair_prob * (threshold - max_stake_under) - q * max_stake_under
        tax_penalty = potential_win * model["win_tax_rate"]
        ev_over = fair_prob * (potential_win - tax_penalty - raw_stake) - q * raw_stake
        if ev_over > ev_under:
            return {"stake": round(raw_stake, 2), "strategy": "FULL_KELLY", "net_ev": round(ev_over, 2), "tax_hit": round(tax_penalty, 2)}
        else:
            return {"stake": round(max_stake_under, 2), "strategy": "CAP_2280", "net_ev": round(ev_under, 2), "tax_hit": 0}
    net_ev = fair_prob * (potential_win - raw_stake) - q * raw_stake
    return {"stake": round(raw_stake, 2), "strategy": "NORMAL", "net_ev": round(net_ev, 2), "tax_hit": 0}

async def fetch_polymarket_sports():
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get("https://gamma-api.polymarket.com/markets",
                params={"closed": "false", "limit": 100, "order": "volume24hr", "ascending": "false"})
            resp.raise_for_status()
            results = []
            for m in resp.json():
                title = m.get("question", "")
                tags = str(m.get("tags", "")).lower()
                is_sports = any(kw in title.lower() or kw in tags for kw in [
                    "win", "beat", "defeat", "match", "game", "fight", "championship",
                    "league", "cup", "final", "vs", "football", "soccer", "tennis",
                    "basketball", "mma", "ufc", "nba", "nfl", "premier league",
                    "la liga", "champions league", "serie a", "bundesliga",
                    "ekstraklasa", "grand slam", "open"])
                if not is_sports:
                    continue
                try:
                    prices = m.get("outcomePrices", "[]")
                    if isinstance(prices, str):
                        prices = json.loads(prices)
                    yes_price = float(prices[0]) if prices else 0
                except:
                    continue
                if yes_price <= 0.02 or yes_price >= 0.98:
                    continue
                results.append({"id": m.get("conditionId", ""), "title": title,
                    "yes_price": yes_price, "no_price": round(1 - yes_price, 4),
                    "volume": float(m.get("volume24hr", 0) or 0),
                    "end_date": m.get("endDate", ""), "source": "polymarket"})
            return results
    except Exception as e:
        print(f"[Polymarket ERROR] {e}")
        return []

SPORTS_TO_SCAN = [
    "soccer_epl", "soccer_germany_bundesliga", "soccer_spain_la_liga",
    "soccer_italy_serie_a", "soccer_france_ligue_one",
    "soccer_uefa_champs_league", "soccer_poland_ekstraklasa",
    "tennis_atp_french_open", "basketball_euroleague", "mma_mixed_martial_arts"]
EU_BOOKMAKERS = ["betclic", "superbet"]

async def fetch_bookmaker_odds():
    if not ODDS_API_KEY:
        print("[OddsAPI] Brak klucza — dodaj ODDS_API_KEY w Railway Variables")
        return []
    all_events = []
    async with httpx.AsyncClient(timeout=30) as client:
        for sport in SPORTS_TO_SCAN:
            try:
                resp = await client.get(f"https://api.the-odds-api.com/v4/sports/{sport}/odds",
                    params={"apiKey": ODDS_API_KEY, "regions": "eu", "markets": "h2h", "oddsFormat": "decimal"})
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                for game in resp.json():
                    for bm in game.get("bookmakers", []):
                        bm_key = bm["key"].lower().replace(" ", "")
                        if not any(pb in bm_key for pb in EU_BOOKMAKERS):
                            continue
                        for market in bm.get("markets", []):
                            if market["key"] != "h2h":
                                continue
                            for outcome in market["outcomes"]:
                                all_events.append({"sport": sport, "game_id": game["id"],
                                    "home": game["home_team"], "away": game["away_team"],
                                    "name": f"{game['home_team']} vs {game['away_team']}",
                                    "outcome": outcome["name"], "odds": outcome["price"],
                                    "bookmaker": bm_key, "bookmaker_title": bm["title"],
                                    "commence": game.get("commence_time", "")})
            except Exception as e:
                print(f"[OddsAPI] {sport}: {e}")
    return all_events

def normalize_name(name):
    name = name.lower().strip()
    for r in ["will", "win", "the", "to", "?", ".", ",", "!", "'"]:
        name = name.replace(r, "")
    return " ".join(name.split())

def match_events(poly_markets, bookie_events, threshold=55):
    matches = []
    for pm in poly_markets:
        pm_norm = normalize_name(pm["title"])
        best_score, best_bookie = 0, None
        for be in bookie_events:
            for c in [normalize_name(be["name"]), normalize_name(be["outcome"]),
                      normalize_name(be["home"]), normalize_name(be["away"])]:
                score = fuzz.token_sort_ratio(pm_norm, c)
                if score > best_score:
                    best_score, best_bookie = score, be
        if best_score >= threshold and best_bookie:
            matches.append({"poly": pm, "bookie": best_bookie, "match_confidence": best_score})
    return matches

def calculate_ev(poly_prob, bookie_odds, bookmaker_key):
    model = TAX_MODELS.get(bookmaker_key, TAX_MODELS["sts"])
    tax_mult = model["stake_multiplier"]
    implied_prob = 1 / bookie_odds
    edge = poly_prob - implied_prob
    ev_pct = poly_prob * (bookie_odds * tax_mult - 1) - (1 - poly_prob)
    return {"poly_prob": round(poly_prob, 4), "implied_prob": round(implied_prob, 4),
            "edge": round(edge, 4), "ev_pct": round(ev_pct, 4),
            "bookie_odds": bookie_odds, "bookmaker": bookmaker_key}

async def run_pipeline():
    global signals_db, shadow_bets_db, last_update
    print(f"\n{'='*50}\n[Pipeline] Start: {datetime.now(timezone.utc).isoformat()}")
    poly_markets = await fetch_polymarket_sports()
    bookie_events = await fetch_bookmaker_odds()
    print(f"[Pipeline] Poly: {len(poly_markets)} | Bookies: {len(bookie_events)}")
    if not poly_markets or not bookie_events:
        last_update = datetime.now(timezone.utc).isoformat()
        return
    matches = match_events(poly_markets, bookie_events)
    print(f"[Pipeline] Matched: {len(matches)}")
    new_signals = []
    for m in matches:
        poly_prob = m["poly"]["yes_price"]
        bookie_odds = m["bookie"]["odds"]
        bookmaker_key = m["bookie"]["bookmaker"]
        ev_data = calculate_ev(poly_prob, bookie_odds, bookmaker_key)
        if ev_data["ev_pct"] >= MIN_EV_THRESHOLD:
            stake_data = optimize_stake(poly_prob, bookie_odds, bookmaker_key)
            signal = {"id": len(signals_db) + len(new_signals) + 1,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": m["poly"]["title"], "outcome": m["bookie"]["outcome"],
                "sport": m["bookie"]["sport"], "bookmaker": bookmaker_key,
                "bookmaker_name": m["bookie"]["bookmaker_title"],
                "bookie_odds": bookie_odds,
                "poly_prob": round(poly_prob * 100, 1),
                "implied_prob": round(ev_data["implied_prob"] * 100, 1),
                "edge_pct": round(ev_data["edge"] * 100, 2),
                "ev_pct": round(ev_data["ev_pct"] * 100, 2),
                "match_confidence": m["match_confidence"],
                "suggested_stake": stake_data["stake"],
                "stake_strategy": stake_data["strategy"],
                "net_ev_pln": stake_data["net_ev"],
                "poly_volume": m["poly"]["volume"],
                "commence": m["bookie"]["commence"], "status": "active"}
            new_signals.append(signal)
            shadow_bets_db.append({"id": len(shadow_bets_db) + 1,
                "signal_id": signal["id"], "event": signal["event"],
                "outcome": signal["outcome"], "bookmaker": signal["bookmaker_name"],
                "odds_entry": signal["bookie_odds"], "poly_prob": signal["poly_prob"],
                "ev_pct": signal["ev_pct"], "stake": signal["suggested_stake"],
                "strategy": signal["stake_strategy"],
                "predicted_net": signal["net_ev_pln"],
                "result": None, "actual_pnl": None,
                "created_at": signal["timestamp"], "is_real": False})
    signals_db = new_signals + signals_db[:200]
    last_update = datetime.now(timezone.utc).isoformat()
    print(f"[Pipeline] EV+ signals: {len(new_signals)} | Shadow total: {len(shadow_bets_db)}")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app):
    scheduler.add_job(run_pipeline, "interval", minutes=5, id="pipeline", replace_existing=True)
    scheduler.start()
    asyncio.create_task(run_pipeline())
    yield
    scheduler.shutdown()

app = FastAPI(title="PolyEdge", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": last_update, "signals": len(signals_db), "shadow_bets": len(shadow_bets_db)}

@app.get("/api/signals")
async def get_signals():
    return signals_db[:50]

@app.get("/api/shadow-bets")
async def get_shadow_bets():
    return shadow_bets_db[-100:]

@app.post("/api/shadow-bets/{bet_id}/promote")
async def promote_to_real(bet_id: int):
    for bet in shadow_bets_db:
        if bet["id"] == bet_id:
            bet["is_real"] = True
            return {"status": "promoted", "bet": bet}
    return {"status": "not_found"}

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """<!DOCTYPE html>
<html lang="pl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>PolyEdge</title><style>
*{margin:0;padding:0;box-sizing:border-box}body{background:#0f172a;color:#e2e8f0;font-family:system-ui,sans-serif}
.w{max-width:1200px;margin:0 auto;padding:20px}.c{background:#1e293b;border:1px solid #334155;border-radius:12px;margin-bottom:20px}
.ch{padding:14px 20px;border-bottom:1px solid #334155;display:flex;justify-content:space-between;align-items:center}
.ch h2{font-size:15px;font-weight:700}.b{font-size:11px;padding:2px 8px;border-radius:9999px;font-weight:600}
.bg{background:#065f46;color:#6ee7b7}.ba{background:#78350f;color:#fcd34d}.bb{background:#1e3a5f;color:#93c5fd}
.st{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
.s{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:18px}
.sl{color:#64748b;font-size:12px;margin-bottom:6px}.sv{font-size:26px;font-weight:800}
table{width:100%;border-collapse:collapse}th{text-align:left;font-size:11px;color:#94a3b8;padding:8px 12px;border-bottom:1px solid #334155;text-transform:uppercase}
td{padding:9px 12px;font-size:13px;border-bottom:1px solid #1e293b40}tr:hover{background:#1e293b80}
.m{font-family:monospace}.g{color:#34d399}.a{color:#fbbf24}.r{color:#f87171}.i{color:#818cf8}.p{color:#f472b6}
.btn{padding:4px 12px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:none;background:#065f46;color:#6ee7b7}
.btn:hover{background:#047857}.hd{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px}
.rb{background:#312e81;color:#a5b4fc;padding:6px 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;border:none}
.rb:hover{background:#3730a3}.pu{animation:pu 2s infinite}@keyframes pu{0%,100%{opacity:1}50%{opacity:.5}}
.tr{max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tx{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:12px}
.tx>div{border-radius:8px;padding:12px;font-size:12px}
@media(max-width:768px){.st{grid-template-columns:repeat(2,1fr)}.tx{grid-template-columns:1fr}}
</style></head><body><div class="w">
<div class="hd"><div><h1 style="font-size:22px;font-weight:800"><span class="i">&#9670;</span> PolyEdge</h1>
<p style="color:#64748b;font-size:13px;margin-top:2px">Polymarket &rarr; PL Bookmakers | EV Scanner</p></div>
<div style="display:flex;align-items:center;gap:10px">
<span id="st" class="b bg pu">&bull; LIVE</span><span id="up" style="color:#64748b;font-size:12px"></span>
<button class="rb" onclick="L()">Odswiez</button></div></div>
<div class="st">
<div class="s"><div class="sl">Sygnaly EV+</div><div class="sv i" id="n1">&mdash;</div></div>
<div class="s"><div class="sl">Shadow Bets</div><div class="sv a" id="n2">&mdash;</div></div>
<div class="s"><div class="sl">Real Bets</div><div class="sv g" id="n3">&mdash;</div></div>
<div class="s"><div class="sl">Avg EV%</div><div class="sv p" id="n4">&mdash;</div></div></div>
<div class="c"><div class="ch"><h2>Live EV+ Signals</h2><span class="b bb">EV &gt; 3%</span></div>
<div style="overflow-x:auto"><table><thead><tr><th>Event</th><th>Outcome</th><th>Buk</th><th>Kurs</th><th>Poly%</th><th>Edge</th><th>EV%</th><th>Stawka</th><th>Strategia</th><th>Net EV</th></tr></thead>
<tbody id="t1"><tr><td colspan="10" style="text-align:center;color:#64748b;padding:40px">Ladowanie... pipeline co 5 min</td></tr></tbody></table></div></div>
<div class="c"><div class="ch"><h2>Shadow Bets</h2><span class="b ba" id="sc">0</span></div>
<div style="overflow-x:auto"><table><thead><tr><th>Data</th><th>Event</th><th>Buk</th><th>Kurs</th><th>EV%</th><th>Stawka</th><th>Status</th><th>Akcja</th></tr></thead>
<tbody id="t2"><tr><td colspan="8" style="text-align:center;color:#64748b;padding:40px">Brak shadow bets</td></tr></tbody></table></div></div>
<div class="c" style="padding:18px"><h3 style="font-size:13px;font-weight:700;color:#94a3b8;margin-bottom:10px">Podatki PL 2026</h3>
<div class="tx"><div style="background:#065f46"><b style="color:#6ee7b7">Betclic &mdash; 0% tax stawki</b><div style="color:#a7f3d0;margin-top:4px">Buk pokrywa 12%. Podatek 10% tylko &gt; 2280 PLN.</div></div>
<div style="background:#78350f"><b style="color:#fcd34d">STS/Superbet &mdash; 12% tax</b><div style="color:#fde68a;margin-top:4px">Efektywna stawka 88%. Podatek 10% &gt; 2280 PLN.</div></div></div></div></div>
<script>
async function L(){try{const[sig,sh,h]=await Promise.all([fetch('/api/signals').then(r=>r.json()),fetch('/api/shadow-bets').then(r=>r.json()),fetch('/health').then(r=>r.json())]);
document.getElementById('n1').textContent=sig.length;document.getElementById('n2').textContent=sh.filter(b=>!b.is_real).length;
document.getElementById('n3').textContent=sh.filter(b=>b.is_real).length;
document.getElementById('n4').textContent=sig.length?(sig.reduce((a,x)=>a+x.ev_pct,0)/sig.length).toFixed(1)+'%':'—';
if(h.last_update)document.getElementById('up').textContent='Scan: '+new Date(h.last_update).toLocaleTimeString('pl-PL');
const t1=document.getElementById('t1');
t1.innerHTML=sig.length?sig.map(s=>`<tr><td class="tr" title="${s.event}">${s.event}</td><td>${s.outcome||'—'}</td>
<td><span class="b ${s.bookmaker==='betclic'?'bg':'ba'}">${s.bookmaker_name}</span></td>
<td class="m" style="font-weight:600">${s.bookie_odds}</td><td class="m">${s.poly_prob}%</td>
<td class="m ${s.edge_pct>0?'g':'r'}">${s.edge_pct>0?'+':''}${s.edge_pct}%</td>
<td class="m g" style="font-weight:700">+${s.ev_pct}%</td><td class="m">${s.suggested_stake} PLN</td>
<td><span class="b ${s.stake_strategy==='CAP_2280'?'ba':'bb'}">${s.stake_strategy}</span></td>
<td class="m i">${s.net_ev_pln} PLN</td></tr>`).join(''):'<tr><td colspan="10" style="text-align:center;color:#64748b;padding:40px">Brak — skanuje co 5 min</td></tr>';
const t2=document.getElementById('t2');document.getElementById('sc').textContent=sh.length+' betow';
t2.innerHTML=sh.length?sh.slice(-20).reverse().map(b=>`<tr><td style="font-size:11px;color:#94a3b8">${new Date(b.created_at).toLocaleString('pl-PL')}</td>
<td class="tr" title="${b.event}">${b.event}</td><td>${b.bookmaker}</td><td class="m">${b.odds_entry}</td>
<td class="m g">+${b.ev_pct}%</td><td class="m">${b.stake} PLN</td>
<td><span class="b ${b.is_real?'bg':b.result?'bb':'ba'}">${b.is_real?'REAL':b.result||'SHADOW'}</span></td>
<td>${b.is_real?'<span class="g">&#10003;</span>':'<button class="btn" onclick="P('+b.id+')">→ REAL</button>'}</td></tr>`).join('')
:'<tr><td colspan="8" style="text-align:center;color:#64748b;padding:40px">Brak</td></tr>';}catch(e){console.error(e)}}
async function P(id){if(!confirm('Oznaczyc jako real?'))return;await fetch('/api/shadow-bets/'+id+'/promote',{method:'POST'});L()}
L();setInterval(L,30000);
</script></body></html>
