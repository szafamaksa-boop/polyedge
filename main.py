"""PolyEdge v1.1"""
import os, json, asyncio, pathlib
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from rapidfuzz import fuzz
from apscheduler.schedulers.asyncio import AsyncIOScheduler

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
MIN_EV = float(os.getenv("MIN_EV_THRESHOLD", "0.03"))
BANKROLL = float(os.getenv("DEFAULT_BANKROLL", "5000"))
signals_db = []
shadow_db = []
last_update = None
TAX = {"betclic":{"name":"Betclic","mult":1.0,"wt":0.10,"th":2280},"superbet":{"name":"Superbet","mult":0.88,"wt":0.10,"th":2280},"sts":{"name":"STS","mult":0.88,"wt":0.10,"th":2280}}

def opt_stake(prob,odds,bk):
    t=TAX.get(bk,TAX["sts"]);b=odds*t["mult"]-1;q=1-prob
    if b<=0:return{"stake":0,"strategy":"NO_EDGE","net_ev":0}
    kf=max(0,((b*prob-q)/b)*KELLY_FRACTION);raw=kf*BANKROLL
    if raw<=0:return{"stake":0,"strategy":"NO_EDGE","net_ev":0}
    pw=raw*odds*t["mult"]
    if pw>t["th"]:
        ms=t["th"]/(odds*t["mult"]);eu=prob*(t["th"]-ms)-q*ms
        tp=pw*t["wt"];eo=prob*(pw-tp-raw)-q*raw
        if eo>eu:return{"stake":round(raw,2),"strategy":"FULL_KELLY","net_ev":round(eo,2)}
        return{"stake":round(ms,2),"strategy":"CAP_2280","net_ev":round(eu,2)}
    return{"stake":round(raw,2),"strategy":"NORMAL","net_ev":round(prob*(pw-raw)-q*raw,2)}

async def fetch_poly():
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.get("https://gamma-api.polymarket.com/markets",params={"closed":"false","limit":100,"order":"volume24hr","ascending":"false"})
            r.raise_for_status();out=[]
            for m in r.json():
                title=m.get("question","");tags=str(m.get("tags","")).lower()
                kws=["win","beat","match","game","fight","league","cup","final","vs","football","soccer","tennis","basketball","mma","ufc","nba","nfl","premier league","la liga","champions league","serie a","bundesliga","ekstraklasa","grand slam","open","championship","defeat"]
                if not any(k in title.lower() or k in tags for k in kws):continue
                try:
                    p=m.get("outcomePrices","[]")
                    if isinstance(p,str):p=json.loads(p)
                    yp=float(p[0]) if p else 0
                except:continue
                if yp<=0.02 or yp>=0.98:continue
                out.append({"id":m.get("conditionId",""),"title":title,"yes_price":yp,"volume":float(m.get("volume24hr",0) or 0)})
            return out
    except Exception as e:
        print(f"[Poly] {e}");return[]

SPORTS=["soccer_epl","soccer_germany_bundesliga","soccer_spain_la_liga","soccer_italy_serie_a","soccer_france_ligue_one","soccer_uefa_champs_league","soccer_poland_ekstraklasa","tennis_atp_french_open","basketball_euroleague","mma_mixed_martial_arts"]

async def fetch_odds():
    if not ODDS_API_KEY:return[]
    out=[]
    async with httpx.AsyncClient(timeout=30) as c:
        for sp in SPORTS:
            try:
                r=await c.get(f"https://api.the-odds-api.com/v4/sports/{sp}/odds",params={"apiKey":ODDS_API_KEY,"regions":"eu","markets":"h2h","oddsFormat":"decimal"})
                if r.status_code==404:continue
                r.raise_for_status()
                for g in r.json():
                    for bm in g.get("bookmakers",[]):
                        bk=bm["key"].lower().replace(" ","")
                        if not any(x in bk for x in["betclic","superbet"]):continue
                        for mk in bm.get("markets",[]):
                            if mk["key"]!="h2h":continue
                            for oc in mk["outcomes"]:
                                out.append({"sport":sp,"home":g["home_team"],"away":g["away_team"],"name":f"{g['home_team']} vs {g['away_team']}","outcome":oc["name"],"odds":oc["price"],"bookmaker":bk,"bookmaker_title":bm["title"],"commence":g.get("commence_time","")})
            except Exception as e:print(f"[Odds] {sp}: {e}")
    return out

def norm(n):
    n=n.lower().strip()
    for r in["will","win","the","to","?",".",",","!","'"]:n=n.replace(r,"")
    return" ".join(n.split())

def match_ev(polys,bookies):
    out=[]
    for pm in polys:
        pn=norm(pm["title"]);bs=0;bb=None
        for be in bookies:
            for c in[norm(be["name"]),norm(be["outcome"]),norm(be["home"]),norm(be["away"])]:
                s=fuzz.token_sort_ratio(pn,c)
                if s>bs:bs=s;bb=be
        if bs>=55 and bb:out.append({"poly":pm,"bookie":bb,"conf":bs})
    return out

async def pipeline():
    global signals_db,shadow_db,last_update
    polys=await fetch_poly();bookies=await fetch_odds()
    print(f"[Pipe] Poly:{len(polys)} Bookies:{len(bookies)}")
    if not polys or not bookies:last_update=datetime.now(timezone.utc).isoformat();return
    matches=match_ev(polys,bookies);ns=[]
    for m in matches:
        pp=m["poly"]["yes_price"];od=m["bookie"]["odds"];bk=m["bookie"]["bookmaker"]
        ip=1/od;edge=pp-ip;ev=pp*(od*TAX.get(bk,TAX["sts"])["mult"]-1)-(1-pp)
        if ev>=MIN_EV:
            st=opt_stake(pp,od,bk)
            sig={"id":len(signals_db)+len(ns)+1,"timestamp":datetime.now(timezone.utc).isoformat(),"event":m["poly"]["title"],"outcome":m["bookie"]["outcome"],"sport":m["bookie"]["sport"],"bookmaker":bk,"bookmaker_name":m["bookie"]["bookmaker_title"],"bookie_odds":od,"poly_prob":round(pp*100,1),"implied_prob":round(ip*100,1),"edge_pct":round(edge*100,2),"ev_pct":round(ev*100,2),"match_confidence":m["conf"],"suggested_stake":st["stake"],"stake_strategy":st["strategy"],"net_ev_pln":st["net_ev"],"poly_volume":m["poly"]["volume"],"commence":m["bookie"]["commence"],"status":"active"}
            ns.append(sig)
            shadow_db.append({"id":len(shadow_db)+1,"event":sig["event"],"outcome":sig["outcome"],"bookmaker":sig["bookmaker_name"],"odds_entry":od,"poly_prob":sig["poly_prob"],"ev_pct":sig["ev_pct"],"stake":st["stake"],"strategy":st["strategy"],"predicted_net":st["net_ev"],"result":None,"created_at":sig["timestamp"],"is_real":False})
    signals_db=ns+signals_db[:200];last_update=datetime.now(timezone.utc).isoformat()
    print(f"[Pipe] EV+:{len(ns)} Shadow:{len(shadow_db)}")

sched=AsyncIOScheduler()
@asynccontextmanager
async def lifespan(a):
    sched.add_job(pipeline,"interval",minutes=5,id="p",replace_existing=True);sched.start();asyncio.create_task(pipeline())
    yield
    sched.shutdown()

app=FastAPI(title="PolyEdge",lifespan=lifespan)
HP=pathlib.Path(__file__).parent/"templates"/"index.html"
@app.get("/test123")
async def test123():
    return {"test": "works", "signals_count": len(signals_db), "shadow_count": len(shadow_db)}
@app.get("/health")
async def health():
    return{"status":"ok","last_update":last_update,"signals":len(signals_db),"shadow_bets":len(shadow_db)}

@app.get("/api/signals")
async def api_signals():
    return signals_db[:50]

@app.get("/api/shadow-bets")
async def api_shadow():
    return shadow_db[-100:]

@app.post("/api/shadow-bets/{bid}/promote")
async def promote(bid:int):
    for b in shadow_db:
        if b["id"]==bid:b["is_real"]=True;return{"status":"promoted"}
    return{"status":"not_found"}

@app.get("/",response_class=HTMLResponse)
async def dash():
    return HP.read_text(encoding="utf-8")
