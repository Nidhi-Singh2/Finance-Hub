#!/usr/bin/env python3
"""
Finance Hub — daily refresh.

Pulls RSS feeds from major finance publications, market KPIs from public
endpoints, and writes everything to data.json which the front-end consumes.

No API keys required.
"""

import json
import re
import time
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
import requests


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

# RSS sources. Public, free, no auth required.
# Keep this list short enough to finish in <2 min on a CI runner.
FEEDS = [
    # General finance / markets
    ("Reuters Business",   "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Markets",    "https://feeds.reuters.com/news/wealth"),
    ("Yahoo Finance",      "https://finance.yahoo.com/news/rssindex"),
    ("MarketWatch Top",    "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("MarketWatch Real-Time", "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"),
    ("CNBC Top",           "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114"),
    ("CNBC Finance",       "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664"),
    ("CNBC Economy",       "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"),
    # Central banks / official
    ("Federal Reserve",    "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("ECB Press",          "https://www.ecb.europa.eu/rss/press.html"),
    ("Bank of England",    "https://www.bankofengland.co.uk/rss/news"),
    ("IMF Blog",           "https://www.imf.org/en/Blogs/rss"),
    # Tech / AI
    ("TechCrunch AI",      "https://techcrunch.com/category/artificial-intelligence/feed/"),
    # Crypto
    ("CoinDesk",           "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph",      "https://cointelegraph.com/rss"),
    # Aviation
    ("FlightGlobal",       "https://www.flightglobal.com/rss/"),
]

# How many days back to keep
KEEP_DAYS = 14
# Max items per feed to consider
MAX_PER_FEED = 25
# Final cap on total articles
MAX_ARTICLES = 80

# Topic classification — first match wins. Order matters.
TOPIC_RULES = [
    ("Aviation Finance",   ["aircraft lease", "aircraft leasing", "aviation finance", "aircraft financ",
                            "boeing", "airbus", "airline", "lessor", "smbc aviation", "avolon",
                            "aercap", "air lease", "aviation abs"]),
    ("Cryptocurrency",     ["bitcoin", "btc", "ethereum", "eth", "crypto", "stablecoin", "binance",
                            "coinbase", "blockchain", "defi", "tether", "usdc", "spot etf",
                            "saylor", "strategy mstr"]),
    ("AI",                 ["ai ", "artificial intelligence", "openai", "anthropic", "chatgpt",
                            "gemini", "nvidia", "llm", "generative ai", "agentic", "machine learning",
                            "claude", "deepmind", "mistral", "cohere"]),
    ("Investment Banking", ["m&a", "merger", "acquisition", "acquires", "to buy ", "private equity",
                            "leveraged buyout", "lbo", "deal worth", "billion deal", "takeover",
                            "ipo filed", "ipo prospectus", "underwriter", "goldman sachs", "jpmorgan",
                            "morgan stanley advis", "rothschild", "lazard", "evercore"]),
    ("Banking",            ["bank ", "banking", "deposit", "loan growth", "net interest margin",
                            "basel", "fdic", "regional bank", "credit suisse", "wells fargo",
                            "citigroup", "hsbc", "barclays", "ubs", "deutsche bank", "santander"]),
    ("Macroeconomics",     ["inflation", "cpi", "ppi", "gdp", "unemployment", "jobs report",
                            "payrolls", "federal reserve", "fed ", "fomc", "interest rate",
                            "rate cut", "rate hike", "ecb", "bank of england", "boj",
                            "yield curve", "treasury yield", "recession", "tariff", "trade deficit"]),
    ("Valuation",          ["ipo", "valuation", "valued at", "going public", "listing", "spac",
                            "direct listing", "raises at", "post-money", "pre-money"]),
    ("Stocks",             ["s&p 500", "nasdaq", "dow jones", "stock market", "earnings", "revenue beat",
                            "missed estimates", "guidance", "buyback", "dividend", "share price",
                            "rally", "selloff", "rate cut"]),
    ("Corporate Finance",  ["bond issuance", "debt offering", "credit facility", "refinanc",
                            "convertible", "high-yield", "investment grade", "rating downgrade",
                            "rating upgrade", "moody's", "fitch", "s&p global"]),
]

# Importance heuristic — keywords that bump score
CRITICAL_KEYWORDS = [
    "fed cuts", "fed raises", "fed holds", "fomc decision", "powell announc",
    "breaking", "announces acquisition", "agrees to buy", "ipo prices", "files for ipo",
    "billion deal", "trillion", "bankruptcy", "collapse", "crash", "soars", "plunges",
]
HIGH_KEYWORDS = [
    "earnings", "guidance", "downgrade", "upgrade", "billion", "raise",
    "outlook", "forecast", "miss", "beat", "warns", "outlook cut",
]

# CoinGecko — free, no key. Bitcoin price + 24h change.
CG_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true"

# Voices — finance figures whose mentions in headlines we promote into Voices.
VOICE_FIGURES = [
    {"name": "Jerome Powell",    "role": "Fed Chair",                   "patterns": ["powell"],          "initials": "JP", "bg": "var(--p-cream)"},
    {"name": "Jamie Dimon",      "role": "CEO, JPMorgan Chase",         "patterns": ["dimon"],           "initials": "JD", "bg": "var(--p-sky)"},
    {"name": "Christine Lagarde","role": "President, ECB",              "patterns": ["lagarde"],         "initials": "CL", "bg": "var(--p-mint)"},
    {"name": "Janet Yellen",     "role": "Former Treasury Secretary",   "patterns": ["yellen"],          "initials": "JY", "bg": "var(--p-sand)"},
    {"name": "Scott Bessent",    "role": "U.S. Treasury Secretary",     "patterns": ["bessent"],         "initials": "SB", "bg": "var(--p-sage)"},
    {"name": "Larry Fink",       "role": "CEO, BlackRock",              "patterns": ["larry fink", "blackrock ceo"], "initials": "LF", "bg": "var(--p-lilac)"},
    {"name": "Warren Buffett",   "role": "CEO, Berkshire Hathaway",     "patterns": ["buffett"],         "initials": "WB", "bg": "var(--p-peach)"},
    {"name": "Ray Dalio",        "role": "Founder, Bridgewater",        "patterns": ["ray dalio", "dalio"], "initials": "RD", "bg": "var(--p-blush)"},
    {"name": "Bill Ackman",      "role": "CEO, Pershing Square",        "patterns": ["ackman"],          "initials": "BA", "bg": "var(--p-lemon)"},
    {"name": "Ken Griffin",      "role": "CEO, Citadel",                "patterns": ["ken griffin"],     "initials": "KG", "bg": "var(--p-sky)"},
    {"name": "David Solomon",    "role": "CEO, Goldman Sachs",          "patterns": ["solomon", "goldman ceo"], "initials": "DS", "bg": "var(--p-mint)"},
    {"name": "Andrew Bailey",    "role": "Governor, Bank of England",   "patterns": ["andrew bailey"],   "initials": "AB", "bg": "var(--p-cream)"},
    {"name": "Sundar Pichai",    "role": "CEO, Alphabet",               "patterns": ["pichai"],          "initials": "SP", "bg": "var(--p-sand)"},
    {"name": "Tim Cook",         "role": "CEO, Apple",                  "patterns": ["tim cook"],        "initials": "TC", "bg": "var(--p-blush)"},
    {"name": "Satya Nadella",    "role": "CEO, Microsoft",              "patterns": ["nadella"],         "initials": "SN", "bg": "var(--p-sky)"},
]


# ---------------------------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------------------------

def now_utc():
    return datetime.now(timezone.utc)

def cutoff_iso():
    return (now_utc() - timedelta(days=KEEP_DAYS)).isoformat()

def make_id(url, title):
    h = hashlib.md5((url + title).encode("utf-8")).hexdigest()[:10]
    return f"a_{h}"

def clean_text(s):
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", "", s)              # strip HTML
    s = re.sub(r"\s+", " ", s).strip()         # collapse whitespace
    s = s.replace("&#8217;", "'").replace("&#8216;", "'")
    s = s.replace("&#8220;", '"').replace("&#8221;", '"')
    s = s.replace("&amp;", "&").replace("&nbsp;", " ")
    return s

def parse_pubdate(entry):
    for key in ("published_parsed", "updated_parsed"):
        v = entry.get(key)
        if v:
            try:
                return datetime(*v[:6], tzinfo=timezone.utc)
            except Exception:
                continue
    return now_utc()


# ---------------------------------------------------------------------------
# CLASSIFICATION
# ---------------------------------------------------------------------------

def _kw_match(text, keyword):
    """
    Word-boundary-aware keyword match.
    Multi-word keywords match as substrings (the spaces act as soft boundaries).
    Single-word keywords match only on word boundaries to avoid false positives
    like 'btc' inside 'subtraction'.
    """
    kw = keyword.lower().strip()
    if " " in kw:
        return kw in text
    # Single word: require word boundaries
    return re.search(r"\b" + re.escape(kw) + r"\b", text) is not None


def classify_topic(title, summary):
    text = (title + " " + summary).lower()
    for topic, keywords in TOPIC_RULES:
        for kw in keywords:
            if _kw_match(text, kw):
                return topic
    return None  # uncategorized → drop

def classify_importance(title, summary):
    text = (title + " " + summary).lower()
    for kw in CRITICAL_KEYWORDS:
        if _kw_match(text, kw):
            return "critical"
    for kw in HIGH_KEYWORDS:
        if _kw_match(text, kw):
            return "high"
    return "normal"


# ---------------------------------------------------------------------------
# RSS COLLECTION
# ---------------------------------------------------------------------------

def fetch_feed(name, url, max_items=MAX_PER_FEED):
    items = []
    try:
        # feedparser handles its own HTTP; give it a UA via request_headers
        d = feedparser.parse(url, request_headers={
            "User-Agent": "Mozilla/5.0 (FinanceHub RSS Reader)"
        })
        for entry in d.entries[:max_items]:
            title = clean_text(entry.get("title", ""))
            summary = clean_text(entry.get("summary", "") or entry.get("description", ""))
            if not title:
                continue
            link = entry.get("link", "")
            if not link:
                continue
            pub = parse_pubdate(entry)
            items.append({
                "title": title,
                "summary": summary[:400],   # cap
                "url": link,
                "source": name,
                "published": pub.isoformat(),
            })
    except Exception as e:
        print(f"[warn] feed failed: {name}: {e}")
    return items

def gather_articles():
    raw = []
    for name, url in FEEDS:
        items = fetch_feed(name, url)
        print(f"  {name}: {len(items)} items")
        raw.extend(items)
        time.sleep(0.5)  # gentle on origins

    # Dedupe by (title-normalized) → keep earliest source seen
    seen = {}
    for it in raw:
        k = re.sub(r"[^a-z0-9]", "", it["title"].lower())[:80]
        if k and k not in seen:
            seen[k] = it
    deduped = list(seen.values())

    # Drop too-old
    cutoff = cutoff_iso()
    deduped = [it for it in deduped if it["published"] >= cutoff]

    # Classify topic & importance, drop non-finance items
    classified = []
    for it in deduped:
        topic = classify_topic(it["title"], it["summary"])
        if not topic:
            continue
        it["topic"] = topic
        it["importance"] = classify_importance(it["title"], it["summary"])
        it["id"] = make_id(it["url"], it["title"])
        # Date in YYYY-MM-DD for grouping
        it["date"] = it["published"][:10]
        classified.append(it)

    # Sort by published desc, cap
    classified.sort(key=lambda x: x["published"], reverse=True)
    classified = classified[:MAX_ARTICLES]
    return classified


# ---------------------------------------------------------------------------
# VOICES EXTRACTION
# ---------------------------------------------------------------------------

QUOTE_RE = re.compile(r'["“]([^"“”]{20,260})["”]')

def extract_voices(articles):
    """
    For each named figure, find the most recent article whose title or summary
    mentions them. Try to extract a quoted phrase; otherwise use the summary.
    """
    voices = []
    for figure in VOICE_FIGURES:
        best = None
        for art in articles:  # already sorted desc
            text_lc = (art["title"] + " " + art["summary"]).lower()
            if any(p in text_lc for p in figure["patterns"]):
                # Try to pull a quote
                quote_match = QUOTE_RE.search(art["summary"]) or QUOTE_RE.search(art["title"])
                if quote_match:
                    quote = quote_match.group(1).strip()
                else:
                    quote = art["summary"][:200] or art["title"]
                best = {
                    "name": figure["name"],
                    "role": figure["role"],
                    "initials": figure["initials"],
                    "bg": figure["bg"],
                    "quote": quote,
                    "source": art["source"],
                    "url": art["url"],
                    "date": art["date"],
                }
                break
        if best:
            voices.append(best)
    return voices


# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------

def fmt_pct(x):
    if x is None:
        return "—"
    sign = "+" if x >= 0 else "−"
    return f"{sign}{abs(x):.2f}%"

def fmt_num(x, big=False):
    if x is None:
        return "—"
    if big and x >= 1000:
        return f"{x:,.0f}"
    return f"{x:,.2f}"


def fetch_stooq(symbol):
    """
    Stooq publishes free, no-auth CSV quotes. Reliable from CI runners.
    Returns (price, change_pct) or (None, None).

    Symbol examples:
      ^spx   -> S&P 500
      ^ndx   -> Nasdaq 100
      ^dji   -> Dow Jones
      10usy.b -> US 10Y Treasury yield
      dx.f   -> Dollar Index futures (continuous)

    The CSV format is: Symbol,Date,Time,Open,High,Low,Close,Volume
    """
    url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
    try:
        r = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (FinanceHub)",
        })
        r.raise_for_status()
        lines = r.text.strip().splitlines()
        if len(lines) < 2:
            return None, None
        # Header: Symbol,Date,Time,Open,High,Low,Close,Volume
        cols = lines[1].split(",")
        if len(cols) < 7:
            return None, None
        try:
            open_p = float(cols[3])
            close_p = float(cols[6])
        except ValueError:
            return None, None
        if close_p == 0 or open_p == 0:
            return None, None
        change_pct = (close_p - open_p) / open_p * 100.0
        return close_p, change_pct
    except Exception as e:
        print(f"[warn] stooq {symbol}: {e}")
        return None, None


def fetch_coingecko():
    try:
        r = requests.get(CG_URL, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[warn] coingecko: {e}")
        return {}


def load_previous_kpis():
    """Read the prior data.json so we can fall back to its values if a fetch fails."""
    prev_path = Path(__file__).resolve().parent.parent / "data.json"
    if not prev_path.exists():
        return {}, None
    try:
        prev = json.loads(prev_path.read_text())
        prev_map = {k.get("name"): k for k in prev.get("kpis", []) if k.get("name")}
        prev_gen = prev.get("generated_at")
        return prev_map, prev_gen
    except Exception as e:
        print(f"[warn] could not read previous data.json: {e}")
        return {}, None


def kpi_with_fallback(name, fresh_val, fresh_delta, fresh_dir, prev_map, prev_gen):
    """
    Return a KPI dict. If fresh_val is None, look up the previous run's value
    and tag it as 'stale' so the UI can mark it visually.
    """
    if fresh_val is not None:
        return {
            "name": name,
            "val":  fresh_val,
            "delta": fresh_delta,
            "dir":  fresh_dir,
            "stale": False,
            "as_of": now_utc().isoformat(),
        }
    # Fall back to previous run, if available
    prev = prev_map.get(name)
    if prev and prev.get("val") not in (None, "—"):
        out = dict(prev)
        out["stale"] = True
        # Preserve as_of from previous run; fill in if missing
        if not out.get("as_of"):
            out["as_of"] = prev_gen or now_utc().isoformat()
        return out
    # Nothing fresh, nothing prior
    return {
        "name": name,
        "val": "—",
        "delta": "—",
        "dir": "flat",
        "stale": False,
        "as_of": now_utc().isoformat(),
    }


def build_kpis():
    prev_map, prev_gen = load_previous_kpis()
    kpis = []

    # ---- S&P 500 (Stooq: ^spx) ----
    px, ch = fetch_stooq("^spx")
    val = fmt_num(px, big=True) if px is not None else None
    kpis.append(kpi_with_fallback(
        "S&P 500", val, fmt_pct(ch) if px is not None else None,
        "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
        prev_map, prev_gen,
    ))

    # ---- Nasdaq 100 (Stooq: ^ndx) ----
    px, ch = fetch_stooq("^ndx")
    val = fmt_num(px, big=True) if px is not None else None
    kpis.append(kpi_with_fallback(
        "Nasdaq", val, fmt_pct(ch) if px is not None else None,
        "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
        prev_map, prev_gen,
    ))

    # ---- 10Y Treasury yield (Stooq: 10usy.b) ----
    px, ch = fetch_stooq("10usy.b")
    val = f"{px:.2f}%" if px is not None else None
    delta = (f"{'+' if (ch or 0) >= 0 else '−'}{abs(ch or 0):.2f}%") if px is not None else None
    kpis.append(kpi_with_fallback(
        "10Y Treasury", val, delta,
        "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
        prev_map, prev_gen,
    ))

    # ---- Dollar Index (Stooq: dx.f continuous future) ----
    px, ch = fetch_stooq("dx.f")
    val = fmt_num(px) if px is not None else None
    kpis.append(kpi_with_fallback(
        "Dollar Index", val, fmt_pct(ch) if px is not None else None,
        "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
        prev_map, prev_gen,
    ))

    # ---- Bitcoin (CoinGecko) ----
    cg = fetch_coingecko()
    btc = cg.get("bitcoin", {})
    btc_p = btc.get("usd")
    btc_c = btc.get("usd_24h_change")
    if btc_p is not None:
        val = f"${btc_p/1000:.1f}K" if btc_p >= 1000 else f"${btc_p:,.0f}"
        kpis.append(kpi_with_fallback(
            "Bitcoin", val, fmt_pct(btc_c),
            "up" if (btc_c or 0) > 0 else "down" if (btc_c or 0) < 0 else "flat",
            prev_map, prev_gen,
        ))
    else:
        kpis.append(kpi_with_fallback("Bitcoin", None, None, "flat", prev_map, prev_gen))

    # ---- Ethereum (CoinGecko) ----
    eth = cg.get("ethereum", {})
    eth_p = eth.get("usd")
    eth_c = eth.get("usd_24h_change")
    if eth_p is not None:
        kpis.append(kpi_with_fallback(
            "Ethereum", f"${eth_p:,.0f}", fmt_pct(eth_c),
            "up" if (eth_c or 0) > 0 else "down" if (eth_c or 0) < 0 else "flat",
            prev_map, prev_gen,
        ))
    else:
        kpis.append(kpi_with_fallback("Ethereum", None, None, "flat", prev_map, prev_gen))

    return kpis


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("Finance Hub refresh started:", now_utc().isoformat())

    print("Fetching RSS feeds...")
    articles = gather_articles()
    print(f"Final article count: {len(articles)}")

    print("Extracting Voices...")
    voices = extract_voices(articles)
    print(f"Voices found: {len(voices)}")

    print("Fetching market KPIs...")
    kpis = build_kpis()
    print(f"KPIs: {len(kpis)}")

    out = {
        "generated_at": now_utc().isoformat(),
        "kpis": kpis,
        "voices": voices,
        "news": articles,
    }

    out_path = Path(__file__).resolve().parent.parent / "data.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
