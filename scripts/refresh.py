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

# Macro KPIs — Yahoo Finance unofficial endpoints (no key required)
# Symbols: ^GSPC = S&P 500, ^TNX = 10Y Treasury yield (x10), DX-Y.NYB = DXY
KPI_SYMBOLS = {
    "S&P 500":      "^GSPC",
    "10Y Treasury": "^TNX",
    "Nasdaq":       "^IXIC",
    "Dollar Index": "DX-Y.NYB",
}

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

def fetch_yahoo_quote(symbol):
    """
    Hits Yahoo Finance's public quote endpoint. No API key.
    Returns (price, change_pct) or (None, None) on failure.
    """
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    try:
        r = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (FinanceHub)",
            "Accept": "application/json",
        })
        r.raise_for_status()
        data = r.json()
        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            return None, None
        q = results[0]
        return q.get("regularMarketPrice"), q.get("regularMarketChangePercent")
    except Exception as e:
        print(f"[warn] yahoo {symbol}: {e}")
        return None, None

def fetch_coingecko():
    try:
        r = requests.get(CG_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data
    except Exception as e:
        print(f"[warn] coingecko: {e}")
        return {}

def build_kpis():
    kpis = []

    # S&P 500
    px, ch = fetch_yahoo_quote("^GSPC")
    kpis.append({
        "name": "S&P 500",
        "val":  fmt_num(px, big=True) if px else "—",
        "delta": fmt_pct(ch),
        "dir":  "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
    })

    # Nasdaq
    px, ch = fetch_yahoo_quote("^IXIC")
    kpis.append({
        "name": "Nasdaq",
        "val":  fmt_num(px, big=True) if px else "—",
        "delta": fmt_pct(ch),
        "dir":  "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
    })

    # 10Y Treasury (Yahoo's ^TNX is yield * 1, e.g. 4.21 means 4.21%)
    px, ch = fetch_yahoo_quote("^TNX")
    if px is not None:
        kpis.append({
            "name": "10Y Treasury",
            "val":  f"{px:.2f}%",
            "delta": f"{'+' if (ch or 0) >= 0 else '−'}{abs(ch or 0):.2f}%",
            "dir":  "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
        })
    else:
        kpis.append({"name": "10Y Treasury", "val": "—", "delta": "—", "dir": "flat"})

    # Dollar Index
    px, ch = fetch_yahoo_quote("DX-Y.NYB")
    kpis.append({
        "name": "Dollar Index",
        "val":  fmt_num(px) if px else "—",
        "delta": fmt_pct(ch),
        "dir":  "up" if (ch or 0) > 0 else "down" if (ch or 0) < 0 else "flat",
    })

    # Bitcoin (CoinGecko)
    cg = fetch_coingecko()
    btc = cg.get("bitcoin", {})
    btc_p = btc.get("usd")
    btc_c = btc.get("usd_24h_change")
    if btc_p is not None:
        if btc_p >= 1000:
            val = f"${btc_p/1000:.1f}K"
        else:
            val = f"${btc_p:,.0f}"
        kpis.append({
            "name": "Bitcoin",
            "val":  val,
            "delta": fmt_pct(btc_c),
            "dir":  "up" if (btc_c or 0) > 0 else "down" if (btc_c or 0) < 0 else "flat",
        })
    else:
        kpis.append({"name": "Bitcoin", "val": "—", "delta": "—", "dir": "flat"})

    # Ethereum
    eth = cg.get("ethereum", {})
    eth_p = eth.get("usd")
    eth_c = eth.get("usd_24h_change")
    if eth_p is not None:
        kpis.append({
            "name": "Ethereum",
            "val":  f"${eth_p:,.0f}",
            "delta": fmt_pct(eth_c),
            "dir":  "up" if (eth_c or 0) > 0 else "down" if (eth_c or 0) < 0 else "flat",
        })
    else:
        kpis.append({"name": "Ethereum", "val": "—", "delta": "—", "dir": "flat"})

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
