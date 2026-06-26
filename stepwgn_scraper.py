"""
CarSensor Honda StepWGN (current-gen, RP6/RP8, 2022-05～) e:HEV scraper.
Scrapes the hybrid listing (spH), walks ALL pages, and tracks four
current-gen e:HEV grade lines independently:
    エアー EX (Air EX), スパーダ (Spada), スパーダ プレミアムライン (Spada
    Premium Line), and 30周年特別仕様車 (30th Anniversary — its own line, as it
    exists for both Spada & Air EX and carries a price premium).
The Premium Line "Black Edition" sub-variant folds into Premium Line.

Generation filter — the previous gen (RP5, 2019-2021) was ALSO rebadged
"e:HEV", so the page contains old "e:HEV スパーダ G / G EX / モデューロX" cars.
Those are excluded by grade designation (current gen has no "G" grade letter
/ no Modulo X) plus a model-year ≥ 2022 safeguard. Plain エアー (AIR, no EX)
is also excluded.

Each grade is scored independently — every line has its OWN price anchors,
so the more-expensive Premium Line is not penalised for costing more than
Air EX. Scoring is also independent from scraper.py (Freed) / noah_scraper.py.

Requirements:
    pip install requests beautifulsoup4

Usage:
    python stepwgn_scraper.py              # scrape all pages (default)
    python stepwgn_scraper.py --pages 10   # limit pages (faster, for testing)
"""

import argparse
import json
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# Force UTF-8 output so Japanese characters print correctly on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import requests
from bs4 import BeautifulSoup

# ── Config ───────────────────────────────────────────────────────────────────

BASE_URL             = "https://www.carsensor.net/usedcar/bHO/s003/spH/"
DATA_FILE            = Path(__file__).parent / "data" / "stepwgn_data.js"
TELEGRAM_CONFIG_FILE = Path(__file__).parent / "telegram_config.json"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
}
DELAY_SEC  = 2
TOP_N           = 9    # number of top-scored vehicles to store per snapshot
SEAT_CHECK_CAP  = 60   # max detail pages to fetch when filtering 8-seat cars

# Grade lines tracked (current-gen e:HEV only). Matching is gen-gated on the
# "e:HEV" badge (see _match_grade) so the previous-gen i-MMD "スパーダ ハイブリッド G"
# is excluded. Longest-first is enforced in _match_grade because
# "スパーダ プレミアムライン" contains "スパーダ" as a substring.
GRADE_ID_TO_LABEL = {
    "air_ex":           "e:HEV エアー EX",
    "spada":            "e:HEV スパーダ",
    "spada_premium":    "e:HEV スパーダ プレミアムライン",
    "anniversary_30th": "e:HEV 30周年特別仕様車",
}

# Scoring weights (must sum to 1.0) — independent per scraper; same shape
# as Freed/Noah ("the logic should be the same").
WEIGHTS = {
    "price":       0.35,
    "mileage":     0.20,
    "shaken":      0.04,
    "accident":    0.13,
    "warranty":    0.09,
    "maintenance": 0.05,
    "navi":        0.11,
    "camera":      0.03,
}

# ── Fixed price anchors (per-grade, stable over time) ─────────────────────────
#   floor   = realistic floor for a clean used deal (→ scores close to 10)
#   ceiling = top of the observed market range → scores near 0
#   typical = fair/average used price — discount-bonus baseline
# Each grade line is anchored INDEPENDENTLY — the pricier Premium Line / 30th
# Anniversary is not penalised for costing more than Air EX or Spada.
# Tuned from the first clean full scrape (current-gen e:HEV only), observed
# per-grade ranges (万円):
#   air_ex           n=191  min 352 · avg 392 · max 488
#   spada            n=565  min 243 · avg 370 · max 438
#   spada_premium    n=337  min 240 · avg 397 · max 479
#   anniversary_30th n= 34  min 362 · avg 395 · max 448
GRADE_PRICE_ANCHORS: dict[str, tuple[float, float, float]] = {
    # (floor_man, ceiling_man, typical_man)  floor≈min, ceiling≈max, typical≈avg
    "air_ex":           (345.0, 485.0, 392.0),
    "spada":            (245.0, 440.0, 370.0),
    "spada_premium":    (255.0, 480.0, 397.0),
    "anniversary_30th": (360.0, 452.0, 395.0),
}
_DEFAULT_ANCHORS: tuple[float, float, float] = (245.0, 480.0, 385.0)

# ── Equipment-value bonus (per grade) ─────────────────────────────────────────
# Some lines ship with materially more standard equipment than their price would
# suggest. The 30th Anniversary special edition bundles the large 11.4" Honda
# CONNECT nav, multi-view camera, both-row seat heaters and exclusive trim as
# STANDARD — it costs more, but you get more for the money. This flat bonus is
# added to the final score (clamped to 10) so that extra value is credited
# rather than the car merely looking "expensive". Other grades default to 0.
GRADE_VALUE_BONUS: dict[str, float] = {
    "anniversary_30th": 0.3,
}


# ── Scraping ─────────────────────────────────────────────────────────────────

def page_url(n: int) -> str:
    return BASE_URL if n == 1 else f"{BASE_URL}index{n}.html"


def fetch_page(n: int) -> str | None:
    url = page_url(n)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        r.encoding = "utf-8"
        return r.text
    except requests.RequestException as e:
        print(f"  [!] Failed to fetch page {n}: {e}")
        return None


def total_pages_from_html(html: str) -> int:
    m = re.search(r'index(\d+)\.html[^"]*"[^>]*>最後', html)
    if m:
        return int(m.group(1))
    nums = re.findall(r'index(\d+)\.html', html)
    return max((int(n) for n in nums), default=1)


def parse_listings(html: str) -> list[dict]:
    """
    Returns list of vehicle dicts with grade_id, price_man,
    mileage_km, shaken_months, accident, url.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    containers = soup.select("div.cassetteWrap")

    for item in containers:
        grade_id = _match_grade(item.get_text(" ", strip=True))
        if grade_id is None:
            continue
        price = _extract_price(item)
        if not price:
            continue
        details = _extract_details(item)
        # Model-year safeguard: the current gen launched 2022-05. Drop anything
        # older that slipped past the grade-string gen filter (2022 boundary).
        if details.get("year") is not None and details["year"] < 2022:
            continue
        results.append({"grade_id": grade_id, "price_man": price, **details})

    if not containers:
        print("  [!] No listing containers found — selectors may need updating")

    return results


def _match_grade(text: str) -> str | None:
    """Classify a listing title into one of the current-gen e:HEV lines.

    IMPORTANT — "e:HEV" alone does NOT identify the current generation:
    Honda rebadged the *previous* gen (RP5, 2019-2021) from "i-MMD" to
    "e:HEV" too, so the page also contains old "e:HEV スパーダ G",
    "e:HEV スパーダ G EX" and "e:HEV モデューロX" cars. Those are excluded
    here by their grade designation (the current gen has no "G" grade
    letter and no Modulo X). A model-year ≥ 2022 filter (applied in
    parse_listings) is the second safeguard at the 2022 boundary.

    Order matters:
      • 30周年 (30th Anniversary special) → its own line, regardless of base
        trim (it exists for both Spada and Air EX) and priced at a premium.
      • "スパーダ プレミアムライン" contains "スパーダ"; Black Edition contains
        "プレミアムライン" → folds into the Premium Line.
    Plain エアー (AIR, without EX) is intentionally not matched → excluded.
    """
    low = text.lower()
    if "e:hev" not in low:
        return None
    # ── Exclude previous-gen (RP5) grades that are also e:HEV-badged ──────────
    # "スパーダ G" / "スパーダ G EX": G as a standalone token (a space or "EX"
    # follows it) — this avoids matching current features like "スパーダ Gathers…".
    if "モデューロx" in low:
        return None
    if re.search(r"スパーダ[\s　]+G(?=[\s　]|EX|$)", text):
        return None
    # ── Current-gen lines ─────────────────────────────────────────────────────
    if "30周年" in text or "３０周年" in text:
        return "anniversary_30th"
    if "スパーダ" in text and ("プレミアムライン" in text or "プレミアム ライン" in text):
        return "spada_premium"
    if re.search(r"エアー\s*EX", text):
        return "air_ex"
    if "スパーダ" in text:
        return "spada"
    return None


def _extract_price(tag) -> float | None:
    for sel in ("div.basePrice", "div.totalPrice"):
        el = tag.select_one(sel)
        if el:
            m = re.search(r"([\d]+\.?\d*)\s*万円", el.get_text())
            if m:
                return float(m.group(1))
    return None


def _extract_details(item) -> dict:
    """Extract URL, mileage, shaken remaining months, accident, warranty,
    maintenance, and OEM navigation flag from a cassette card."""
    details: dict = {
        "url":           None,
        "mileage_km":    None,
        "shaken_months": None,
        "accident":      None,
        "warranty":      None,   # True = present, False = none
        "maintenance":   None,   # True = 法定整備付, False = 法定整備無
        "navi":          None,   # True = メーカー純正ナビ present, False = ナビレス, None = unknown
        "camera":        None,   # True = マルチビューカメラ detected, None = not mentioned
        "year":          None,   # model year (年式) — used for the gen filter
        "color":         None,   # body color string from listing card
        "photo_url":     None,   # main listing photo (CDN URL)
        "dealer_name":    None,   # dealer / shop display name
        "dealer_rating":  None,   # float 0.0–5.0 from CarSensor evaluation score
        "dealer_reviews": None,   # integer review count shown next to the score
    }

    # Detail page URL
    link = item.select_one('a[name="detail_a"]')
    if link and link.get("href"):
        href = link["href"].split("?")[0]
        details["url"] = "https://www.carsensor.net" + href

    # Spec boxes: 年式 / 走行距離 / 車検 / 修復歴 / 保証 / 整備
    for box in item.select("div.specList__detailBox"):
        text = box.get_text(" ", strip=True)

        if "年式" in text:
            # e.g. "年式 2025 (R07)" — first 4-digit number is the model year
            m = re.search(r"(\d{4})", text)
            if m:
                details["year"] = int(m.group(1))

        elif "走行距離" in text:
            m = re.search(r"([\d.]+)\s*万\s*km", text)
            if m:
                details["mileage_km"] = round(float(m.group(1)) * 10_000)
            else:
                m = re.search(r"(\d+)\s*km", text)
                if m:
                    details["mileage_km"] = int(m.group(1))

        elif "車検" in text:
            # e.g. "車検 2029(R11)年04月"
            m = re.search(r"(\d{4})\(.*?\)年(\d{1,2})月", text)
            if m:
                today = date.today()
                months = (int(m.group(1)) - today.year) * 12 + (int(m.group(2)) - today.month)
                details["shaken_months"] = max(0, months)
            elif "整備付" in text or "車検付" in text:
                details["shaken_months"] = 24   # new shaken included
            elif "なし" in text:
                details["shaken_months"] = 0

        elif "修復歴" in text:
            if "なし" in text:
                details["accident"] = False
            elif "あり" in text:
                details["accident"] = True

        elif "保証" in text:
            if "なし" in text:
                details["warranty"] = False
            elif "付" in text or "あり" in text:
                details["warranty"] = True

        elif "整備" in text:
            if "法定整備付" in text:
                details["maintenance"] = True
            elif "法定整備無" in text or "法定整備なし" in text:
                details["maintenance"] = False

    # Both navi and camera are detected from the free-text headline of the listing card.
    # CarSensor embeds equipment keywords in the title, not in structured spec fields.
    full_text = item.get_text(" ", strip=True)

    # OEM Navigation
    if re.search(r'純正.{0,6}ナビ|ナビ.{0,6}純正|Gathers.{0,4}ナビ|メーカー.{0,4}ナビ', full_text):
        details["navi"] = True
    elif re.search(r'ナビレス|オーディオレス', full_text):
        details["navi"] = False
    # else: None → not mentioned / unknown

    # Multi-view camera (マルチビューカメラ / 全周囲カメラ / アラウンドビューモニター)
    if re.search(r'マルチビューカメラ|マルチビュー|アラウンドビュー|全周囲カメラ|パノラミックビュー', full_text):
        details["camera"] = True
    # else: None → not mentioned (can't reliably detect absence from listing text)

    # Body color — CarSensor puts it as the 2nd <li> in carBodyInfoList
    # (1st item is body type e.g. ミニバン, 2nd is the color name)
    body_items = item.select('li.carBodyInfoList__item')
    if len(body_items) >= 2:
        color_val = body_items[1].get_text(strip=True)
        if color_val and 1 <= len(color_val) <= 30:
            details["color"] = color_val
    elif len(body_items) == 1:
        # Only one item — could be just the color (no body-type listed)
        val = body_items[0].get_text(strip=True)
        BODY_TYPES = {'ミニバン','SUV','セダン','ハッチバック','ワゴン','クーペ','軽','コンパクト'}
        if val not in BODY_TYPES and 1 <= len(val) <= 30:
            details["color"] = val

    # Main listing photo — first <img> whose src is on the ccsrpcma CDN (large photo)
    for img in item.select('img'):
        src = img.get('src') or img.get('data-src') or ''
        if 'ccsrpcma' in src:
            # Normalise protocol-relative URL  (//ccsrpcma... → https://ccsrpcma...)
            if src.startswith('//'):
                src = 'https:' + src
            details["photo_url"] = src
            break

    # Dealer name — CarSensor shows it in a <p class="js_shop"> element
    for sel in (
        'p.js_shop', 'p.shopName a', 'p.shopName',
        'div.shopName a', 'div.shopName',
        '.shopNameLink', '.dealerName a', '.dealerName',
    ):
        el = item.select_one(sel)
        if el:
            # js_shop may include rating text — extract only the name portion
            txt = el.get_text(strip=True)
            # Strip trailing review/rating boilerplate
            name = re.sub(r'\s*クチコミ評価.*$', '', txt).strip()
            if name and 2 <= len(name) <= 60:
                details["dealer_name"] = name
                break

    # Dealer rating + review count — CarSensor uses:
    #   <p class="js_shop">クチコミ評価： 4.6 点（ 55 件）</p>
    #   or <div class="cassetteSub__review">クチコミ評価： 4.6 点（ 55 件）</div>
    for sel in ('p.js_shop', 'div.cassetteSub__review', '.cassetteSub__review'):
        el = item.select_one(sel)
        if el:
            txt = el.get_text(' ', strip=True)
            m_r = re.search(r'(\d+\.\d+|\d)\s*点', txt)
            if m_r:
                val = float(m_r.group(1))
                if 0.5 <= val <= 5.0:
                    details["dealer_rating"] = val
            m_cnt = re.search(r'[（(]\s*(\d+)\s*件\s*[）)]', txt)
            if m_cnt:
                details["dealer_reviews"] = int(m_cnt.group(1))
            if details["dealer_rating"]:
                break

    # Fallback rating from full_text
    if not details["dealer_rating"]:
        m_rate = re.search(r'クチコミ評価\s*[:：]?\s*(\d+\.\d+|\d)\s*点', full_text)
        if m_rate:
            val = float(m_rate.group(1))
            if 0.5 <= val <= 5.0:
                details["dealer_rating"] = val
    if not details["dealer_reviews"]:
        m_rev = re.search(r'[（(]\s*(\d+)\s*件\s*[）)]', full_text)
        if m_rev:
            details["dealer_reviews"] = int(m_rev.group(1))

    return details


def _fetch_seat_count(url: str) -> int | None:
    """
    Fetch a vehicle detail page and return its 乗車定員 (seating capacity).
    Uses a raw-text regex so it's robust to whatever tag structure CarSensor uses.
    Returns the integer seat count, or None if it can't be determined.
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        r.encoding = "utf-8"
        # Regex approach: find "乗車定員" followed by the seat count within ~200 chars.
        # Handles both <dt>/<dd> and <th>/<td> structures without caring about tags.
        m = re.search(r'乗車定員.{1,200}?(\d+)\s*名', r.text, re.DOTALL)
        if m:
            return int(m.group(1))
    except requests.RequestException as e:
        print(f"    [!] Seat-check failed for {url}: {e}")
    return None


# ── Scoring ──────────────────────────────────────────────────────────────────

def score_vehicle(vehicle: dict) -> tuple[float, dict]:
    """
    Score a vehicle 0–10 using fixed, per-grade price anchors.

    Scores are stable across time — they don't shift with the daily pool.
    Returns (score_rounded_to_2dp, breakdown_dict).

    Price (35%): two components averaged (70/30):
      - Range score:    where in [floor, ceiling] does this price sit?
      - Discount score: how much below the grade's typical market price?
    Mileage (20%): linear decay from 0 km — 0 km→10, 100k km→0. No flat zone.
    """
    # ── Price: per-grade fixed anchors ────────────────────────────────────────
    anchors    = GRADE_PRICE_ANCHORS.get(vehicle.get("grade_id", ""), _DEFAULT_ANCHORS)
    floor, ceiling, typical = anchors
    price_man  = vehicle["price_man"]
    price_span = max(ceiling - floor, 1.0)

    range_score    = max(0.0, min(10.0, 10.0 * (ceiling - price_man) / price_span))
    discount_pct   = (typical - price_man) / max(typical, 1.0)   # >0 = below typical
    discount_score = max(0.0, min(10.0, 5.0 + discount_pct * 25.0))
    price_score    = 0.70 * range_score + 0.30 * discount_score

    # ── Mileage: linear from 0, no flat zone ──────────────────────────────────
    km = vehicle.get("mileage_km")
    if km is None:
        mileage_score = 5.0
    else:
        mileage_score = max(0.0, min(10.0, 10.0 - km / 10_000.0))

    # Shaken: 0 months → 2,  ≥ 24 months → 10
    months = vehicle.get("shaken_months")
    shaken_score = (2.0 + min(months, 24) / 24.0 * 8.0) if months is not None else 5.0

    # Accident history
    accident = vehicle.get("accident")
    if accident is False:
        accident_score = 10.0
    elif accident is True:
        accident_score = 1.0
    else:
        accident_score = 5.0   # unknown = neutral

    # Warranty: present = 8, none = 1, unknown = 4
    warranty = vehicle.get("warranty")
    if warranty is True:
        warranty_score = 8.0
    elif warranty is False:
        warranty_score = 1.0
    else:
        warranty_score = 4.0   # unknown = below neutral

    # Legal maintenance (法定整備): with = 10, without = 3, unknown = 5
    maintenance = vehicle.get("maintenance")
    if maintenance is True:
        maintenance_score = 10.0
    elif maintenance is False:
        maintenance_score = 3.0
    else:
        maintenance_score = 5.0  # unknown = neutral

    # OEM Navigation (メーカー純正ナビ): present = 10, absent = 2, unknown = 5 (neutral)
    navi = vehicle.get("navi")
    if navi is True:
        navi_score = 10.0
    elif navi is False:
        navi_score = 2.0
    else:
        navi_score = 5.0  # not mentioned → neutral

    # Multi-view camera: detected = 8, not mentioned = 5 (neutral — can't detect absence)
    camera = vehicle.get("camera")
    camera_score = 8.0 if camera is True else 5.0

    total = (
        price_score       * WEIGHTS["price"]       +
        mileage_score     * WEIGHTS["mileage"]     +
        shaken_score      * WEIGHTS["shaken"]      +
        accident_score    * WEIGHTS["accident"]    +
        warranty_score    * WEIGHTS["warranty"]    +
        maintenance_score * WEIGHTS["maintenance"] +
        navi_score        * WEIGHTS["navi"]        +
        camera_score      * WEIGHTS["camera"]
    )

    # Equipment-value bonus for richly-equipped special editions (e.g. 30th
    # Anniversary). Added on top, then clamped so the score stays within 0–10.
    bonus = GRADE_VALUE_BONUS.get(vehicle.get("grade_id", ""), 0.0)
    total = min(10.0, total + bonus)

    breakdown = {
        "price":       round(price_score,       1),
        "mileage":     round(mileage_score,     1),
        "shaken":      round(shaken_score,      1),
        "accident":    round(accident_score,    1),
        "warranty":    round(warranty_score,    1),
        "maintenance": round(maintenance_score, 1),
        "navi":        round(navi_score,        1),
        "camera":      round(camera_score,      1),
        "equipment":   round(bonus,             2),
    }
    return round(total, 2), breakdown


# ── Statistics ───────────────────────────────────────────────────────────────

def compute_stats(prices: list[float]) -> dict:
    if not prices:
        return {}
    avg = round(sum(prices) / len(prices) * 10_000)
    return {
        "avg":   avg,
        "min":   round(min(prices) * 10_000),
        "max":   round(max(prices) * 10_000),
        "count": len(prices),
    }


def _clean_vehicle(v: dict) -> dict:
    """Serialize a scored vehicle dict for storage in stepwgn_data.js."""
    return {
        "score":           v["score"],
        "score_breakdown": v["score_breakdown"],
        "grade_id":        v["grade_id"],
        "grade_label":     GRADE_ID_TO_LABEL.get(v["grade_id"], v["grade_id"]),
        "price_man":       v["price_man"],
        "year":            v.get("year"),
        "mileage_km":      v.get("mileage_km"),
        "shaken_months":   v.get("shaken_months"),
        "accident":        v.get("accident"),
        "warranty":        v.get("warranty"),
        "maintenance":     v.get("maintenance"),
        "navi":            v.get("navi"),
        "camera":          v.get("camera"),
        "color":           v.get("color"),
        "photo_url":       v.get("photo_url"),
        "dealer_name":     v.get("dealer_name"),
        "dealer_rating":   v.get("dealer_rating"),
        "dealer_reviews":  v.get("dealer_reviews"),
        "seats":           v.get("seats"),
        "url":             v.get("url"),
    }


def build_snapshot(
    by_grade_prices: dict[str, list[float]],
    pages_scraped: int,
    top_vehicles: list[dict],
    category_gems: dict[str, list[dict]],
) -> dict:
    return {
        "date":          str(date.today()),
        "pages_scraped": pages_scraped,
        "by_grade": {
            gid: compute_stats(prices)
            for gid, prices in by_grade_prices.items()
            if prices
        },
        "top_vehicles": [_clean_vehicle(v) for v in top_vehicles],
        "category_gems": {
            key: [_clean_vehicle(v) for v in gems]
            for key, gems in category_gems.items()
        },
    }


# ── Telegram ─────────────────────────────────────────────────────────────────

def _recent_top_n_urls(
    snapshots: list[dict],
    today: str,
    n: int | None = None,
    days: int = 7,
) -> set[str]:
    """Return the set of vehicle URLs that appeared in the top-N list of any
    snapshot within the last *days* days (exclusive of today)."""
    cutoff = str(date.today() - timedelta(days=days))
    urls: set[str] = set()
    for s in snapshots:
        if cutoff <= s["date"] < today:
            vehicles = s.get("top_vehicles", [])
            if n is not None:
                vehicles = vehicles[:n]
            for v in vehicles:
                if v.get("url"):
                    urls.add(v["url"])
    return urls


def build_telegram_alert(v: dict) -> str:
    """Single-vehicle 🚨 alert for an exceptional deal (score ≥ 8.8)."""
    km   = f"{v['mileage_km']:,} km"     if v.get("mileage_km")    is not None else "?"
    shk  = f"{v['shaken_months']}mo車検" if v.get("shaken_months") is not None else "車検?"
    acc  = "✅ 修復歴なし"   if v.get("accident")    is False else ("⚠️ 修復歴あり" if v.get("accident") else "")
    war  = "保証付"          if v.get("warranty")    is True  else ""
    navi = "純正ナビ"        if v.get("navi")        is True  else ""
    cam  = "マルチビュー"    if v.get("camera")      is True  else ""
    extras = "  ·  ".join(x for x in [acc, war, navi, cam] if x)

    lines = [
        f"🚨 <b>EXCEPTIONAL DEAL — {v['score']}/10</b>",
        "",
        f"Honda StepWGN  {v['grade_label']}",
        f"<b>¥{v['price_man']}万</b>  ·  {km}  ·  {shk}",
    ]
    if extras:
        lines.append(extras)
    lines += ["", f"👉 {v.get('url', '')}"]
    return "\n".join(lines)


def load_telegram_config() -> dict | None:
    import os
    env_token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    env_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if env_token and env_chat_id:
        return {"bot_token": env_token, "chat_id": env_chat_id}
    if not TELEGRAM_CONFIG_FILE.exists():
        return None
    try:
        return json.loads(TELEGRAM_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  [!] Could not read telegram_config.json: {e}")
        return None


def send_telegram(message: str) -> None:
    cfg = load_telegram_config()
    if not cfg:
        print("  [!] Telegram config not found — skipping notification")
        return
    url = f"https://api.telegram.org/bot{cfg['bot_token']}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": cfg["chat_id"], "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if r.ok:
            print("  Telegram notification sent.")
        else:
            print(f"  [!] Telegram error {r.status_code}: {r.text}")
    except requests.RequestException as e:
        print(f"  [!] Telegram request failed: {e}")


def build_telegram_message(
    snapshot: dict,
    prev_snapshot: dict | None,
    known_top3_urls: set[str] | None = None,
) -> str:
    """Compact daily summary. known_top3_urls: URLs seen in top-3 in the last
    7 days — vehicles NOT in that set get a 🆕 NEW marker."""
    if known_top3_urls is None:
        known_top3_urls = set()

    today = snapshot["date"]
    lines = [f"<b>🚐 Honda StepWGN e:HEV — {today}</b>"]

    # ── Grade stats: one compact line per grade ───────────────────────────
    SHORT = {
        "air_ex":           "エアー EX",
        "spada":            "スパーダ",
        "spada_premium":    "スパーダ プレミアムライン",
        "anniversary_30th": "30周年特別仕様車",
    }
    grade_lines = []
    for gid, label in SHORT.items():
        cur = snapshot["by_grade"].get(gid)
        if not cur:
            continue
        pre = prev_snapshot["by_grade"].get(gid) if prev_snapshot else None
        count = cur["count"]
        avg   = cur["avg"]
        if pre:
            dc = count - pre["count"]
            da = avg   - pre["avg"]
            c_str = f"{count:,}台 ({'+' if dc>0 else ''}{dc:,})"
            a_str = f"¥{avg:,} ({'+' if da>0 else ''}¥{da:,})"
        else:
            c_str = f"{count:,}台"
            a_str = f"¥{avg:,}"
        grade_lines.append(f"  {label}: {c_str}  avg {a_str}")

    if grade_lines:
        lines.append("")
        lines.extend(grade_lines)

    # ── Top 3 deals ───────────────────────────────────────────────────────
    top = snapshot.get("top_vehicles", [])[:3]
    if top:
        lines.append("")
        lines.append("<b>🏆 Top 3</b>")
        for i, v in enumerate(top, 1):
            new_tag = "" if v.get("url") in known_top3_urls else "  🆕 NEW"
            km  = f"{v['mileage_km']:,}km"     if v.get("mileage_km")    is not None else "?km"
            shk = f"{v['shaken_months']}mo車検" if v.get("shaken_months") is not None else "車検?"
            acc = "✅"  if v.get("accident")  is False else ("⚠️" if v.get("accident") else "")
            war = "保証" if v.get("warranty")  is True  else ""
            navi = "ナビ" if v.get("navi")     is True  else ""
            tags = " ".join(x for x in [acc, war, navi] if x)
            lines.append(
                f"  #{i} <b>[{v['score']}]</b> ¥{v['price_man']}万"
                f"  {km} · {shk}{new_tag}"
            )
            if tags:
                lines.append(f"      {tags}")
            if v.get("url"):
                lines.append(f"      {v['url']}")

    lines.append("")
    lines.append("📊 <a href=\"https://umbasimpy.github.io/japan-used-car-tracker/\">View dashboard</a>")

    return "\n".join(lines)


# ── Data file I/O ─────────────────────────────────────────────────────────────

def load_existing() -> dict:
    if not DATA_FILE.exists():
        return _default_structure()
    src = DATA_FILE.read_text(encoding="utf-8")
    m = re.search(r"window\.STEPWGN_DATA\s*=\s*(\{.*\});", src, re.DOTALL)
    if not m:
        print("  [!] Could not parse existing stepwgn_data.js — starting fresh")
        return _default_structure()
    return json.loads(m.group(1))


def _default_structure() -> dict:
    return {
        "vehicle": {
            "make":          "Honda",
            "model":         "StepWGN",
            "carsensor_url": BASE_URL,
            "generation":    "Current gen e:HEV (2022年05月～)",
        },
        "grades": [
            {"id": "air_ex",           "label": "e:HEV エアー EX",            "label_en": "e:HEV Air EX",            "drive": "FWD"},
            {"id": "spada",            "label": "e:HEV スパーダ",             "label_en": "e:HEV Spada",             "drive": "FWD"},
            {"id": "spada_premium",    "label": "e:HEV スパーダ プレミアムライン", "label_en": "e:HEV Spada Premium Line", "drive": "FWD"},
            {"id": "anniversary_30th", "label": "e:HEV 30周年特別仕様車",      "label_en": "e:HEV 30th Anniversary",  "drive": "FWD"},
        ],
        "snapshots": [],
    }


def save(data: dict) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    js = (
        "// Auto-generated by stepwgn_scraper.py — do not edit manually.\n"
        "// Prices in yen (JPY). Each snapshot = one daily scrape of CarSensor.net.\n"
        f"window.STEPWGN_DATA = {json.dumps(data, ensure_ascii=False, indent=2)};\n"
    )
    DATA_FILE.write_text(js, encoding="utf-8")
    print(f"  Saved → {DATA_FILE}  ({len(data['snapshots'])} total snapshot(s))")


# ── Main ─────────────────────────────────────────────────────────────────────

def run(max_pages: int | None) -> None:
    print("Fetching page 1 …")
    html1 = fetch_page(1)
    if not html1:
        print("Aborting: could not fetch page 1.")
        return

    total = total_pages_from_html(html1)
    limit = min(total, max_pages) if max_pages else total
    print(f"  {total} pages found on CarSensor — will scrape {limit}")

    # ── Collect all vehicle data ──────────────────────────────────────────
    all_vehicles: list[dict] = []

    for n in range(1, limit + 1):
        html = html1 if n == 1 else fetch_page(n)
        if not html:
            break
        listings = parse_listings(html)
        all_vehicles.extend(listings)
        print(f"  Page {n}/{limit}  — {len(listings)} matches  (running total: {len(all_vehicles)})")
        if n < limit:
            time.sleep(DELAY_SEC)

    # ── Per-grade stats ───────────────────────────────────────────────────
    target_grade_ids = set(GRADE_ID_TO_LABEL.keys())
    by_grade_prices: dict[str, list[float]] = {gid: [] for gid in target_grade_ids}
    for v in all_vehicles:
        by_grade_prices[v["grade_id"]].append(v["price_man"])

    # ── Score every vehicle (fixed per-grade anchors — no global stats needed) ──
    for v in all_vehicles:
        v["score"], v["score_breakdown"] = score_vehicle(v)

    # ── Top N across all grades (excluding 8-seat configurations) ────────
    # Seating capacity is only on the detail page, so we fetch detail pages
    # for the top SEAT_CHECK_CAP candidates and skip any with 乗車定員 = 8名
    # (we want the 7-seat captain-chair configuration).
    scored_sorted = sorted(
        [v for v in all_vehicles if "score" in v],
        key=lambda x: -x["score"],
    )
    candidates = scored_sorted[:SEAT_CHECK_CAP]
    top_vehicles: list[dict] = []

    print(f"\nChecking seating capacity for top {len(candidates)} candidates "
          f"(targeting {TOP_N} with 7 seats) …")
    for v in candidates:
        if len(top_vehicles) >= TOP_N:
            break
        url = v.get("url")
        if not url:
            top_vehicles.append(v)   # no URL → can't check, include anyway
            continue
        seats = _fetch_seat_count(url)
        v["seats"] = seats           # store for display (no scoring impact)
        if seats == 8:
            print(f"  ✗ skipped  8-seat  ¥{v['price_man']}万  {v['grade_id']}  {url}")
        else:
            top_vehicles.append(v)
            seat_label = f"{seats}名" if seats else "?名"
            print(f"  ✓ #{len(top_vehicles):2d}  [{v['score']}]  {seat_label}  "
                  f"¥{v['price_man']}万  {v['grade_id']}")
        time.sleep(DELAY_SEC)

    # ── Category Gems ─────────────────────────────────────────────────────
    # Vehicles that excel in ONE specific parameter.
    # Each URL appears in at most ONE category (strict deduplication).
    # We over-pick candidates (GEM_BUFFER), fetch all seat counts up-front,
    # then filter 8-seat vehicles before trimming to GEM_TARGET.
    GEM_TARGET = 3
    GEM_BUFFER = 25

    top_urls = {v.get("url") for v in top_vehicles}
    non_top  = [v for v in scored_sorted if v.get("url") not in top_urls]

    def _pick_candidates(candidates: list[dict], n: int = GEM_BUFFER) -> list[dict]:
        """Pick up to n candidates without cross-category URL tracking.
        Deduplication across categories is handled at finalisation stage."""
        result: list[dict] = []
        seen: set[str] = set()
        for v in candidates:
            url = v.get("url")
            if url and url in seen:
                continue
            result.append(v)
            if url:
                seen.add(url)
            if len(result) >= n:
                break
        return result

    W_ML = WEIGHTS["mileage"]
    raw_candidates: dict[str, list[dict]] = {
        # Price gems: cheapest non-top cars (likely trade-offs on mileage / shaken)
        "price": _pick_candidates(sorted(
            non_top,
            key=lambda x: (-x["score_breakdown"]["price"], -x["score"]),
        )),
        # High-mileage gems: ≥40k km, ranked excluding mileage penalty
        # (StepWGN e:HEV is a 2022+ design, so used units rarely hit huge km —
        #  the bar is a touch lower than Noah's 50k)
        "high_mileage": _pick_candidates(sorted(
            [v for v in non_top if (v.get("mileage_km") or 0) >= 40_000],
            key=lambda x: -(
                (x["score"] - x["score_breakdown"]["mileage"] * W_ML) / (1 - W_ML)
            ),
        )),
        # No-shaken gems: no JCI remaining, buyer arranges inspection
        "no_shaken": _pick_candidates(sorted(
            [v for v in scored_sorted if not v.get("shaken_months")],
            key=lambda x: -x["score"],
        )),
    }

    # ── Seat counts for ALL gem candidates ────────────────────────────────────
    gem_by_url: dict[str, dict] = {}
    for gem_list in raw_candidates.values():
        for v in gem_list:
            u = v.get("url")
            if u and u not in gem_by_url and v.get("seats") is None:
                gem_by_url[u] = v

    if gem_by_url:
        print(f"\nFetching seat counts for {len(gem_by_url)} gem vehicle(s) …")
        for u, v in gem_by_url.items():
            seats = _fetch_seat_count(u)
            v["seats"] = seats
            print(f"  {u}  → {seats}名" if seats else f"  {u}  → ?")
            time.sleep(DELAY_SEC)

    # ── Finalise: skip 8-seat, deduplicate across categories, trim to target ──
    used_gem_urls: set[str] = set()

    def _finalize_gems(candidates: list[dict]) -> list[dict]:
        result: list[dict] = []
        for v in candidates:
            url = v.get("url")
            if url and url in used_gem_urls:
                continue      # already claimed by a higher-priority category
            if v.get("seats") == 8:
                continue      # 8-seat vehicles excluded from all gem categories
            result.append(v)
            if url:
                used_gem_urls.add(url)
            if len(result) >= GEM_TARGET:
                break
        return result

    category_gems = {
        "price":        _finalize_gems(raw_candidates["price"]),
        "high_mileage": _finalize_gems(raw_candidates["high_mileage"]),
        "no_shaken":    _finalize_gems(raw_candidates["no_shaken"]),
    }

    print("\nCategory gems:")
    for cat, gems in category_gems.items():
        print(f"  {cat}:")
        for g in gems:
            km  = f"{g['mileage_km']:,} km" if g.get("mileage_km") is not None else "km=?"
            shk = f"{g.get('shaken_months')}mo" if g.get("shaken_months") else "no shaken"
            seat_label = f"  {g['seats']}名" if g.get("seats") is not None else ""
            print(f"    [{g['score']}] ¥{g['price_man']}万  {km}  {shk}{seat_label}  {g.get('url','')}")

    # ── Save ──────────────────────────────────────────────────────────────
    snapshot = build_snapshot(by_grade_prices, limit, top_vehicles, category_gems)
    data     = load_existing()
    today    = str(date.today())

    # Always refresh the canonical metadata (vehicle + grade list) so schema
    # changes — e.g. adding the 30th Anniversary line — propagate to existing
    # data files instead of being frozen at whatever the first run wrote.
    canonical = _default_structure()
    data["vehicle"] = canonical["vehicle"]
    data["grades"]  = canonical["grades"]

    existing_today = next((s for s in data["snapshots"] if s["date"] == today), None)
    prev_snapshot  = None
    if existing_today:
        prev_snapshot = next(
            (s for s in reversed(sorted(data["snapshots"], key=lambda s: s["date"])) if s["date"] < today),
            None,
        )
    else:
        if data["snapshots"]:
            prev_snapshot = sorted(data["snapshots"], key=lambda s: s["date"])[-1]

    data["snapshots"] = [s for s in data["snapshots"] if s["date"] != today]
    data["snapshots"].append(snapshot)
    data["snapshots"].sort(key=lambda s: s["date"])
    save(data)

    # ── Telegram ──────────────────────────────────────────────────────────
    # URLs that appeared anywhere in top_vehicles in the last 7 days
    known_all    = _recent_top_n_urls(data["snapshots"], today, n=None, days=7)
    # URLs that appeared in the top-3 specifically in the last 7 days
    known_top3   = _recent_top_n_urls(data["snapshots"], today, n=3,    days=7)

    # 🚨 Alert for any exceptional vehicle (score ≥ 8.8) not seen recently
    for v in snapshot.get("top_vehicles", []):
        if v.get("score", 0) >= 8.8 and v.get("url") and v["url"] not in known_all:
            send_telegram(build_telegram_alert(v))

    # Regular compact daily summary
    msg = build_telegram_message(snapshot, prev_snapshot, known_top3)
    send_telegram(msg)

    # ── Console summary ───────────────────────────────────────────────────
    print("\nResults:")
    for gid, prices in by_grade_prices.items():
        if prices:
            s = compute_stats(prices)
            print(f"  {gid:22s}  {s['count']:4d} vehicles  "
                  f"avg Y{s['avg']:,}  min Y{s['min']:,}  max Y{s['max']:,}")
        else:
            print(f"  {gid:22s}     0 vehicles  (no listings found)")

    print(f"\nTop {TOP_N} vehicles by score:")
    for i, v in enumerate(top_vehicles, 1):
        km   = f"{v['mileage_km']:,} km" if v.get("mileage_km") is not None else "km=?"
        shk  = f"{v['shaken_months']}mo"  if v.get("shaken_months") is not None else "shaken=?"
        acc  = "clean"    if v.get("accident")    is False else ("accident" if v.get("accident")    else "acc=?")
        navi   = "純正ナビ"   if v.get("navi")   is True else ("ナビレス" if v.get("navi") is False else "navi=?")
        cam    = "マルチビュー" if v.get("camera") is True else ""
        print(f"  #{i} [{v['score']:4.1f}] {v['grade_id']:22s}  "
              f"¥{v['price_man']}万  {km}  {shk}  {acc}  {navi}  {cam}")
        print(f"       {v.get('url', '')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pages", type=int, default=None,
        help="Max pages to scrape (default: all)"
    )
    args = parser.parse_args()
    run(args.pages)
