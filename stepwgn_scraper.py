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
import math
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
LOWKM_DATA_FILE      = Path(__file__).parent / "data" / "stepwgn_lowkm_data.js"
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
TOP_N           = 40    # number of top-scored vehicles to store per snapshot
SEAT_CHECK_CAP  = 160   # max detail pages to fetch when filtering 8-seat cars

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

# Scoring weights (must sum to 1.0) — independent per scraper.
# Tuned for StepWGN: navi and the multi-view camera are weighted UP, and mileage
# DOWN. Rationale — near-new "登録済未使用車" with ~0 km otherwise score very high on
# mileage despite being priced like new (no real bargain), and these stripped
# units typically lack nav/camera. Rewarding equipment and easing the near-zero-km
# mileage bonus pushes those cars down and surfaces genuinely well-equipped value.
WEIGHTS = {
    "price":       0.42,   # incl. the old camera weight (camera is now an accessory bonus)
    "mileage":     0.12,
    "shaken":      0.04,
    "accident":    0.13,
    "warranty":    0.08,
    "maintenance": 0.03,
    "navi":        0.18,   # a screen (nav OR display audio) installed; no screen is penalised hard
}

# Price is scored across ALL grades POOLED (absolute cheapness — see run()), so the
# cheapest StepWGN overall scores 10 regardless of grade.

# ── Accessories / option packages ─────────────────────────────────────────────
# The multi-view camera is the one worthwhile factory addon on the StepWGN, so it
# is tracked as an accessory (cumulative bonus + purple badge), not a weighted
# factor. (label, bonus, regex)
OPTION_PACKAGES = [
    ("マルチビュー", 0.30, re.compile(r"マルチビューカメラ|マルチビュー|アラウンドビュー|全周囲カメラ|全方位カメラ|パノラミックビュー")),
]
_OPTION_BONUS = {label: bonus for label, bonus, _ in OPTION_PACKAGES}

# ── Equipment-value bonus (per grade) ─────────────────────────────────────────
# The 30th Anniversary special edition bundles materially more standard equipment
# (big Honda CONNECT nav, dual-row seat heaters, exclusive trim) than its price
# implies — a flat per-grade bonus credits that. Added on top of any accessory
# bonus, clamped to 10.
GRADE_VALUE_BONUS: dict[str, float] = {
    "anniversary_30th": 0.3,
}

# ── New-car reference prices (for the used-vs-new gap on each card) ────────────
# Honda メーカー希望小売価格 (税込), current lineup, e:HEV FF 7-seat (万円), from
# Honda / webCG (May 2025) and the 30th-anniversary release (Dec 2025).
# To approximate an out-the-door 支払総額 (comparable to the used total price) we add
# the 諸費用 subtotal from an actual Honda 見積 (¥136,370 ≈ ¥13.6万):
#   自動車税 21,000 + 環境性能割 0 + 重量税 15,000 + 自賠責(37ヶ月) 24,190
#   + 手続き費用 47,300 + 預かり法定費用 6,400 + リサイクル 22,480 = 136,370円.
# The マルチビューカメラ (incl. the nav needed to use it) runs ≈ ¥10万 as a factory
# option on Air EX / Spada; it is STANDARD on Premium Line and the 30th editions.
NEW_CAR_FEES_MAN       = 13.6    # 諸費用 subtotal (from a real StepWGN e:HEV 見積)
NEW_CAR_CAMERA_OPT_MAN = 10.0    # マルチビューカメラ option (as seen on the 見積)
NEW_CAR_MSRP_MAN = {
    "air_ex":        393.8,      # camera optional
    "spada":         399.85,     # camera optional
    "spada_premium": 426.8,      # camera standard
    # anniversary_30th resolved per listing (Air EX-based 409.86 / Spada-based 415.91)
}


def _new_car_total(grade_id: str, options: list | None, text: str) -> float | None:
    """Out-the-door new-car price (万円) for the SAME configuration as this used car:
    grade MSRP + 諸費用, plus the multi-view camera option only when the used car has
    it and the camera isn't already standard on that grade."""
    opts = options or []
    has_cam = "マルチビュー" in opts
    if grade_id == "anniversary_30th":
        base = 409.86 if re.search(r"エアー\s*EX", text) else 415.91
        cam_standard = True
    elif grade_id == "spada_premium":
        base = NEW_CAR_MSRP_MAN["spada_premium"]; cam_standard = True
    elif grade_id in NEW_CAR_MSRP_MAN:
        base = NEW_CAR_MSRP_MAN[grade_id]; cam_standard = False
    else:
        return None
    total = base + NEW_CAR_FEES_MAN
    if has_cam and not cam_standard:
        total += NEW_CAR_CAMERA_OPT_MAN
    return round(total, 1)


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


def _is_flood_damaged(text: str) -> bool:
    """True if the listing is a flood-damaged car (冠水歴車 / 水没車).
    Guards against false positives like 冠水歴なし / 冠水歴無し (= NO flood history),
    which describe a clean car and must NOT be excluded."""
    return bool(
        re.search(r'冠水(?!.{0,4}(?:なし|無))', text) or
        re.search(r'水没(?!.{0,4}(?:なし|無))', text)
    )


def parse_listings(html: str) -> list[dict]:
    """
    Returns list of vehicle dicts with grade_id, price_man,
    mileage_km, shaken_months, accident, url.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    containers = soup.select("div.cassetteWrap")

    for item in containers:
        item_text = item.get_text(" ", strip=True)
        # Exclude flood-damaged cars (冠水歴車 / 水没車) entirely — never scored or
        # shown. Keep cars that explicitly advertise NO flood history (冠水歴なし).
        if _is_flood_damaged(item_text):
            continue
        grade_id = _match_grade(item_text)
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
        details["new_price_man"] = _new_car_total(grade_id, details.get("options"), item_text)
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
    # Use 支払総額 (total out-the-door price) first, falling back to 車両本体価格
    # (vehicle-only) only if the total is absent. The fees bundled into the total
    # vary a lot between listings, so the total is the truly comparable cost.
    for sel in ("div.totalPrice", "div.basePrice"):
        el = tag.select_one(sel)
        if el:
            m = re.search(r"([\d]+\.?\d*)\s*万円", el.get_text().replace(" ", ""))
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
        "navi":          None,   # True = screen (nav OR display audio) present, False = none, None = unknown
        "screen_size":   None,   # float inches (9 / 11.4) parsed from the listing, else None
        "camera":        None,   # legacy field (multi-view camera is now an accessory)
        "unused":        None,   # True = 未使用車 / 登録済未使用車 (near-new, priced like new)
        "options":       [],     # detected accessories (cumulative bonus)
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

    # Head-unit / screen ("navi" factor — what matters is that a SCREEN is
    # installed, whether a nav or a display audio unit). A car is positive if it
    # has an installed nav OR a ディスプレイオーディオ (display audio screen).
    # It is negative only when explicitly screen-less: ナビレス / オーディオレス /
    # モニターレス, or a "ナビ装着用" nav-ready package with no actual unit.
    # ("ナビ装着用スペシャルパッケージ" = wired/mounted for a unit but none fitted.)
    INSTALLED_NAVI = (
        r'純正[^\s　]{0,6}ナビ|'                      # 純正ナビ / 純正9型ナビ / 純正コネクトナビ
        r'\d+\.?\d*\s*(?:インチ|型)[^\s　]{0,6}ナビ|'  # 9インチナビ / 11.4型コネクトナビ
        r'メモリーナビ|HDDナビ|SDナビ|DAナビ|インターナビ|コネクトナビ|メーカーナビ|'
        r'ナビTV|フルセグ[^\s　]{0,4}ナビ|ナビ[^\s　]{0,4}フルセグ'
    )
    has_screen = bool(
        re.search(INSTALLED_NAVI, full_text) or
        "ディスプレイオーディオ" in full_text or "ディスプレイ オーディオ" in full_text or
        "ディスプレイオーデイオ" in full_text
    )
    if re.search(r'ナビレス|オーディオレス|モニターレス', full_text):
        details["navi"] = False        # explicitly no screen
    elif "ナビ装着用" in full_text and not has_screen:
        details["navi"] = False        # nav-ready package only → no unit fitted
    elif has_screen:
        details["navi"] = True
    # else: None → not mentioned / unknown

    # Screen SIZE (inches) — the size token must sit right next to a screen word
    # (ナビ / ディスプレイ / オーディオ / コネクト) so we don't pick up アルミ 16インチ
    # wheels. A plausibility clamp (6–13") is a second backstop against wheels.
    # Screen SIZE (inches). The size token must sit ADJACENT (no space) to a screen
    # word — either just before it ("11.4型ナビ", "9インチナビ") or just after it
    # ("ディスプレイオーディオ9型") — so wheel sizes like "アルミ 16インチ" aren't read
    # as screens. A 6–13" clamp is a second backstop (StepWGN wheels are ≥16"). A
    # size that's split from its screen word by a space is left unknown on purpose.
    _SCREEN_WORD = r'ナビ|ディスプレイ|オーディオ|コネクト|モニター|CONNECT'
    _INCH = r'型|インチ|[iI][nN](?:[cC][hH])?'   # 型 / インチ / in / inch (some listings write "11.4in")
    m_sz = re.search(
        rf'(\d{{1,2}}(?:\.\d)?)\s*(?:{_INCH})[^\s　]{{0,6}}(?:{_SCREEN_WORD})',
        full_text,
    ) or re.search(
        rf'(?:{_SCREEN_WORD})[^\d\s　]{{0,4}}(\d{{1,2}}(?:\.\d)?)\s*(?:{_INCH})',
        full_text,
    )
    if m_sz:
        try:
            sz = float(m_sz.group(1))
            if 6.0 <= sz <= 13.0:
                details["screen_size"] = sz
        except ValueError:
            pass

    # Accessories — the multi-view camera (NOT a plain バックカメラ; and NOT
    # CarSensor's "360°画像付" photo badge) is tracked as a cumulative accessory
    # bonus + purple badge rather than a weighted factor.
    details["options"] = [label for label, _, rx in OPTION_PACKAGES if rx.search(full_text)]

    # Registered-but-unused / near-new stock (未使用車 / 登録済未使用車). These sit at
    # ~0 km and are priced like new — excluded from the Low-KM (Newest) view, which
    # wants genuinely registered used cars. NOTE: match only 未使用 — do NOT key on
    # 新車 (as "新車保証" = remaining new-car warranty is common on real used cars).
    details["unused"] = bool(re.search(r'未使用', full_text))

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

def score_vehicle(vehicle: dict, price_bounds: dict[str, tuple[float, float]]) -> tuple[float, dict]:
    """
    Score a vehicle 0–10. Returns (score_rounded_to_2dp, breakdown_dict).

    Price: RELATIVE within the grade and intentionally TIGHT — the cheapest car
    in a grade scores 10 and the score falls off steeply, so a 10 is effectively
    reserved for the cheapest (only a small band near it scores high).
    Mileage: linear decay from 0 km — 0 km→10, 100k km→0.
    """
    # ── Price: relative within grade, tight (cheapest = 10) ───────────────────
    # price_bounds[grade] = (lo, hi): lo = grade minimum, hi = grade 75th pct.
    # Cheapest car → 10; falls off steeply; cars at/above the 75th pct → ~0.
    price_man = vehicle["price_man"]
    lo, hi    = price_bounds.get(vehicle.get("grade_id", ""), (price_man, price_man))
    span      = hi - lo
    if span <= 0:
        price_score = 10.0          # only one price point in the grade
    else:
        price_score = max(0.0, min(10.0, 10.0 * (hi - price_man) / span))

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

    # Screen / head unit (nav OR display audio): installed = 10; otherwise 0.
    # Both confirmed-absent AND unclear/unmentioned score 0 — only a clearly
    # installed screen earns points.
    navi = vehicle.get("navi")
    navi_score = 10.0 if navi is True else 0.0

    total = (
        price_score       * WEIGHTS["price"]       +
        mileage_score     * WEIGHTS["mileage"]     +
        shaken_score      * WEIGHTS["shaken"]      +
        accident_score    * WEIGHTS["accident"]    +
        warranty_score    * WEIGHTS["warranty"]    +
        maintenance_score * WEIGHTS["maintenance"] +
        navi_score        * WEIGHTS["navi"]
    )

    # Accessory bonus (multi-view camera) + per-grade equipment bonus (30th
    # Anniversary). Cumulative, added on top, then clamped to 10.
    opt_bonus   = sum(_OPTION_BONUS.get(o, 0.0) for o in (vehicle.get("options") or []))
    grade_bonus = GRADE_VALUE_BONUS.get(vehicle.get("grade_id", ""), 0.0)
    bonus = opt_bonus + grade_bonus
    total = min(10.0, total + bonus)

    breakdown = {
        "price":       round(price_score,       1),
        "mileage":     round(mileage_score,     1),
        "shaken":      round(shaken_score,      1),
        "accident":    round(accident_score,    1),
        "warranty":    round(warranty_score,    1),
        "maintenance": round(maintenance_score, 1),
        "navi":        round(navi_score,        1),
        "equipment":   round(bonus,             2),
    }
    return round(total, 2), breakdown


# ── Statistics ───────────────────────────────────────────────────────────────

def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Linear-interpolated percentile of an already-sorted list."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def compute_price_bounds(by_grade_prices: dict[str, list[float]]) -> dict[str, tuple[float, float]]:
    """Per-grade (lo, hi) for the relative/tight price score: lo = grade minimum,
    hi = grade 75th percentile (robust high — keeps a few pricey outliers from
    flattening the scale). Cheapest car scores 10, cars at/above hi score ~0."""
    bounds: dict[str, tuple[float, float]] = {}
    for gid, prices in by_grade_prices.items():
        if not prices:
            continue
        s = sorted(prices)
        lo = s[0]
        hi = _percentile(s, 0.75)
        if hi <= lo:                     # tiny/degenerate grade — widen slightly
            hi = lo * 1.10
        bounds[gid] = (lo, hi)
    return bounds


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
        "new_price_man":   v.get("new_price_man"),
        "year":            v.get("year"),
        "mileage_km":      v.get("mileage_km"),
        "shaken_months":   v.get("shaken_months"),
        "accident":        v.get("accident"),
        "warranty":        v.get("warranty"),
        "maintenance":     v.get("maintenance"),
        "navi":            v.get("navi"),
        "screen_size":     v.get("screen_size"),
        "camera":          v.get("camera"),
        "unused":          v.get("unused"),
        "options":         v.get("options", []),
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


# ── "Best Value" view ─────────────────────────────────────────────────────────
# A SECOND dataset built from the very same scrape (no extra requests). The
# must-have parameters are a HARD GATE — a car only appears if it has ALL of:
#   1. ≤10,000 km (LOWKM_MAX_KM)             2. a confirmed clean record (修復歴なし)
#   3. a multi-view camera (マルチビュー)     4. an installed screen
#   5. a 7-seat layout (verified on detail)
# Among the cars that clear the gate, PRICE is the biggest single factor (cheaper =
# better) but km / year / screen size still count, so a slightly pricier but clearly
# better car can edge ahead.
LOWKM_MAX_KM = 10_000   # HARD cap — only cars at/under 10,000 km appear in this view
LOWKM_WEIGHTS = {
    "price":    0.45,   # the biggest single factor — cheaper is better ("price is kind")
    "mileage":  0.12,   # still counts (all cars are ≤10k km, so this is fine-grained)
    "year":     0.18,   # newer ranks higher
    "screen":   0.15,   # a bigger factory screen (11.4") ranks above the 9"
    "shaken":   0.05,
    "warranty": 0.05,
}
LOWKM_MILEAGE_KNEE    = 5_000     # km — the "sweet spot" edge; low-km taper ends here
LOWKM_KNEE_SCORE      = 9.0       # score at the knee (0 km = 10.0); above it, exp decay
LOWKM_MILEAGE_TAU     = 10_000    # km — exponential decay constant beyond the knee
LOWKM_YEAR_STEP       = 2.0       # score points lost per model-year of age


def score_vehicle_lowkm(
    vehicle: dict,
    price_bounds: dict[str, tuple[float, float]],
    current_year: int,
) -> tuple[float, dict]:
    """Score a camera+clean StepWGN 0–10 for the Low-KM (Newest) view.

    Mileage: a gentle taper from 10 (0 km) down to LOWKM_KNEE_SCORE at the ~5k-km
    knee — so a 10 km car still beats a 5k car but only slightly, both staying at
    the top — then 9·e^(−Δkm/τ) exponential fall-off with every extra km above it.
    Year: newest = 10, losing LOWKM_YEAR_STEP per year of age.
    Price: relative within the pooled camera+clean set (cheapest = 10).
    """
    # ── Price: relative within the pooled camera+clean set (cheapest = 10) ────
    price_man = vehicle["price_man"]
    lo, hi    = price_bounds.get(vehicle.get("grade_id", ""), (price_man, price_man))
    span      = hi - lo
    price_score = 10.0 if span <= 0 else max(0.0, min(10.0, 10.0 * (hi - price_man) / span))

    # ── Mileage: gentle low-km taper (0 km = 10 → knee = 9.0), then exp decay ──
    km = vehicle.get("mileage_km")
    if km is None:
        mileage_score = 5.0
    elif km <= LOWKM_MILEAGE_KNEE:
        # 0 km → 10.0, knee → LOWKM_KNEE_SCORE: a 10 km car edges out a 5k one,
        # but both stay near the top (not scored identically).
        mileage_score = 10.0 - (10.0 - LOWKM_KNEE_SCORE) * (km / LOWKM_MILEAGE_KNEE)
    else:
        mileage_score = LOWKM_KNEE_SCORE * math.exp(-(km - LOWKM_MILEAGE_KNEE) / LOWKM_MILEAGE_TAU)

    # ── Registration year: newest = 10 ────────────────────────────────────────
    yr = vehicle.get("year")
    if yr is None:
        year_score = 5.0
    else:
        year_score = max(0.0, min(10.0, 10.0 - (current_year - yr) * LOWKM_YEAR_STEP))

    # ── Screen: a screen is required to reach this view, so what varies is SIZE.
    #    Honda CONNECT nav is 9" or 11.4" — the bigger unit ranks higher. When the
    #    screen is only inferred but the size isn't stated, it lands mid-high. When
    #    the screen itself is UNSURE (navi None — the "unverified" bucket), it's
    #    neutral; a confirmed-absent screen scores 0 (never in the main pool). ──
    navi = vehicle.get("navi")
    size = vehicle.get("screen_size")
    if navi is True:
        if size is not None and size >= 11:
            screen_score = 10.0          # 11.4" big screen
        elif size is not None and size <= 9.5:
            screen_score = 6.0           # 9" standard screen
        else:
            screen_score = 8.0           # screen present, size not stated
    elif navi is None:
        screen_score = 5.0               # unverified — the "not sure" bucket
    else:
        screen_score = 0.0               # confirmed no screen

    # Shaken: 0 months → 2, ≥ 24 months → 10 (same shape as the main view)
    months = vehicle.get("shaken_months")
    shaken_score = (2.0 + min(months, 24) / 24.0 * 8.0) if months is not None else 5.0

    # Warranty: present = 8, none = 1, unknown = 4
    warranty = vehicle.get("warranty")
    warranty_score = 8.0 if warranty is True else (1.0 if warranty is False else 4.0)

    total = (
        price_score    * LOWKM_WEIGHTS["price"]    +
        mileage_score  * LOWKM_WEIGHTS["mileage"]  +
        year_score     * LOWKM_WEIGHTS["year"]     +
        screen_score   * LOWKM_WEIGHTS["screen"]   +
        shaken_score   * LOWKM_WEIGHTS["shaken"]   +
        warranty_score * LOWKM_WEIGHTS["warranty"]
    )
    breakdown = {
        "price":    round(price_score,    1),
        "mileage":  round(mileage_score,  1),
        "year":     round(year_score,     1),
        "screen":   round(screen_score,   1),
        "shaken":   round(shaken_score,   1),
        "warranty": round(warranty_score, 1),
    }
    return round(min(10.0, total), 2), breakdown


def _default_structure_lowkm() -> dict:
    base = _default_structure()
    base["vehicle"]["generation"] = "Current gen e:HEV (2022年05月～) · Newest / Low-KM view"
    return base


def load_existing_lowkm() -> dict:
    if not LOWKM_DATA_FILE.exists():
        return _default_structure_lowkm()
    src = LOWKM_DATA_FILE.read_text(encoding="utf-8")
    m = re.search(r"window\.STEPWGN_LOWKM_DATA\s*=\s*(\{.*\});", src, re.DOTALL)
    if not m:
        print("  [!] Could not parse existing stepwgn_lowkm_data.js — starting fresh")
        return _default_structure_lowkm()
    return json.loads(m.group(1))


def save_lowkm(data: dict) -> None:
    LOWKM_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    js = (
        "// Auto-generated by stepwgn_scraper.py (Best-Value view) — do not edit manually.\n"
        "// Same scrape as stepwgn_data.js, re-ranked by value among cars that clear the\n"
        "// gate: ≤10,000 km + clean record + multi-view camera + screen + 7-seat layout.\n"
        "// 未使用車 included (badged). Price is the biggest factor; km/year/screen also count.\n"
        f"window.STEPWGN_LOWKM_DATA = {json.dumps(data, ensure_ascii=False, indent=2)};\n"
    )
    LOWKM_DATA_FILE.write_text(js, encoding="utf-8")
    print(f"  Saved → {LOWKM_DATA_FILE}  ({len(data['snapshots'])} total snapshot(s))")


def build_and_save_lowkm(all_vehicles: list[dict], ensure_seats, pages_scraped: int) -> None:
    """Build the Low-KM (Newest) dataset from the already-scraped vehicles and
    write stepwgn_lowkm_data.js. Reuses `ensure_seats` (a URL→seat-count helper
    that caches, so seat counts fetched for the main view are not re-fetched)."""
    current_year = date.today().year

    # ── Cheap hard filters (no extra HTTP) — a car appears only if it has a
    #    multi-view camera AND an installed screen AND a clean record. (7-seat is
    #    verified later.) 未使用車 (registered-unused) cars ARE included — they're
    #    just flagged with an "未使用" badge — since they're screen-equipped low-km
    #    stock the buyer still wants to see. ──
    def _low_km(v: dict) -> bool:
        km = v.get("mileage_km")
        return km is not None and km <= LOWKM_MAX_KM

    pool = [
        v for v in all_vehicles
        if "マルチビュー" in (v.get("options") or [])
        and v.get("navi") is True          # must have an installed screen/nav
        and v.get("accident") is False     # confirmed clean record
        and _low_km(v)                     # HARD cap: ≤10,000 km
    ]
    print(f"\n── Best-Value view ── {len(pool)} camera+screen+clean+≤{LOWKM_MAX_KM//1000}k-km candidate(s)")
    if not pool:
        print("  [!] No qualifying StepWGN found — writing empty low-km snapshot")

    # ── "Screen unverified" pool — same requirements EXCEPT the screen could not
    #    be confirmed from the listing (navi is None). Screen detection is
    #    imperfect, so rather than silently dropping these we surface them in a
    #    dedicated section: they'd rank high IF they have a screen — verify first.
    uncertain_pool = [
        v for v in all_vehicles
        if "マルチビュー" in (v.get("options") or [])
        and v.get("navi") is None           # screen status unknown
        and v.get("accident") is False
        and _low_km(v)                      # HARD cap: ≤10,000 km
    ]
    print(f"   + {len(uncertain_pool)} screen-unverified candidate(s)")

    # ── Per-grade price stats (over the camera+clean pool) ────────────────────
    by_grade_prices: dict[str, list[float]] = {gid: [] for gid in GRADE_ID_TO_LABEL}
    for v in pool:
        by_grade_prices[v["grade_id"]].append(v["price_man"])

    # Price scored across ALL grades POOLED (absolute cheapness). Price dominates
    # this view, so anchor the top at the 90th percentile (not 75th) — the gradient
    # then spans nearly the whole range so cheapest-first ordering is clean and the
    # pricey cars still land near 0 (a couple of ultra-pricey outliers are clamped).
    all_prices = [p for prices in by_grade_prices.values() for p in prices]
    if all_prices:
        _lo = min(all_prices)
        _hi = _percentile(sorted(all_prices), 0.90)
        if _hi <= _lo:
            _hi = _lo * 1.10
        price_bounds = {gid: (_lo, _hi) for gid in by_grade_prices}
    else:
        price_bounds = {}

    for v in pool + uncertain_pool:
        v["lowkm_score"], v["lowkm_breakdown"] = score_vehicle_lowkm(v, price_bounds, current_year)

    # Ranked by the blended score: price is the biggest single factor (cheaper =
    # better) but low km / newer year / bigger screen still count, so a slightly
    # pricier but clearly better car can edge ahead. Every car here is already
    # ≤10k km, clean, camera+screen+7-seat — the gate does the heavy lifting.
    ranked = sorted(pool, key=lambda x: -x["lowkm_score"])

    # ── Third hard requirement — 7-seat layout, verified on the detail page ───
    print(f"Verifying 7-seat layout for up to {SEAT_CHECK_CAP} candidate(s) (target {TOP_N}) …")
    top_vehicles: list[dict] = []
    for v in ranked[:SEAT_CHECK_CAP]:
        if len(top_vehicles) >= TOP_N:
            break
        if ensure_seats(v) == 7:
            top_vehicles.append(v)
            km = f"{v['mileage_km']:,}km" if v.get("mileage_km") is not None else "?km"
            print(f"  ✓ #{len(top_vehicles):2d}  [{v['lowkm_score']}]  "
                  f"{v.get('year','?')}  {km}  ¥{v['price_man']}万  {v['grade_id']}")

    # ── Category gems (each strictly 7-seat, deduped across categories) ────────
    GEM_TARGET = 3
    top_urls = {v.get("url") for v in top_vehicles}
    non_top  = [v for v in ranked if v.get("url") not in top_urls]
    used_gem_urls: set[str] = set()

    def _finalize(cands: list[dict], seen: set[str] | None = None) -> list[dict]:
        """Take up to GEM_TARGET strictly-7-seat cars, skipping any URL already in
        `seen` (defaults to the shared cross-lane dedup set)."""
        if seen is None:
            seen = used_gem_urls
        out: list[dict] = []
        for v in cands:
            url = v.get("url")
            if url and url in seen:
                continue
            if ensure_seats(v) != 7:
                continue
            out.append(v)
            if url:
                seen.add(url)
            if len(out) >= GEM_TARGET:
                break
        return out

    BIG = 10 ** 12
    category_gems = {
        # Alternate cuts the cheapest-first main list buries. Freshest registration
        # year (tie-break: fewer km) — the newest cars that cost too much for the top.
        "newest":     _finalize(sorted(
            non_top, key=lambda x: (-(x.get("year") or 0), x.get("mileage_km") if x.get("mileage_km") is not None else BIG))),
        # Absolute lowest odometer among the pricier remainder
        "lowest_km":  _finalize(sorted(
            non_top, key=lambda x: x.get("mileage_km") if x.get("mileage_km") is not None else BIG)),
        # Screen unverified — would rank high, but the screen isn't confirmed.
        # Ranked by the same score; still strictly 7-seat. Its own dedup set (a
        # different pool, so it never collides with the confirmed lanes).
        "screen_unknown": _finalize(
            sorted(uncertain_pool, key=lambda x: -x["lowkm_score"]), seen=set()),
    }

    # ── Serialize: _clean_vehicle reads v["score"]/["score_breakdown"], so point
    #    those at the low-km values (the main dataset is already saved by now). ──
    for v in pool + uncertain_pool:
        v["score"]           = v["lowkm_score"]
        v["score_breakdown"] = v["lowkm_breakdown"]

    snapshot = build_snapshot(by_grade_prices, pages_scraped, top_vehicles, category_gems)
    data     = load_existing_lowkm()
    canonical = _default_structure_lowkm()
    data["vehicle"] = canonical["vehicle"]
    data["grades"]  = canonical["grades"]
    data["weights"] = LOWKM_WEIGHTS

    today = str(date.today())
    data["snapshots"] = [s for s in data["snapshots"] if s["date"] != today]
    data["snapshots"].append(snapshot)
    data["snapshots"].sort(key=lambda s: s["date"])
    save_lowkm(data)

    print("\nLow-KM category gems:")
    for cat, gems in category_gems.items():
        print(f"  {cat}:")
        for g in gems:
            km = f"{g['mileage_km']:,}km" if g.get("mileage_km") is not None else "km=?"
            print(f"    [{g['score']}] {g.get('year','?')}  {km}  ¥{g['price_man']}万  {g.get('url','')}")


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

    # ── Deduplicate by detail URL ─────────────────────────────────────────
    # CarSensor pins/repeats some listings across pages, so the same car (same
    # detail URL) can be scraped several times. Collapse to the first occurrence
    # so it isn't counted or ranked twice (URL-less listings are all kept).
    _seen_urls: set[str] = set()
    _deduped: list[dict] = []
    for v in all_vehicles:
        u = v.get("url")
        if u and u in _seen_urls:
            continue
        if u:
            _seen_urls.add(u)
        _deduped.append(v)
    dropped = len(all_vehicles) - len(_deduped)
    if dropped:
        print(f"  Deduplicated {dropped} repeated listing(s) → {len(_deduped)} unique")
    all_vehicles = _deduped

    # ── Per-grade stats ───────────────────────────────────────────────────
    target_grade_ids = set(GRADE_ID_TO_LABEL.keys())
    by_grade_prices: dict[str, list[float]] = {gid: [] for gid in target_grade_ids}
    for v in all_vehicles:
        by_grade_prices[v["grade_id"]].append(v["price_man"])

    # ── Price bounds for the relative (tight) price score ──────────────────
    # StepWGN's four grades sit in a similar price band, so price is scored
    # across ALL grades POOLED (absolute cheapness) — the cheapest StepWGN
    # overall scores 10, regardless of grade. (Freed scores per-grade.)
    all_prices = [p for prices in by_grade_prices.values() for p in prices]
    if all_prices:
        _lo = min(all_prices)
        _hi = _percentile(sorted(all_prices), 0.75)
        if _hi <= _lo:
            _hi = _lo * 1.10
        price_bounds = {gid: (_lo, _hi) for gid in by_grade_prices}
    else:
        price_bounds = {}

    # ── Score every vehicle ───────────────────────────────────────────────
    for v in all_vehicles:
        v["score"], v["score_breakdown"] = score_vehicle(v, price_bounds)

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

    # Seat-count fetcher shared by the main pass, the gem pass, and the Low-KM
    # view — caches by URL so no detail page is fetched twice across all three.
    _seat_cache: dict[str, int | None] = {}

    def ensure_seats(v: dict) -> int | None:
        if v.get("seats") is not None:
            return v["seats"]
        url = v.get("url")
        if not url:
            return None
        if url in _seat_cache:
            v["seats"] = _seat_cache[url]
            return v["seats"]
        seats = _fetch_seat_count(url)
        _seat_cache[url] = seats
        v["seats"] = seats
        time.sleep(DELAY_SEC)
        return seats

    print(f"\nChecking seating capacity for top {len(candidates)} candidates "
          f"(targeting {TOP_N} with 7 seats) …")
    for v in candidates:
        if len(top_vehicles) >= TOP_N:
            break
        url = v.get("url")
        if not url:
            top_vehicles.append(v)   # no URL → can't check, include anyway
            continue
        seats = ensure_seats(v)      # store for display (no scoring impact)
        if seats == 8:
            print(f"  ✗ skipped  8-seat  ¥{v['price_man']}万  {v['grade_id']}  {url}")
        else:
            top_vehicles.append(v)
            seat_label = f"{seats}名" if seats else "?名"
            print(f"  ✓ #{len(top_vehicles):2d}  [{v['score']}]  {seat_label}  "
                  f"¥{v['price_man']}万  {v['grade_id']}")

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
            seats = ensure_seats(v)
            print(f"  {u}  → {seats}名" if seats else f"  {u}  → ?")

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
    # Expose scoring config so the dashboard can show how each total is formed.
    data["weights"]            = WEIGHTS
    data["grade_value_bonus"]  = GRADE_VALUE_BONUS

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

    # ── Low-KM (Newest) view ──────────────────────────────────────────────
    # Built from the SAME scrape (the main dataset is already saved above), so
    # any failure here must never abort the successful main run.
    try:
        build_and_save_lowkm(all_vehicles, ensure_seats, limit)
    except Exception as e:
        print(f"  [!] Low-KM dataset build failed: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pages", type=int, default=None,
        help="Max pages to scrape (default: all)"
    )
    args = parser.parse_args()
    run(args.pages)
