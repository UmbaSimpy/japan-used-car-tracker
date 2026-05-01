"""
CarSensor Honda Freed 3rd-gen price scraper.
Scrapes the hybrid listing (spH), walks ALL pages, filters to the four
target grades, scores each vehicle, and writes a snapshot to data/freed_data.js.

Requirements:
    pip install requests beautifulsoup4

Usage:
    python scraper.py              # scrape all pages (default)
    python scraper.py --pages 10   # limit pages (faster, for testing)
"""

import argparse
import json
import re
import sys
import time
from datetime import date
from pathlib import Path

# Force UTF-8 output so Japanese characters print correctly on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import requests
from bs4 import BeautifulSoup

# ── Config ───────────────────────────────────────────────────────────────────

BASE_URL             = "https://www.carsensor.net/usedcar/bHO/s083/spH/"
DATA_FILE            = Path(__file__).parent / "data" / "freed_data.js"
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
TOP_N           = 15   # number of top-scored vehicles to store per snapshot
SEAT_CHECK_CAP  = 60   # max detail pages to fetch when filtering 5-seat cars

# Longest names first so substring matching can't misfire
TARGET_GRADES = {
    "1.5 e:HEV エアー EX 4WD":  "ehev_air_ex_4wd",
    "1.5 e:HEV エアー EX":      "ehev_air_ex",
    "1.5 e:HEV クロスター 4WD": "ehev_crosstar_4wd",
    "1.5 e:HEV クロスター":     "ehev_crosstar",
}

GRADE_ID_TO_LABEL = {v: k for k, v in TARGET_GRADES.items()}

# Scoring weights (must sum to 1.0)
WEIGHTS = {
    "price":       0.33,   # up from 0.28 — price spread matters more
    "mileage":     0.19,   # down from 0.24 — 0–3k km flat zone reduces its impact
    "shaken":      0.13,
    "accident":    0.13,
    "warranty":    0.09,
    "maintenance": 0.05,
    "navi":        0.05,   # small bonus for OEM nav presence
    "camera":      0.03,   # slight bonus for マルチビューカメラ
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
        results.append({"grade_id": grade_id, "price_man": price, **details})

    if not containers:
        print("  [!] No listing containers found — selectors may need updating")

    return results


def _match_grade(text: str) -> str | None:
    for jp, gid in sorted(TARGET_GRADES.items(), key=lambda x: -len(x[0])):
        if jp in text:
            return gid
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
        "color":         None,   # body color string from listing card
    }

    # Detail page URL
    link = item.select_one('a[name="detail_a"]')
    if link and link.get("href"):
        href = link["href"].split("?")[0]
        details["url"] = "https://www.carsensor.net" + href

    # Spec boxes: 走行距離 / 車検 / 修復歴 / 保証 / 整備
    for box in item.select("div.specList__detailBox"):
        text = box.get_text(" ", strip=True)

        if "走行距離" in text:
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

        elif "ボディカラー" in text or "カラー" in text:
            color_val = re.sub(r'^(?:ボディカラー|カラー)\s*[:：\s]*', '', text).strip()
            if color_val and len(color_val) <= 40:
                details["color"] = color_val

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

    # Color: regex fallback in case it wasn't in a spec box
    if not details["color"]:
        m_color = re.search(
            r'(?:ボディカラー|カラー)\s*[:：]?\s*([゠-ヿぁ-ゞ一-龥][^\n\r\t]{1,38}?)(?:\s{2,}|\n|　|$)',
            full_text + '  ',
        )
        if m_color:
            color_val = m_color.group(1).strip()
            if 2 <= len(color_val) <= 40:
                details["color"] = color_val

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

def score_vehicle(vehicle: dict, global_stats: dict) -> tuple[float, dict]:
    """
    Score a vehicle 0–10 using price (35%), mileage (30%),
    shaken remaining (20%), accident history (15%).
    All criteria use the same scale regardless of grade.
    Price is normalized against the global min/max across ALL grades.
    """
    # Price: normalized globally so every vehicle is judged on the same scale
    g_min = global_stats["min"]
    g_max = global_stats["max"]
    price_range = max(g_max - g_min, 1.0)
    price_score = max(0.0, min(10.0, 10.0 * (g_max - vehicle["price_man"]) / price_range))

    # Mileage: 0–3,000 km all score 10 (flat max zone — brand-new vs demo-car irrelevant).
    # Above 3,000 km the score decays linearly: 10 at 3,000 km → 0 at 103,000 km.
    km = vehicle.get("mileage_km")
    if km is None:
        mileage_score = 5.0
    else:
        effective_km = max(0, km - 3_000)
        mileage_score = max(0.0, min(10.0, 10.0 - (effective_km / 10_000.0)))

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

    breakdown = {
        "price":       round(price_score,       1),
        "mileage":     round(mileage_score,     1),
        "shaken":      round(shaken_score,      1),
        "accident":    round(accident_score,    1),
        "warranty":    round(warranty_score,    1),
        "maintenance": round(maintenance_score, 1),
        "navi":        round(navi_score,        1),
        "camera":      round(camera_score,      1),
    }
    return round(total, 1), breakdown


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
    """Serialize a scored vehicle dict for storage in freed_data.js."""
    return {
        "score":           v["score"],
        "score_breakdown": v["score_breakdown"],
        "grade_id":        v["grade_id"],
        "grade_label":     GRADE_ID_TO_LABEL.get(v["grade_id"], v["grade_id"]),
        "price_man":       v["price_man"],
        "mileage_km":      v.get("mileage_km"),
        "shaken_months":   v.get("shaken_months"),
        "accident":        v.get("accident"),
        "warranty":        v.get("warranty"),
        "maintenance":     v.get("maintenance"),
        "navi":            v.get("navi"),
        "camera":          v.get("camera"),
        "color":           v.get("color"),
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


def build_telegram_message(snapshot: dict, prev_snapshot: dict | None) -> str:
    today = snapshot["date"]
    lines = [f"<b>🚗 Honda Freed Scan — {today}</b>"]

    grade_labels = {
        "ehev_air_ex":       "e:HEV エアー EX (2WD)",
        "ehev_air_ex_4wd":   "e:HEV エアー EX 4WD",
        "ehev_crosstar":     "e:HEV クロスター (2WD)",
        "ehev_crosstar_4wd": "e:HEV クロスター 4WD",
    }

    for gid, label in grade_labels.items():
        cur = snapshot["by_grade"].get(gid)
        if not cur:
            continue
        pre = prev_snapshot["by_grade"].get(gid) if prev_snapshot else None

        lines.append("")
        lines.append(f"<b>{label}</b>")

        count = cur["count"]
        if pre:
            d_count = count - pre["count"]
            arrow = "▲" if d_count > 0 else ("▼" if d_count < 0 else "→")
            sign  = "+" if d_count > 0 else ""
            lines.append(f"  台数: {count:,} 台  ({arrow} {sign}{d_count:,} from last scan)")
        else:
            lines.append(f"  台数: {count:,} 台  (first snapshot)")

        avg = cur["avg"]
        if pre:
            d_avg = avg - pre["avg"]
            arrow = "▲" if d_avg > 0 else ("▼" if d_avg < 0 else "→")
            sign  = "+" if d_avg > 0 else ""
            lines.append(f"  Avg:  ¥{avg:,}  ({arrow} {sign}¥{d_avg:,})")
        else:
            lines.append(f"  Avg:  ¥{avg:,}")

    # Top 3 deals
    top = snapshot.get("top_vehicles", [])[:3]
    if top:
        lines.append("")
        lines.append("<b>🏆 Top 3 Deals Today</b>")
        for i, v in enumerate(top, 1):
            km    = f"{v['mileage_km']:,} km" if v.get("mileage_km") is not None else "km ?"
            shk   = f"{v['shaken_months']}mo shaken" if v.get("shaken_months") is not None else "shaken ?"
            acc   = "No accident" if v.get("accident") is False else ("Accident" if v.get("accident") else "acc ?")
            war   = "保証付" if v.get("warranty") is True else ("保証なし" if v.get("warranty") is False else "保証 ?")
            mnt   = "整備付" if v.get("maintenance") is True else ("整備無" if v.get("maintenance") is False else "整備 ?")
            navi   = "純正ナビ" if v.get("navi") is True else ("ナビレス" if v.get("navi") is False else "ナビ ?")
            cam    = "マルチビュー" if v.get("camera") is True else ""
            extras = " · ".join(x for x in [navi, cam] if x)
            lines.append(f"  #{i} [{v['score']}/10] {v['grade_label']} — ¥{v['price_man']}万")
            lines.append(f"      {km} · {shk} · {acc} · {war} · {mnt} · {extras}")
            if v.get("url"):
                lines.append(f"      {v['url']}")

    return "\n".join(lines)


# ── Data file I/O ─────────────────────────────────────────────────────────────

def load_existing() -> dict:
    if not DATA_FILE.exists():
        return _default_structure()
    src = DATA_FILE.read_text(encoding="utf-8")
    m = re.search(r"window\.FREED_DATA\s*=\s*(\{.*\});", src, re.DOTALL)
    if not m:
        print("  [!] Could not parse existing freed_data.js — starting fresh")
        return _default_structure()
    return json.loads(m.group(1))


def _default_structure() -> dict:
    return {
        "vehicle": {
            "make":          "Honda",
            "model":         "Freed",
            "carsensor_url": BASE_URL,
            "generation":    "3rd gen (2024年06月～)",
        },
        "grades": [
            {"id": "ehev_air_ex",       "label": "e:HEV エアー EX",       "label_en": "e:HEV Air EX",       "drive": "2WD"},
            {"id": "ehev_air_ex_4wd",   "label": "e:HEV エアー EX 4WD",   "label_en": "e:HEV Air EX 4WD",   "drive": "4WD"},
            {"id": "ehev_crosstar",     "label": "e:HEV クロスター",       "label_en": "e:HEV Crosstar",     "drive": "2WD"},
            {"id": "ehev_crosstar_4wd", "label": "e:HEV クロスター 4WD",  "label_en": "e:HEV Crosstar 4WD", "drive": "4WD"},
        ],
        "snapshots": [],
    }


def save(data: dict) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    js = (
        "// Auto-generated by scraper.py — do not edit manually.\n"
        "// Prices in yen (JPY). Each snapshot = one daily scrape of CarSensor.net.\n"
        f"window.FREED_DATA = {json.dumps(data, ensure_ascii=False, indent=2)};\n"
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
    by_grade_prices: dict[str, list[float]] = {gid: [] for gid in TARGET_GRADES.values()}
    for v in all_vehicles:
        by_grade_prices[v["grade_id"]].append(v["price_man"])

    # ── Global price range (shared across ALL grades for fair scoring) ────
    all_prices = [v["price_man"] for v in all_vehicles]
    global_stats = {
        "min": min(all_prices),
        "max": max(all_prices),
    } if all_prices else {"min": 0, "max": 1}

    # ── Score every vehicle using the same global scale ───────────────────
    for v in all_vehicles:
        v["score"], v["score_breakdown"] = score_vehicle(v, global_stats)

    # ── Top N across all grades (excluding 5-seat configurations) ────────
    # Seating capacity is only on the detail page, so we fetch detail pages
    # for the top SEAT_CHECK_CAP candidates and skip any with 乗車定員 = 5名.
    scored_sorted = sorted(
        [v for v in all_vehicles if "score" in v],
        key=lambda x: -x["score"],
    )
    candidates = scored_sorted[:SEAT_CHECK_CAP]
    top_vehicles: list[dict] = []

    print(f"\nChecking seating capacity for top {len(candidates)} candidates "
          f"(targeting {TOP_N} with 6+ seats) …")
    for v in candidates:
        if len(top_vehicles) >= TOP_N:
            break
        url = v.get("url")
        if not url:
            top_vehicles.append(v)   # no URL → can't check, include anyway
            continue
        seats = _fetch_seat_count(url)
        if seats == 5:
            print(f"  ✗ skipped  5-seat  ¥{v['price_man']}万  {v['grade_id']}  {url}")
        else:
            top_vehicles.append(v)
            seat_label = f"{seats}名" if seats else "?名"
            print(f"  ✓ #{len(top_vehicles):2d}  [{v['score']}]  {seat_label}  "
                  f"¥{v['price_man']}万  {v['grade_id']}")
        time.sleep(DELAY_SEC)

    # ── Category Gems ─────────────────────────────────────────────────────
    # Vehicles that excel in ONE specific parameter.
    # Each URL appears in at most ONE category (strict deduplication).
    top_urls = {v.get("url") for v in top_vehicles}
    non_top  = [v for v in scored_sorted if v.get("url") not in top_urls]

    used_gem_urls: set[str] = set()

    def _pick_gems(candidates: list[dict], n: int = 3) -> list[dict]:
        result: list[dict] = []
        for v in candidates:
            url = v.get("url")
            if url and url in used_gem_urls:
                continue
            result.append(v)
            if url:
                used_gem_urls.add(url)
            if len(result) >= n:
                break
        return result

    # Price gems: cheapest non-top cars (likely trade-offs on mileage / shaken).
    price_gems = _pick_gems(sorted(
        non_top,
        key=lambda x: (-x["score_breakdown"]["price"], -x["score"]),
    ))

    # Mileage gems: lowest km non-top cars (may be pricier or lack extras).
    mileage_gems = _pick_gems(sorted(
        non_top,
        key=lambda x: (-x["score_breakdown"]["mileage"], -x["score"]),
    ))

    # No-shaken gems: cars with no remaining shaken (shaken_months None or 0),
    # ranked by overall score. Buyer must arrange JCI — often priced lower.
    no_shaken_gems = _pick_gems(sorted(
        [v for v in scored_sorted if not v.get("shaken_months")],
        key=lambda x: -x["score"],
    ))

    category_gems = {
        "price":     price_gems,
        "mileage":   mileage_gems,
        "no_shaken": no_shaken_gems,
    }

    print("\nCategory gems:")
    for cat, gems in category_gems.items():
        print(f"  {cat}:")
        for g in gems:
            km  = f"{g['mileage_km']:,} km" if g.get("mileage_km") is not None else "km=?"
            shk = f"{g.get('shaken_months')}mo" if g.get("shaken_months") else "no shaken"
            print(f"    [{g['score']}] ¥{g['price_man']}万  {km}  {shk}  {g.get('url','')}")

    # ── Save ──────────────────────────────────────────────────────────────
    snapshot = build_snapshot(by_grade_prices, limit, top_vehicles, category_gems)
    data     = load_existing()
    today    = str(date.today())

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
    msg = build_telegram_message(snapshot, prev_snapshot)
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
