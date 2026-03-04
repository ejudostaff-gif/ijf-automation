import os
import re
import time
from typing import Optional, Tuple
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


# ==============================
# 環境変数
# ==============================

SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
SHEET_NAME = os.getenv("SHEET_NAME")
START_ROW = int(os.getenv("START_ROW", "2"))
END_ROW = int(os.getenv("END_ROW", "1000"))

SEARCH_COL = os.getenv("SEARCH_COL", "F")
OUTPUT_COL = os.getenv("OUTPUT_COL", "Q")          # IJF
JUDOINSIDE_COL = os.getenv("JUDOINSIDE_COL", "P")  # JudoInside
BIRTH_COL = os.getenv("BIRTH_COL", "M")

ENABLE_JUDOINSIDE = os.getenv("ENABLE_JUDOINSIDE", "0") == "1"


# ==============================
# HTTP 共通（ブロック回避寄り）
# ==============================

SESSION = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


# ==============================
# Google 接続
# ==============================

def open_worksheet():
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL が空です（GitHub Actions の env に設定してください）")
    if not SHEET_NAME:
        raise RuntimeError("SHEET_NAME が空です（GitHub Actions の env に設定してください）")

    creds = Credentials.from_service_account_file(
        "credentials.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ],
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_url(SPREADSHEET_URL)
    ws = ss.worksheet(SHEET_NAME)
    return ws


# ==============================
# 名前整形
# ==============================

def normalize_name(name: str) -> str:
    name = name.replace(",", " ")
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def swap_name(name: str) -> str:
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            return f"{parts[1]} {parts[0]}".strip()
    return name


def tokens_for_match(s: str) -> list:
    """
    一致判定用トークン化。
    記号を落として、英数字の単語に分割。
    """
    s = normalize_name(s).lower()
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in re.split(r"\s+", s) if t]
    return toks


# ==============================
# IJF検索（誤マッチ削減版）
# ==============================

def search_ijf(name: str) -> Optional[str]:
    """
    以前の実装:
      soup.find("a", href=/judoka/\d+/) の最初のリンクを拾う
    → これはページのヘッダ/フッタ等の無関係リンクを拾うため誤マッチが出る。

    改善:
      1) /judoka?q=... にアクセスして、最終URL(r.url) が /judoka/<id> なら候補
      2) そのページ本文に、クエリ名のトークンが十分含まれるか検証（雑でも強い）
      3) 条件を満たすときだけURLを返す
    """
    base = "https://www.ijf.org/judoka"
    url = f"{base}?{urlencode({'q': name})}"

    try:
        r = SESSION.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code != 200:
            return None

        final_url = (r.url or "").split("#")[0]

        # 最終URLが /judoka/<数字> 系でないなら、そもそもプロフィールに辿り着けていない
        if not re.search(r"^https://www\.ijf\.org/judoka/\d+(?:/.*)?$", final_url):
            return None

        soup = BeautifulSoup(r.text, "html.parser")
        page_text = soup.get_text(" ", strip=True).lower()

        q_tokens = tokens_for_match(name)
        if not q_tokens:
            return None

        # 2語以上なら最低2トークン一致を要求（姓+名を想定）
        hit = sum(1 for t in q_tokens if t in page_text)

        if len(q_tokens) >= 2 and hit < 2:
            return None
        if len(q_tokens) == 1 and hit < 1:
            return None

        return final_url

    except Exception:
        return None


# ==============================
# JudoInside検索（現状維持）
# ==============================

def parse_birth_any(text: str) -> Optional[str]:
    """
    JudoInside は表記揺れがあるので、いくつかの形式を拾う。
    返す形式は yyyy/m/d（ゼロ埋めなし）。
    """
    # 例: 2002-01-17
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y}/{mo}/{d}"

    # 例: 17 Jan 2002 / 17 January 2002
    m = re.search(r"\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})\b", text)
    if m:
        d = int(m.group(1))
        mon = m.group(2).lower()
        y = int(m.group(3))
        months = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "december": 12,
        }
        if mon in months:
            mo = months[mon]
            return f"{y}/{mo}/{d}"

    return None


def search_judoinside(name: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    戻り値: (profile_url, birth_yyyy/m/d, status_text)
    status_text はログ用
    """
    search_url = "https://judoinside.com/search"
    params = urlencode({"q": name})
    url = f"{search_url}?{params}"

    try:
        r = SESSION.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if r.status_code in (403, 429):
            return None, None, f"BLOCKED status={r.status_code}"
        if r.status_code != 200:
            return None, None, f"HTTP status={r.status_code}"

        soup = BeautifulSoup(r.text, "html.parser")

        # /judoka/111259/... のようなリンク
        link = soup.find("a", href=re.compile(r"/judoka/\d+/"))
        if not link:
            t = soup.get_text(" ", strip=True)[:200]
            return None, None, f"NO_LINK (page_text='{t}')"

        profile_url = "https://judoinside.com" + link["href"]

        r2 = SESSION.get(profile_url, headers=HEADERS, timeout=15, allow_redirects=True)
        if r2.status_code in (403, 429):
            return profile_url, None, f"PROFILE_BLOCKED status={r2.status_code}"
        if r2.status_code != 200:
            return profile_url, None, f"PROFILE_HTTP status={r2.status_code}"

        soup2 = BeautifulSoup(r2.text, "html.parser")
        text2 = soup2.get_text(" ", strip=True)

        birth = parse_birth_any(text2)
        return profile_url, birth, "OK"

    except Exception as e:
        return None, None, f"EXCEPTION {type(e).__name__}"


# ==============================
# メイン処理
# ==============================

def main():
    print("=== START main() ===", flush=True)
    print(
        f"Config: SEARCH_COL={SEARCH_COL} OUTPUT_COL={OUTPUT_COL} "
        f"JUDOINSIDE_COL={JUDOINSIDE_COL} BIRTH_COL={BIRTH_COL} ENABLE_JUDOINSIDE={ENABLE_JUDOINSIDE}",
        flush=True
    )

    ws = open_worksheet()
    print("Opened worksheet", flush=True)

    names = ws.get(f"{SEARCH_COL}{START_ROW}:{SEARCH_COL}{END_ROW}")
    q_vals = ws.get(f"{OUTPUT_COL}{START_ROW}:{OUTPUT_COL}{END_ROW}")
    p_vals = ws.get(f"{JUDOINSIDE_COL}{START_ROW}:{JUDOINSIDE_COL}{END_ROW}")
    m_vals = ws.get(f"{BIRTH_COL}{START_ROW}:{BIRTH_COL}{END_ROW}")

    total = len(names)
    checked = 0
    found_ijf = 0
    found_ji = 0
    found_birth = 0

    for i in range(total):
        row = START_ROW + i

        if i >= len(names) or (not names[i]):
            continue

        original_name = str(names[i][0]).strip()
        if not original_name:
            continue

        checked += 1

        name = normalize_name(original_name)
        swapped = normalize_name(swap_name(original_name))

        # ---- IJF ----
        already_q = (i < len(q_vals) and q_vals[i] and str(q_vals[i][0]).strip())
        if not already_q:
            print(f"Search IJF: row={row} q='{name}'", flush=True)
            ijf_url = search_ijf(name)

            if (not ijf_url) and swapped and swapped != name:
                print(f"Search IJF: row={row} q='{swapped}'", flush=True)
                ijf_url = search_ijf(swapped)

            if ijf_url:
                ws.update_acell(f"{OUTPUT_COL}{row}", ijf_url)
                found_ijf += 1
                print(f"FOUND IJF row={row}", flush=True)
            else:
                print(f"NOT FOUND IJF row={row}", flush=True)

        # ---- JudoInside + Birth ----
        if ENABLE_JUDOINSIDE:
            already_p = (i < len(p_vals) and p_vals[i] and str(p_vals[i][0]).strip())
            already_m = (i < len(m_vals) and m_vals[i] and str(m_vals[i][0]).strip())

            if (not already_p) or (not already_m):
                print(f"Search JudoInside: row={row} q='{name}'", flush=True)
                ji_url, birth, status = search_judoinside(name)

                if (not ji_url) and swapped and swapped != name:
                    print(f"Search JudoInside: row={row} q='{swapped}'", flush=True)
                    ji_url, birth, status = search_judoinside(swapped)

                print(f"JudoInside result: row={row} status={status}", flush=True)

                if ji_url and (not already_p):
                    ws.update_acell(f"{JUDOINSIDE_COL}{row}", ji_url)
                    found_ji += 1
                    print(f"WRITE P row={row}", flush=True)

                if birth and (not already_m):
                    ws.update_acell(f"{BIRTH_COL}{row}", birth)
                    found_birth += 1
                    print(f"WRITE M row={row} birth={birth}", flush=True)

        time.sleep(0.5)

    print(f"=== DONE checked={checked} IJF={found_ijf} JI={found_ji} BIRTH={found_birth} ===", flush=True)


if __name__ == "__main__":
    main()
