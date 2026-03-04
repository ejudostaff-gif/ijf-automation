import os
import re
import time
from typing import Optional, Tuple, List
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
START_ROW = int(os.getenv("START_ROW", "3"))
END_ROW = int(os.getenv("END_ROW", "1000"))

SEARCH_COL = os.getenv("SEARCH_COL", "F")
OUTPUT_COL = os.getenv("OUTPUT_COL", "Q")          # IJF
JUDOINSIDE_COL = os.getenv("JUDOINSIDE_COL", "P")  # JudoInside
BIRTH_COL = os.getenv("BIRTH_COL", "M")

ENABLE_JUDOINSIDE = os.getenv("ENABLE_JUDOINSIDE", "0") == "1"

GOOGLE_CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY")  # Secrets / env
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")            # Secrets / env

# レート調整（エコ＆BAN回避寄り）
SLEEP_PER_ROW = float(os.getenv("SLEEP_PER_ROW", "0.4"))
SLEEP_PER_PROFILE = float(os.getenv("SLEEP_PER_PROFILE", "0.2"))
SLEEP_PER_GOOGLE = float(os.getenv("SLEEP_PER_GOOGLE", "0.2"))


# ==============================
# HTTP 共通
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
    name = str(name or "")
    name = name.replace("’", "'").replace("–", "-").replace("—", "-")
    name = name.replace(",", " ")
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def swap_name(name: str) -> str:
    name = str(name or "")
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            return f"{parts[1]} {parts[0]}".strip()
    return name


def tokens_for_match(s: str) -> List[str]:
    """
    名寄せ用トークン。
    - 記号除去
    - 大文字小文字無視
    - 連続空白圧縮
    """
    s = normalize_name(s).lower()
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return []
    return [t for t in s.split(" ") if t]


# ==============================
# 安全にセル値を読む
# ==============================

def get_cell_2d(values_2d, i: int) -> str:
    """
    ws.get() の結果（2次元配列）から i 行目の 1列目を安全に取り出す。
    無ければ空文字。
    """
    if i < 0:
        return ""
    if i >= len(values_2d):
        return ""
    row = values_2d[i]
    if not row:
        return ""
    v = row[0] if len(row) >= 1 else ""
    return str(v or "").strip()


# ==============================
# IJF検索（誤マッチ削減版）
# ==============================

def _score_name_match(query: str, candidate: str) -> int:
    q = tokens_for_match(query)
    c = tokens_for_match(candidate)
    if not q or not c:
        return 0

    inter = set(q) & set(c)
    score = len(inter) * 10

    if q[0] in c:
        score += 2
    if q[-1] in c:
        score += 2
    return score


def _extract_candidates_from_search_html(html: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    candidates: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not re.search(r"^/judoka/\d+", href):
            continue

        text = a.get_text(" ", strip=True)
        if not text:
            continue
        if len(text) < 4:
            continue

        candidates.append(("https://www.ijf.org" + href, text))

    # URL重複除去
    uniq = {}
    for url, nm in candidates:
        if url not in uniq:
            uniq[url] = nm
    return [(u, n) for u, n in uniq.items()]


def _verify_profile_contains_name(profile_html: str, query: str) -> bool:
    soup = BeautifulSoup(profile_html, "html.parser")
    txt = soup.get_text(" ", strip=True).lower()

    q_tokens = tokens_for_match(query)
    if not q_tokens:
        return False

    hit = sum(1 for t in q_tokens if t in txt)

    # 2語以上なら 2トークン以上一致を要求（誤マッチ抑制）
    if len(q_tokens) >= 2:
        return hit >= 2
    return hit >= 1


def search_ijf(name: str) -> Optional[str]:
    """
    1) /judoka?q=... を取得
    2) 直接プロフィールに飛んでいれば本文検証して採用
    3) 検索結果一覧なら /judoka/{id} 候補を複数抽出
    4) 一致度上位をプロフィール本文で再検証して採用
    """
    base = "https://www.ijf.org/judoka"
    url = f"{base}?{urlencode({'q': name})}"

    try:
        r = SESSION.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code != 200:
            return None

        final_url = (r.url or "").split("#")[0]

        # 直接プロフィールに遷移しているケース
        if re.search(r"^https://www\.ijf\.org/judoka/\d+(?:/.*)?$", final_url):
            if _verify_profile_contains_name(r.text, name):
                return final_url
            return None

        # 一覧ページ解析
        candidates = _extract_candidates_from_search_html(r.text)
        if not candidates:
            return None

        scored = []
        for prof_url, disp_name in candidates:
            score = _score_name_match(name, disp_name)
            if score > 0:
                scored.append((score, prof_url, disp_name))

        if not scored:
            return None

        scored.sort(reverse=True, key=lambda x: x[0])

        # 上位3件だけプロフィール本文で検証（エコ）
        for score, prof_url, disp_name in scored[:3]:
            time.sleep(SLEEP_PER_PROFILE)
            r2 = SESSION.get(prof_url, headers=HEADERS, timeout=20, allow_redirects=True)
            if r2.status_code != 200:
                continue
            if _verify_profile_contains_name(r2.text, name):
                return (r2.url or prof_url).split("#")[0]

        return None

    except Exception:
        return None


# ==============================
# JudoInside（Google CSE 経由）
# ==============================

def parse_birth_any(text: str) -> Optional[str]:
    """
    表記揺れを拾う。返す形式は yyyy/m/d（ゼロ埋めなし）。
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


def google_cse_search_judoinside_profile(query: str) -> Tuple[Optional[str], str]:
    """
    Google CSE で judoinside.com の judoka ページを探す。
    戻り値: (url, status_text)
    """
    if not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_ID:
        return None, "CSE_NOT_CONFIGURED"

    api = "https://www.googleapis.com/customsearch/v1"
    # judoka のページを優先。/judo-career があればさらに良い
    q = f'site:judoinside.com judoka "{query}"'

    params = {
        "key": GOOGLE_CSE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": q,
        "num": 5,
    }

    try:
        time.sleep(SLEEP_PER_GOOGLE)
        r = SESSION.get(api, params=params, timeout=20)
        if r.status_code != 200:
            return None, f"CSE_HTTP_{r.status_code}"

        data = r.json()
        items = data.get("items") or []
        if not items:
            return None, "CSE_NO_ITEMS"

        # まず /judo-career を含むURLを優先
        for it in items:
            link = str(it.get("link") or "")
            if "judoinside.com/judoka/" in link and "/judo-career" in link:
                return link.replace("http://", "https://"), "OK"

        # 次に judoka のURL（末尾は /judo-career に寄せる）
        for it in items:
            link = str(it.get("link") or "")
            if "judoinside.com/judoka/" in link:
                link = link.replace("http://", "https://")
                link = link.split("#")[0]
                link = link.rstrip("/")
                if "/judo-career" not in link:
                    link = link + "/judo-career"
                return link, "OK"

        return None, "CSE_NO_JUDOKA_LINK"

    except Exception as e:
        return None, f"CSE_EXCEPTION_{type(e).__name__}"


def fetch_judoinside_birth(profile_url: str) -> Tuple[Optional[str], str]:
    """
    JudoInside プロフィールを取得して誕生日を抜く。
    """
    try:
        time.sleep(SLEEP_PER_PROFILE)
        r = SESSION.get(profile_url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code in (403, 429):
            return None, f"PROFILE_BLOCKED_{r.status_code}"
        if r.status_code != 200:
            return None, f"PROFILE_HTTP_{r.status_code}"

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        birth = parse_birth_any(text)
        if birth:
            return birth, "OK"
        return None, "NO_BIRTH"

    except Exception as e:
        return None, f"PROFILE_EXCEPTION_{type(e).__name__}"


def search_judoinside_via_google(name: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    戻り値: (profile_url, birth_yyyy/m/d, status_text)
    """
    url, st = google_cse_search_judoinside_profile(name)
    if not url:
        return None, None, st

    birth, st2 = fetch_judoinside_birth(url)
    if st2 == "OK":
        return url, birth, "OK"
    return url, birth, st2


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
    if ENABLE_JUDOINSIDE:
        print(
            f"Config: GOOGLE_CSE_API_KEY={'SET' if GOOGLE_CSE_API_KEY else 'EMPTY'} "
            f"GOOGLE_CSE_ID={'SET' if GOOGLE_CSE_ID else 'EMPTY'}",
            flush=True
        )

    ws = open_worksheet()
    print(f"Opened worksheet: {SHEET_NAME}", flush=True)

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

        raw_name = get_cell_2d(names, i)
        if not raw_name:
            continue

        checked += 1

        name = normalize_name(raw_name)
        swapped = normalize_name(swap_name(raw_name))

        # ---- IJF ----
        already_q = bool(get_cell_2d(q_vals, i))
        if not already_q:
            print(f"Search IJF: row={row} q='{name}'", flush=True)
            ijf_url = search_ijf(name)
            if not ijf_url and swapped != name:
                print(f"Search IJF: row={row} q='{swapped}'", flush=True)
                ijf_url = search_ijf(swapped)

            if ijf_url:
                ws.update_acell(f"{OUTPUT_COL}{row}", ijf_url)
                found_ijf += 1
                print(f"FOUND IJF row={row} -> {ijf_url}", flush=True)
            else:
                print(f"NOT FOUND IJF row={row} name='{raw_name}'", flush=True)

        # ---- JudoInside + Birth (Google CSE) ----
        if ENABLE_JUDOINSIDE:
            already_p = bool(get_cell_2d(p_vals, i))
            already_m = bool(get_cell_2d(m_vals, i))

            if (not already_p) or (not already_m):
                # Google検索はコストが出るので、必要なときだけ回す
                print(f"Search JudoInside(Google): row={row} q='{name}'", flush=True)
                ji_url, birth, status = search_judoinside_via_google(name)

                # 入替も試す（CSEの取りこぼし対策）
                if (not ji_url) and swapped != name:
                    print(f"Search JudoInside(Google): row={row} q='{swapped}'", flush=True)
                    ji_url, birth, status = search_judoinside_via_google(swapped)

                print(f"JudoInside result: row={row} status={status} url={ji_url}", flush=True)

                if ji_url and (not already_p):
                    ws.update_acell(f"{JUDOINSIDE_COL}{row}", ji_url)
                    found_ji += 1
                    print(f"WRITE P row={row}", flush=True)

                if birth and (not already_m):
                    ws.update_acell(f"{BIRTH_COL}{row}", birth)
                    found_birth += 1
                    print(f"WRITE M row={row} birth={birth}", flush=True)

        time.sleep(SLEEP_PER_ROW)

    print(f"=== DONE checked={checked} IJF={found_ijf} JI={found_ji} BIRTH={found_birth} ===", flush=True)


if __name__ == "__main__":
    main()
