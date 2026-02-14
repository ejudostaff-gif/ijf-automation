# main.py
import os
import re
import time
from typing import Optional, List, Tuple, Dict
from urllib.parse import quote_plus

import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials


# =========================
# Config (GitHub Actions env)
# =========================
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "").strip()
SHEET_NAME = os.environ.get("SHEET_NAME", "データベース").strip()

SEARCH_COL = os.environ.get("SEARCH_COL", "F").strip().upper()          # 検索値列
IJF_COL = os.environ.get("IJF_COL", "Q").strip().upper()               # IJF URL 出力列
JUDOINSIDE_COL = os.environ.get("JUDOINSIDE_COL", "P").strip().upper() # JudoInside URL 出力列

START_ROW = int(os.environ.get("START_ROW", "2"))
END_ROW = int(os.environ.get("END_ROW", "1000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))  # 取得の間隔（サイト負荷対策）

ENABLE_IJF = os.environ.get("ENABLE_IJF", "1").strip() != "0"
ENABLE_JUDOINSIDE = os.environ.get("ENABLE_JUDOINSIDE", "0").strip() != "0"

# 誤マッチ検知（列へ書かず、色だけ変える）
ENABLE_AUDIT_COLOR = os.environ.get("ENABLE_AUDIT_COLOR", "1").strip() != "0"
AUDIT_THRESHOLD = float(os.environ.get("AUDIT_THRESHOLD", "0.78"))  # 低いほど甘い

# NG種別ごとの背景色（RGB: 0.0〜1.0）
COLOR_FETCH = (1.0, 0.85, 0.85)     # 薄赤: 404/取得失敗
COLOR_NO_NAME = (1.0, 0.92, 0.80)   # 薄橙: ページは取れたが名前取れず
COLOR_LOW_MATCH = (1.0, 0.98, 0.75) # 薄黄: 一致度不足

IJF_BASE = "https://www.ijf.org"
IJF_JUDOKA_PREFIX = "https://www.ijf.org/judoka/"
JUDOINSIDE_BASE = "https://judoinside.com"


# =========================
# Utils
# =========================
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def col_to_index(col: str) -> int:
    """A -> 1, Z -> 26, AA -> 27"""
    col = col.upper()
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n

def a1_notation(col_letter: str, row: int) -> str:
    return f"{col_letter}{row}"

def http_get(session: requests.Session, url: str, timeout: int = 25) -> Optional[requests.Response]:
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        return r
    except Exception:
        return None

def build_queries_from_name(raw: str) -> List[str]:
    """
    ルール:
      ・F列の ',' を削除して検索する（例：SHAHEEN, Nigara -> SHAHEEN Nigara）
    追加で命中率のため、カンマがある場合は姓名入替版も試す（ただし列への出力は一つのみ）
    """
    raw = normalize_spaces(raw)
    if not raw:
        return []

    # カンマ削除
    no_comma = normalize_spaces(raw.replace(",", " "))
    qs = [no_comma]

    # 追加: "LAST, First Middle" の場合は "First Middle LAST" も試す（任意だが命中率↑）
    if "," in raw:
        parts = [normalize_spaces(p) for p in raw.split(",")]
        if len(parts) >= 2 and parts[0] and parts[1]:
            swapped = normalize_spaces(parts[1] + " " + parts[0])
            if swapped and swapped not in qs:
                qs.append(swapped)

    # 重複除去
    out = []
    seen = set()
    for q in qs:
        if q and q not in seen:
            out.append(q)
            seen.add(q)
    return out

def token_set(s: str) -> set:
    s = normalize_spaces(s).lower()
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    s = normalize_spaces(s)
    if not s:
        return set()
    return set(s.split(" "))

def name_match_score(query: str, page_name: str) -> float:
    """
    軽量一致度:
      ・トークン集合のJaccard（簡易）
    """
    a = token_set(query)
    b = token_set(page_name)
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


# =========================
# Google Sheets helpers
# =========================
def open_worksheet(gc: gspread.Client) -> gspread.Worksheet:
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL が空です（GitHub Actions の env に設定してください）")
    ss = gc.open_by_url(SPREADSHEET_URL)
    return ss.worksheet(SHEET_NAME)

def get_column_values_padded(ws: gspread.Worksheet, col: str, start_row: int, end_row: int) -> List[List[str]]:
    rng = f"{col}{start_row}:{col}{end_row}"
    vals = ws.get(rng)
    need = (end_row - start_row + 1)
    if len(vals) < need:
        vals.extend([[] for _ in range(need - len(vals))])
    return vals

def update_cells_in_col(ws: gspread.Worksheet, col: str, updates: List[Tuple[int, str]]) -> None:
    """
    updates: [(row, value), ...]
    """
    if not updates:
        return
    updates = sorted(updates, key=lambda x: x[0])
    data = []
    for row, value in updates:
        a1 = f"{col}{row}"
        data.append({"range": f"{ws.title}!{a1}", "values": [[value]]})
    ws.spreadsheet.values_batch_update(
        body={"valueInputOption": "RAW", "data": data}
    )
    print(f"Updated {len(updates)} cells in {col}", flush=True)

def set_bg_colors_batch(ws: gspread.Worksheet, a1_list: List[str], rgb: Tuple[float, float, float]) -> None:
    """
    a1_list: ["Q10", "Q25", ...]
    rgb: 0.0〜1.0
    """
    if not a1_list:
        return

    sheet_id = ws._properties["sheetId"]
    requests_payload = []

    for a1 in a1_list:
        m = re.match(r"^([A-Z]+)(\d+)$", a1)
        if not m:
            continue
        col = m.group(1)
        row = int(m.group(2))
        r0 = row - 1
        c0 = col_to_index(col) - 1

        requests_payload.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": r0,
                    "endRowIndex": r0 + 1,
                    "startColumnIndex": c0,
                    "endColumnIndex": c0 + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": rgb[0], "green": rgb[1], "blue": rgb[2]}
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    if requests_payload:
        ws.spreadsheet.batch_update({"requests": requests_payload})
        print(f"Colored {len(requests_payload)} cells", flush=True)


# =========================
# IJF search / parse
# =========================
def ijf_search_judoka_url(session: requests.Session, query: str) -> Optional[str]:
    """
    IJFサイト内検索（HTML）から /judoka/<id> を拾う。
    ※IJF側の検索ページ仕様が変わると要調整。
    """
    q = normalize_spaces(query)
    if not q:
        return None

    # よくある形式: https://www.ijf.org/search?query=...
    # （動かない場合は、ここだけログ見ながら差し替えればOK）
    url = f"{IJF_BASE}/search?query={quote_plus(q)}"
    r = http_get(session, url)
    if not r or r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if "/judoka/" in href:
            if href.startswith("http"):
                u = href
            else:
                u = IJF_BASE + href
            # 正規化
            m = re.search(r"(https?://www\.ijf\.org/judoka/\d+)", u)
            if m:
                return m.group(1)
    return None

def ijf_extract_profile_name(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    if h1:
        t = normalize_spaces(h1.get_text(" ", strip=True))
        if t and len(t) >= 3:
            return t
    if soup.title and soup.title.string:
        t = normalize_spaces(soup.title.string)
        t = re.sub(r"\s*[\-|/]\s*IJF.*$", "", t, flags=re.IGNORECASE).strip()
        if t and len(t) >= 3:
            return t
    return None


# =========================
# JudoInside search / parse (optional)
# =========================
def judoinside_search_url(session: requests.Session, query: str) -> Optional[str]:
    """
    JudoInside 検索（簡易）
    """
    q = normalize_spaces(query)
    if not q:
        return None
    # 検索ページ例: https://judoinside.com/search?query=...
    url = f"{JUDOINSIDE_BASE}/search?query={quote_plus(q)}"
    r = http_get(session, url)
    if not r or r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if href.startswith("/judoka/"):
            return JUDOINSIDE_BASE + href
        if href.startswith(JUDOINSIDE_BASE + "/judoka/"):
            return href
    return None

def judoinside_extract_profile_name(soup: BeautifulSoup) -> Optional[str]:
    # ページ構造が変わる可能性あり。まずはH1優先。
    h1 = soup.find("h1")
    if h1:
        t = normalize_spaces(h1.get_text(" ", strip=True))
        if t and len(t) >= 3:
            return t
    if soup.title and soup.title.string:
        t = normalize_spaces(soup.title.string)
        # "Assunta Scutto - JudoInside" のような後ろを削る
        t = re.sub(r"\s*-\s*JudoInside.*$", "", t, flags=re.IGNORECASE).strip()
        if t and len(t) >= 3:
            return t
    return None


# =========================
# Fetch profile display name (for audit)
# =========================
def fetch_profile_display_name(session: requests.Session, url: str) -> Tuple[Optional[str], str]:
    """
    return (display_name, status)
    status: OK / FETCH / NO_NAME
    """
    r = http_get(session, url)
    if not r or r.status_code != 200:
        return None, "FETCH"

    soup = BeautifulSoup(r.text, "html.parser")
    if url.startswith(IJF_JUDOKA_PREFIX):
        name = ijf_extract_profile_name(soup)
    elif url.startswith(JUDOINSIDE_BASE + "/judoka/"):
        name = judoinside_extract_profile_name(soup)
    else:
        name = None

    if not name:
        return None, "NO_NAME"
    return name, "OK"


# =========================
# Main
# =========================
def main() -> None:
    print("=== START main() ===", flush=True)
    print(f"Config: SEARCH_COL={SEARCH_COL} IJF_COL={IJF_COL} JUDOINSIDE_COL={JUDOINSIDE_COL} START_ROW={START_ROW} END_ROW={END_ROW}", flush=True)

    # ---- Credentials
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    print(f"Credential path: {cred_path}", flush=True)
    print(f"Credential exists: {os.path.exists(cred_path)}", flush=True)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    print("Authorized Google client", flush=True)

    ws = open_worksheet(gc)
    print(f"Opened worksheet: {ws.title}", flush=True)

    # ---- Load columns
    start = START_ROW
    end = END_ROW

    names_vals = get_column_values_padded(ws, SEARCH_COL, start, end)
    ijf_vals = get_column_values_padded(ws, IJF_COL, start, end)
    ji_vals = get_column_values_padded(ws, JUDOINSIDE_COL, start, end)

    # ---- Targets
    targets: List[int] = []
    for i in range(0, end - start + 1):
        row = start + i
        raw = (names_vals[i][0] if names_vals[i] else "").strip()
        if not raw or raw.lower() == "name":
            continue

        ijf_cell = (ijf_vals[i][0] if ijf_vals[i] else "").strip()
        ji_cell = (ji_vals[i][0] if ji_vals[i] else "").strip()

        need_fill = (ENABLE_IJF and not ijf_cell) or (ENABLE_JUDOINSIDE and not ji_cell)
        need_audit = ENABLE_AUDIT_COLOR and ((ijf_cell != "") or (ji_cell != ""))  # URLがあるなら監査可能

        if need_fill or need_audit:
            targets.append(i)

    print(f"Targets found: {len(targets)}", flush=True)

    # ---- HTTP session
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; ijf-automation/1.0; +https://github.com/)",
        "Accept-Language": "en-US,en;q=0.9",
    })

    # ---- Caches / buffers
    profile_name_cache: Dict[str, Tuple[Optional[str], str]] = {}

    ijf_updates: List[Tuple[int, str]] = []
    ji_updates: List[Tuple[int, str]] = []

    color_updates_fetch: List[str] = []
    color_updates_noname: List[str] = []
    color_updates_lowmatch: List[str] = []

    checked = 0
    found = 0

    for idx in targets:
        row = start + idx
        raw_name = (names_vals[idx][0] if names_vals[idx] else "").strip()
        ijf_cell = (ijf_vals[idx][0] if ijf_vals[idx] else "").strip()
        ji_cell = (ji_vals[idx][0] if ji_vals[idx] else "").strip()

        qs = build_queries_from_name(raw_name)
        if not qs:
            continue

        # =======================
        # 1) IJF fill
        # =======================
        if ENABLE_IJF and not ijf_cell:
            got = None
            for q in qs:
                print(f"Search IJF: row={row} q='{q}'", flush=True)
                got = ijf_search_judoka_url(session, q)
                time.sleep(SLEEP_SEC)
                if got:
                    break

            if got:
                ijf_updates.append((row, got))
                found += 1
                ijf_cell = got
                print(f"FOUND: row={row} -> {got}", flush=True)
            else:
                print(f"NOT FOUND: row={row} name='{raw_name}'", flush=True)

        # =======================
        # 2) JudoInside fill (optional)
        # =======================
        if ENABLE_JUDOINSIDE and not ji_cell:
            got = None
            for q in qs:
                print(f"Search JudoInside: row={row} q='{q}'", flush=True)
                got = judoinside_search_url(session, q)
                time.sleep(SLEEP_SEC)
                if got:
                    break

            if got:
                ji_updates.append((row, got))
                ji_cell = got
                print(f"FOUND JudoInside: row={row} -> {got}", flush=True)
            else:
                print(f"NOT FOUND JudoInside: row={row} name='{raw_name}'", flush=True)

        # =======================
        # 3) AUDIT (color only)
        #   - 1) URLセル（P/Q）だけ色付け
        #   - OKは何もしない / NGだけ色変更
        # =======================
        if ENABLE_AUDIT_COLOR:
            audit_target_col = None
            best_url = None

            # IJF優先（Q列）・なければJudoInside（P列）
            if ijf_cell.startswith(IJF_JUDOKA_PREFIX):
                best_url = ijf_cell
                audit_target_col = IJF_COL
            elif ji_cell.startswith(JUDOINSIDE_BASE + "/judoka/"):
                best_url = ji_cell
                audit_target_col = JUDOINSIDE_COL

            if best_url and audit_target_col:
                if best_url in profile_name_cache:
                    disp_name, st = profile_name_cache[best_url]
                else:
                    disp_name, st = fetch_profile_display_name(session, best_url)
                    profile_name_cache[best_url] = (disp_name, st)
                    time.sleep(SLEEP_SEC)

                a1 = a1_notation(audit_target_col, row)

                if st == "FETCH":
                    color_updates_fetch.append(a1)
                    print(f"AUDIT NG_FETCH: row={row} cell={a1} url={best_url}", flush=True)
                elif st == "NO_NAME" or not disp_name:
                    color_updates_noname.append(a1)
                    print(f"AUDIT NG_NO_NAME: row={row} cell={a1} url={best_url}", flush=True)
                else:
                    mx = max((name_match_score(q, disp_name) for q in qs), default=0.0)
                    if mx < AUDIT_THRESHOLD:
                        color_updates_lowmatch.append(a1)
                        print(f"AUDIT NG_LOW_MATCH: row={row} cell={a1} score={mx:.2f} F='{raw_name}' page='{disp_name}'", flush=True)
                    # OKは色変更しない

        checked += 1

        # =======================
        # Flush (batch)
        # =======================
        total_pending = len(ijf_updates) + len(ji_updates) + len(color_updates_fetch) + len(color_updates_noname) + len(color_updates_lowmatch)
        if total_pending >= BATCH_SIZE:
            if ijf_updates:
                update_cells_in_col(ws, IJF_COL, ijf_updates)
                ijf_updates = []
            if ji_updates:
                update_cells_in_col(ws, JUDOINSIDE_COL, ji_updates)
                ji_updates = []

            # NGだけ色付け（種類別）
            if ENABLE_AUDIT_COLOR:
                if color_updates_fetch:
                    set_bg_colors_batch(ws, color_updates_fetch, COLOR_FETCH)
                    color_updates_fetch = []
                if color_updates_noname:
                    set_bg_colors_batch(ws, color_updates_noname, COLOR_NO_NAME)
                    color_updates_noname = []
                if color_updates_lowmatch:
                    set_bg_colors_batch(ws, color_updates_lowmatch, COLOR_LOW_MATCH)
                    color_updates_lowmatch = []

            print(f"Progress: checked={checked} found={found}", flush=True)

    # Final flush
    if ijf_updates:
        update_cells_in_col(ws, IJF_COL, ijf_updates)
    if ji_updates:
        update_cells_in_col(ws, JUDOINSIDE_COL, ji_updates)

    if ENABLE_AUDIT_COLOR:
        if color_updates_fetch:
            set_bg_colors_batch(ws, color_updates_fetch, COLOR_FETCH)
        if color_updates_noname:
            set_bg_colors_batch(ws, color_updates_noname, COLOR_NO_NAME)
        if color_updates_lowmatch:
            set_bg_colors_batch(ws, color_updates_lowmatch, COLOR_LOW_MATCH)

    print(f"=== DONE checked={checked} found={found} ===", flush=True)


if __name__ == "__main__":
    main()
