import os
import time
import re
import traceback
from typing import Optional, Tuple, List

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ===== 設定 =====
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1SwjfDRfcikHrNo38CgFjwBa_oWQM5pnGynFeqikeVU4"
WORKSHEET_NAME = ""  # 対象タブが先頭でない場合はタブ名を入れる（例: "Sheet1"）

ROW_START = 1
ROW_END = 5000

COL_F = 6    # 検索値（名前）
COL_Q = 17   # IJF URL
COL_R = 18   # judobase URL

# 1回の実行で処理する最大件数（未入力行のみ）
MAX_UPDATES_PER_RUN = 120

# ブロック回避・安定性
SLEEP_PER_PERSON_SEC = 1.0
NAV_TIMEOUT_MS = 30000

# ===== 正規表現 =====
PROFILE_ID_RE = re.compile(r"/competitor/profile/(\d+)", re.I)


# ===== 文字列処理 =====
def normalize_name(raw: str) -> str:
    s = (raw or "").replace(",", " ").replace("，", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def swap_first_last(name: str) -> str:
    parts = name.split()
    if len(parts) < 2:
        return name
    return f"{parts[-1]} {parts[0]}"


def build_queries(name: str) -> List[str]:
    a = name
    b = swap_first_last(name)
    if b.lower() == a.lower():
        return [a]
    return [a, b]


def extract_profile_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = PROFILE_ID_RE.search(url)
    return m.group(1) if m else None


# ===== judobase検索（UIクリック → URL/HTMLからIDを取る方式） =====
def judobase_search_id(page, query: str) -> Optional[str]:
    print(f"Searching judobase for: {query}", flush=True)

    try:
        page.goto(
            "https://judobase.ijf.org/#/search",
            wait_until="domcontentloaded",
            timeout=NAV_TIMEOUT_MS,
        )
    except PWTimeoutError:
        print("Timeout loading judobase search page", flush=True)
        return None

    page.wait_for_timeout(1500)

    # 検索入力欄候補（UI変更に強め）
    input_box = None
    for sel in [
        "input[type='search']",
        "input[placeholder*='Search' i]",
        "input[aria-label*='Search' i]",
        "input",
    ]:
        try:
            cand = page.query_selector(sel)
            if cand:
                input_box = cand
                break
        except Exception:
            pass

    if not input_box:
        print("Search input not found", flush=True)
        return None

    # 入力→Enter
    try:
        input_box.click()
        input_box.fill(query)
        input_box.press("Enter")
    except Exception as e:
        print("Search input error:", e, flush=True)
        return None

    # 結果描画待ち
    page.wait_for_timeout(2500)

    # 1) すでに profile に遷移しているなら URL から取る
    pid = extract_profile_id_from_url(page.url)
    if pid:
        print(f"Found ID from URL: {pid}", flush=True)
        return pid

    # 2) 結果の「クリック可能っぽい」要素を広めに拾って1件目をクリック
    click_selectors = [
        "a[href*='competitor/profile']",
        "a[href*='/competitor/profile/']",
        "[role='link']",
        "tbody tr",
        "mat-row",
        "mat-list-item",
        "li",
    ]

    clicked = False
    for sel in click_selectors:
        try:
            el = page.query_selector(sel)
            if el:
                el.click()
                clicked = True
                break
        except Exception:
            pass

    if clicked:
        page.wait_for_timeout(2000)

        pid = extract_profile_id_from_url(page.url)
        if pid:
            print(f"Found ID after click (URL): {pid}", flush=True)
            return pid

        # URLに出なくてもHTMLから拾える場合がある
        try:
            html = page.content()
            m = PROFILE_ID_RE.search(html)
            if m:
                print(f"Found ID after click (HTML): {m.group(1)}", flush=True)
                return m.group(1)
        except Exception:
            pass

    # 3) 最後の手段：ページHTMLから拾う
    try:
        html = page.content()
        m = PROFILE_ID_RE.search(html)
        if m:
            print(f"Found ID from HTML: {m.group(1)}", flush=True)
            return m.group(1)
    except Exception:
        pass

    print("No ID found", flush=True)
    return None


def get_urls_from_judobase(page, raw_name: str) -> Tuple[Optional[str], Optional[str]]:
    name = normalize_name(raw_name)

    # ヘッダーっぽい行をスキップ（必要なら増やせます）
    if name.lower() in ("name",):
        return None, None

    if not name:
        return None, None

    for q in build_queries(name):
        pid = judobase_search_id(page, q)
        if pid:
            r_url = f"https://judobase.ijf.org/#/competitor/profile/{pid}"
            q_url = f"https://www.ijf.org/judoka/{pid}"
            return q_url, r_url
        page.wait_for_timeout(600)

    return None, None


# ===== Sheets =====
def open_worksheet(client) -> gspread.Worksheet:
    ss = client.open_by_url(SPREADSHEET_URL)
    if WORKSHEET_NAME:
        return ss.worksheet(WORKSHEET_NAME)
    return ss.get_worksheet(0)


def pad_list(lst: List[str], size: int) -> List[str]:
    if len(lst) < size:
        lst.extend([""] * (size - len(lst)))
    return lst


# ===== Main =====
def main():
    print("=== START main() ===", flush=True)

    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    print("Credential path:", cred_path, flush=True)
    print("Credential exists:", os.path.exists(cred_path), flush=True)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)

    print("Authorized Google client", flush=True)
    ws = open_worksheet(gc)
    print("Opened worksheet", flush=True)

    n_rows = ROW_END - ROW_START + 1

    f_vals = ws.col_values(COL_F)[ROW_START - 1 : ROW_END]
    q_vals = ws.col_values(COL_Q)[ROW_START - 1 : ROW_END]
    r_vals = ws.col_values(COL_R)[ROW_START - 1 : ROW_END]

    pad_list(f_vals, n_rows)
    pad_list(q_vals, n_rows)
    pad_list(r_vals, n_rows)

    # 未入力対象（Fあり & (Q空 or R空)）
    targets: List[int] = []
    for i in range(n_rows):
        row = ROW_START + i
        name = (f_vals[i] or "").strip()
        if not name:
            continue
        q = (q_vals[i] or "").strip()
        r = (r_vals[i] or "").strip()
        if (not q) or (not r):
            targets.append(row)

    print("Targets found:", len(targets), flush=True)
    if not targets:
        print("No targets.", flush=True)
        return

    targets = targets[:MAX_UPDATES_PER_RUN]

    updates_q: List[Tuple[int, str]] = []
    updates_r: List[Tuple[int, str]] = []

    print("Launching Playwright...", flush=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
        )
        page = context.new_page()

        for row in targets:
            idx = row - ROW_START
            raw_name = f_vals[idx]
            cur_q = (q_vals[idx] or "").strip()
            cur_r = (r_vals[idx] or "").strip()

            print(f"Processing row {row}: {raw_name}", flush=True)

            q_url, r_url = get_urls_from_judobase(page, raw_name)

            if q_url and not cur_q:
                updates_q.append((row, q_url))
            if r_url and not cur_r:
                updates_r.append((row, r_url))

            time.sleep(SLEEP_PER_PERSON_SEC)

        context.close()
        browser.close()

    print("Updating sheet...", flush=True)

    if updates_q:
        cells = [gspread.Cell(r, COL_Q, v) for r, v in updates_q]
        ws.update_cells(cells, value_input_option="RAW")
    if updates_r:
        cells = [gspread.Cell(r, COL_R, v) for r, v in updates_r]
        ws.update_cells(cells, value_input_option="RAW")

    print(f"Updated Q: {len(updates_q)} / Updated R: {len(updates_r)} / Targets: {len(targets)}", flush=True)
    print("=== DONE main() ===", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
