import os
import time
import re
from typing import Optional, Tuple, List

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ===== 設定 =====
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1SwjfDRfcikHrNo38CgFjwBa_oWQM5pnGynFeqikeVU4"
WORKSHEET_NAME = ""  # 対象タブが先頭でなければタブ名を入れる（例: "Sheet1"）

ROW_START = 1
ROW_END = 5000

COL_F = 6    # 名前
COL_Q = 17   # IJF URL
COL_R = 18   # judobase URL

MAX_UPDATES_PER_RUN = 120          # 1回の実行で処理する最大件数（未入力行）
SLEEP_PER_PERSON_SEC = 1.0         # ブロック回避
NAV_TIMEOUT_MS = 30000

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


# ===== judobase検索 =====
PROFILE_ID_RE = re.compile(r"/competitor/profile/(\d+)", re.I)

def extract_profile_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = PROFILE_ID_RE.search(url)
    return m.group(1) if m else None

def judobase_search_id(page, query: str) -> Optional[str]:
    """
    judobaseのUIを操作して検索し、最初に見つかったprofile IDを返す。
    （UI変更に備えて、複数のセレクタ候補を試す）
    """
    try:
        page.goto("https://judobase.ijf.org/#/search", wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    except PWTimeoutError:
        return None

    # 検索入力欄候補（UIが変わってもどれか当たりやすい）
    input_selectors = [
        "input[type='search']",
        "input[placeholder*='Search']",
        "input[placeholder*='search']",
        "input",
    ]

    # 検索ボタン候補
    button_selectors = [
        "button:has-text('Search')",
        "button:has-text('SEARCH')",
        "button[type='submit']",
    ]

    # 入力欄を探して入力
    inp = None
    for sel in input_selectors:
        try:
            cand = page.query_selector(sel)
            if cand:
                inp = cand
                break
        except Exception:
            pass
    if not inp:
        return None

    try:
        inp.fill(query)
        inp.press("Enter")
    except Exception:
        # Enterが効かないUI向けにボタン押下も試す
        for bsel in button_selectors:
            try:
                btn = page.query_selector(bsel)
                if btn:
                    btn.click()
                    break
            except Exception:
                pass

    # 結果が出るまで少し待つ
    page.wait_for_timeout(2500)

    # 結果一覧から profile リンクを拾う
    # a[href*="/competitor/profile/"] を優先
    try:
        hrefs = page.eval_on_selector_all("a", "els => els.map(e => e.href)")
    except Exception:
        hrefs = []

    for h in hrefs:
        pid = extract_profile_id_from_url(h)
        if pid:
            return pid

    # それでもダメならページ全体HTMLから拾う（最終手段）
    try:
        html = page.content()
        m = PROFILE_ID_RE.search(html)
        if m:
            return m.group(1)
    except Exception:
        pass

    return None


def get_urls_from_judobase(page, raw_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    judobase検索でID取得 → R(judobase) & Q(IJF) を生成
    """
    name = normalize_name(raw_name)
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


def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=scopes
    )
    gc = gspread.authorize(creds)
    ws = open_worksheet(gc)

    n_rows = ROW_END - ROW_START + 1

    # 列をまとめて取得（高速）
    f_vals = ws.col_values(COL_F)[ROW_START-1:ROW_END]
    q_vals = ws.col_values(COL_Q)[ROW_START-1:ROW_END]
    r_vals = ws.col_values(COL_R)[ROW_START-1:ROW_END]

    def pad(lst, size):
        if len(lst) < size:
            lst.extend([""] * (size - len(lst)))
        return lst

    pad(f_vals, n_rows)
    pad(q_vals, n_rows)
    pad(r_vals, n_rows)

    targets = []
    for i in range(n_rows):
        row = ROW_START + i
        name = (f_vals[i] or "").strip()
        if not name:
            continue
        q = (q_vals[i] or "").strip()
        r = (r_vals[i] or "").strip()
        if (not q) or (not r):
            targets.append(row)

    if not targets:
        print("No targets (Q/R already filled).")
        return

    targets = targets[:MAX_UPDATES_PER_RUN]

    updates_q = []
    updates_r = []

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

            q_url, r_url = get_urls_from_judobase(page, raw_name)

            if q_url and not cur_q:
                updates_q.append((row, q_url))
            if r_url and not cur_r:
                updates_r.append((row, r_url))

            time.sleep(SLEEP_PER_PERSON_SEC)

        context.close()
        browser.close()

    if updates_q:
        cells = [gspread.Cell(r, COL_Q, v) for r, v in updates_q]
        ws.update_cells(cells, value_input_option="RAW")
    if updates_r:
        cells = [gspread.Cell(r, COL_R, v) for r, v in updates_r]
        ws.update_cells(cells, value_input_option="RAW")

    print(f"Updated Q: {len(updates_q)} / Updated R: {len(updates_r)} / Targets: {len(targets)}")


if __name__ == "__main__":
    main()
