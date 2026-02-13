import os
import time
import re
import json
from typing import Optional, Tuple, List

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ===== 設定 =====
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1SwjfDRfcikHrNo38CgFjwBa_oWQM5pnGynFeqikeVU4"

# 重要：対象タブが「1枚目じゃない」場合は、ここをシート下のタブ名に合わせて変更してください
WORKSHEET_NAME = ""  # 例: "Sheet1" / "2026" など。空なら先頭シートを使います。

ROW_START = 1
ROW_END = 5000

COL_F = 6    # 検索値（名前）
COL_Q = 17   # IJF URL
COL_R = 18   # judobase URL

# 1回のGitHub Actions実行で処理する「未入力行」の最大件数（成功率優先なら小さめが安定）
MAX_UPDATES_PER_RUN = 120

# 1人あたりの待機（ブロック回避＆安定性）
SLEEP_PER_PERSON_SEC = 1.5

# Google検索のタイムアウト
NAV_TIMEOUT_MS = 25000


# ===== 文字列処理 =====
def normalize_name(raw: str) -> str:
    # ルール：カンマ削除のみ + 空白正規化
    s = (raw or "").replace(",", " ").replace("，", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def swap_first_last(name: str) -> str:
    # "First Last" -> "Last First"（2語以上は先頭と末尾のみ入れ替え）
    parts = name.split()
    if len(parts) < 2:
        return name
    return f"{parts[-1]} {parts[0]}"

def build_queries(name: str) -> List[str]:
    n1 = name
    n2 = swap_first_last(name)
    # 同一なら1回だけ
    if n2.lower() == n1.lower():
        return [n1]
    return [n1, n2]


# ===== PlaywrightでGoogleからIJF URL抽出 =====
IJF_URL_RE = re.compile(r"^https?://www\.ijf\.org/judoka/\d+/?$", re.I)

def extract_ijf_from_hrefs(hrefs: List[str]) -> Optional[str]:
    for h in hrefs:
        if not h:
            continue
        h = h.split("#", 1)[0]
        h = h.split("?", 1)[0]
        if IJF_URL_RE.match(h):
            return h if h.startswith("https://") else h.replace("http://", "https://")
    return None

def google_search_ijf(page, name_query: str) -> Optional[str]:
    # site指定でIJF judokaだけ狙う
    q = f"site:ijf.org/judoka {name_query}"
    url = "https://www.google.com/search?q=" + re.sub(r"\s+", "+", q.strip())

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        page.wait_for_timeout(1500)
    except PWTimeoutError:
        return None

    # aタグのhrefを収集して、IJFのjudoka URLだけ拾う
    try:
        hrefs = page.eval_on_selector_all("a", "els => els.map(e => e.href)")
    except Exception:
        return None

    return extract_ijf_from_hrefs(hrefs)


def get_urls_for_name(page, raw_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    戻り値:
      (ijf_url, judobase_url)
    judobaseはIJFのIDと同じ前提で生成:
      ijf: https://www.ijf.org/judoka/38986
      jbase: https://judobase.ijf.org/#/competitor/profile/38986
    """
    name = normalize_name(raw_name)
    if not name:
        return None, None

    for q in build_queries(name):
        ijf = google_search_ijf(page, q)
        if ijf:
            jid = ijf.rstrip("/").split("/")[-1]
            jbase = f"https://judobase.ijf.org/#/competitor/profile/{jid}"
            return ijf, jbase

        # 少し間を空けて再試行耐性
        page.wait_for_timeout(600)

    return None, None


# ===== Sheets =====
def open_worksheet(client) -> gspread.Worksheet:
    ss = client.open_by_url(SPREADSHEET_URL)
    if WORKSHEET_NAME:
        return ss.worksheet(WORKSHEET_NAME)
    return ss.get_worksheet(0)


def main():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)

    ws = open_worksheet(gc)

    # F/Q/R をまとめて読む（高速）
    n_rows = ROW_END - ROW_START + 1
    f_vals = ws.col_values(COL_F)[ROW_START-1:ROW_END]  # 1-index行に合わせてスライス
    q_vals = ws.col_values(COL_Q)[ROW_START-1:ROW_END]
    r_vals = ws.col_values(COL_R)[ROW_START-1:ROW_END]

    # gspreadのcol_valuesは列の末尾空白を返さないことがあるので長さを揃える
    def pad(lst, size):
        if len(lst) < size:
            lst.extend([""] * (size - len(lst)))
        return lst

    pad(f_vals, n_rows)
    pad(q_vals, n_rows)
    pad(r_vals, n_rows)

    targets = []
    for idx in range(n_rows):
        row_num = ROW_START + idx
        name = f_vals[idx] or ""
        if not name.strip():
            continue
        q = (q_vals[idx] or "").strip()
        r = (r_vals[idx] or "").strip()
        # QかRどちらか空なら対象
        if (not q) or (not r):
            targets.append(row_num)

    if not targets:
        print("No targets (Q/R already filled).")
        return

    # 1回の実行で処理する件数を絞る（成功率優先）
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
            current_q = (q_vals[idx] or "").strip()
            current_r = (r_vals[idx] or "").strip()

            ijf, jbase = get_urls_for_name(page, raw_name)

            if ijf and not current_q:
                updates_q.append((row, ijf))
            if jbase and not current_r:
                updates_r.append((row, jbase))

            time.sleep(SLEEP_PER_PERSON_SEC)

        context.close()
        browser.close()

    # まとめて書き込み（高速・API節約）
    if updates_q:
        cell_list = [gspread.Cell(r, COL_Q, v) for r, v in updates_q]
        ws.update_cells(cell_list, value_input_option="RAW")
    if updates_r:
        cell_list = [gspread.Cell(r, COL_R, v) for r, v in updates_r]
        ws.update_cells(cell_list, value_input_option="RAW")

    print(f"Updated Q: {len(updates_q)} / Updated R: {len(updates_r)} / Scanned targets: {len(targets)}")


if __name__ == "__main__":
    main()
