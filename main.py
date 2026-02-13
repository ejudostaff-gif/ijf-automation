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
WORKSHEET_NAME = ""  # タブ名を指定する場合は例: "Sheet1"

ROW_START = 1
ROW_END = 5000

COL_F = 6
COL_Q = 17
COL_R = 18

MAX_UPDATES_PER_RUN = 100
SLEEP_PER_PERSON_SEC = 1.0
NAV_TIMEOUT_MS = 30000


# ===== 名前処理 =====
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


# ===== judobase =====
PROFILE_ID_RE = re.compile(r"/competitor/profile/(\d+)", re.I)


def extract_profile_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = PROFILE_ID_RE.search(url)
    return m.group(1) if m else None


def judobase_search_id(page, query: str) -> Optional[str]:
    print(f"Searching judobase for: {query}")

    try:
        page.goto("https://judobase.ijf.org/#/search",
                  wait_until="domcontentloaded",
                  timeout=NAV_TIMEOUT_MS)
    except PWTimeoutError:
        print("Timeout loading judobase search page")
        return None

    page.wait_for_timeout(1500)

    try:
        input_box = page.query_selector("input")
        if not input_box:
            print("Search input not found")
            return None

        input_box.fill(query)
        input_box.press("Enter")
        page.wait_for_timeout(3000)
    except Exception as e:
        print("Search error:", e)
        return None

    try:
        hrefs = page.eval_on_selector_all("a", "els => els.map(e => e.href)")
    except Exception:
        hrefs = []

    for h in hrefs:
        pid = extract_profile_id_from_url(h)
        if pid:
            print(f"Found ID: {pid}")
            return pid

    print("No ID found")
    return None


def get_urls_from_judobase(page, raw_name: str) -> Tuple[Optional[str], Optional[str]]:
    name = normalize_name(raw_name)
    if not name:
        return None, None

    for q in build_queries(name):
        pid = judobase_search_id(page, q)
        if pid:
            r_url = f"https://judobase.ijf.org/#/competitor/profile/{pid}"
            q_url = f"https://www.ijf.org/judoka/{pid}"
            return q_url, r_url
        page.wait_for_timeout(800)

    return None, None


# ===== Sheets =====
def open_worksheet(client) -> gspread.Worksheet:
    ss = client.open_by_url(SPREADSHEET_URL)
    if WORKSHEET_NAME:
        return ss.worksheet(WORKSHEET_NAME)
    return ss.get_worksheet(0)


# ===== Main =====
def main():
    print("=== START main() ===")

    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    print("Credential path:", cred_path)
    print("Credential exists:", os.path.exists(cred_path))

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = Credentials.from_service_account_file(
        cred_path,
        scopes=scopes
    )

    gc = gspread.authorize(creds)
    print("Authorized Google client")

    ws = open_worksheet(gc)
    print("Opened worksheet")

    n_rows = ROW_END - ROW_START + 1

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

    print("Targets found:", len(targets))

    if not targets:
        print("No targets.")
        return

    targets = targets[:MAX_UPDATES_PER_RUN]

    updates_q = []
    updates_r = []

    print("Launching Playwright...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for row in targets:
            idx = row - ROW_START
            raw_name = f_vals[idx]

            print(f"Processing row {row}: {raw_name}")

            q_url, r_url = get_urls_from_judobase(page, raw_name)

            if q_url:
                updates_q.append((row, q_url))
            if r_url:
                updates_r.append((row, r_url))

            time.sleep(SLEEP_PER_PERSON_SEC)

        context.close()
        browser.close()

    print("Updating sheet...")

    if updates_q:
        cells = [gspread.Cell(r, COL_Q, v) for r, v in updates_q]
        ws.update_cells(cells, value_input_option="RAW")

    if updates_r:
        cells = [gspread.Cell(r, COL_R, v) for r, v in updates_r]
        ws.update_cells(cells, value_input_option="RAW")

    print(f"Updated Q: {len(updates_q)} / Updated R: {len(updates_r)}")
    print("=== DONE main() ===")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
