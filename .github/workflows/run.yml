import os
import re
import time
from typing import Optional, List, Tuple
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


# ====== env ======
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "").strip()
SHEET_NAME = os.environ.get("SHEET_NAME", "").strip()

START_ROW = int(os.environ.get("START_ROW", "2"))
END_ROW = int(os.environ.get("END_ROW", "1000"))

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))

SEARCH_COL = os.environ.get("SEARCH_COL", "F").strip().upper()   # 例: F
OUTPUT_COL = os.environ.get("OUTPUT_COL", "Q").strip().upper()   # 例: Q

IJF_SEARCH_URL = "https://www.ijf.org/search"
IJF_BASE = "https://www.ijf.org"
IJF_JUDOKA_PREFIX = "https://www.ijf.org/judoka/"


# ====== helpers ======
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def col_to_index(col: str) -> int:
    """A->1, B->2, ..., Z->26, AA->27 ..."""
    c = col.strip().upper()
    if not re.fullmatch(r"[A-Z]+", c):
        raise ValueError(f"Invalid column: {col}")
    n = 0
    for ch in c:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n


def queries_from_cell(raw: str) -> List[str]:
    """
    入力が 'SURNAME, Given...' の場合は入れ替えも試す。
    それ以外はそのまま。
    """
    s = (raw or "").strip()
    if not s:
        return []

    if "," in s:
        last, first = s.split(",", 1)
        last = normalize_spaces(last.replace(",", " "))
        first = normalize_spaces(first.replace(",", " "))
        q1 = normalize_spaces(f"{last} {first}")
        q2 = normalize_spaces(f"{first} {last}")
        if q1.lower() == q2.lower():
            return [q1]
        return [q1, q2]

    return [normalize_spaces(s.replace(",", " "))]


def ijf_search_first_judoka_url(session: requests.Session, query: str) -> Optional[str]:
    q = normalize_spaces(query)
    if not q:
        return None

    params = {"group": "competitors", "p": "1", "q": q}
    url = IJF_SEARCH_URL + "?" + urlencode(params)

    resp = session.get(url, timeout=30)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/judoka/"):
            return IJF_BASE + href
        if href.startswith(IJF_JUDOKA_PREFIX):
            return href
    return None


def open_worksheet(gc: gspread.Client) -> gspread.Worksheet:
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL is empty. Set it in workflow inputs/env.")

    ss = gc.open_by_url(SPREADSHEET_URL)
    if SHEET_NAME:
        return ss.worksheet(SHEET_NAME)
    return ss.get_worksheet(0)


def get_column_values_padded(ws: gspread.Worksheet, col_letter: str, start_row: int, end_row: int) -> List[List[str]]:
    rng = f"{col_letter}{start_row}:{col_letter}{end_row}"
    vals = ws.get(rng)
    expected = end_row - start_row + 1
    if len(vals) < expected:
        vals = vals + ([[]] * (expected - len(vals)))
    return vals


def update_output_cells(ws: gspread.Worksheet, output_col_letter: str, updates: List[Tuple[int, str]]) -> None:
    if not updates:
        return
    col_idx = col_to_index(output_col_letter)
    cells = [gspread.Cell(row, col_idx, url) for row, url in updates]
    ws.update_cells(cells, value_input_option="RAW")
    print(f"Updated {len(updates)} cells in {output_col_letter}", flush=True)


def main():
    print("=== START main() ===", flush=True)
    print(f"Config: SEARCH_COL={SEARCH_COL} OUTPUT_COL={OUTPUT_COL} START_ROW={START_ROW} END_ROW={END_ROW}", flush=True)

    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    print("Credential path:", cred_path, flush=True)
    print("Credential exists:", os.path.exists(cred_path), flush=True)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    print("Authorized Google client", flush=True)

    ws = open_worksheet(gc)
    print("Opened worksheet:", ws.title, flush=True)

    start = START_ROW
    end = max(START_ROW, END_ROW)
    expected_len = end - start + 1

    in_vals = get_column_values_padded(ws, SEARCH_COL, start, end)
    out_vals = get_column_values_padded(ws, OUTPUT_COL, start, end)

    targets: List[Tuple[int, str]] = []
    for i in range(expected_len):
        row = start + i
        in_cell = (in_vals[i][0] if in_vals[i] else "").strip()
        out_cell = (out_vals[i][0] if out_vals[i] else "").strip()

        if not in_cell:
            continue
        if in_cell.lower() == "name":
            continue
        if out_cell:
            continue

        targets.append((row, in_cell))

    print(f"Targets found: {len(targets)}", flush=True)
    if not targets:
        print("No targets. Done.", flush=True)
        return

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; ijf-sheet-bot/1.0)"})

    updates_batch: List[Tuple[int, str]] = []
    checked = 0
    found = 0

    for row, raw_name in targets:
        checked += 1
        qs = queries_from_cell(raw_name)

        got = None
        for q in qs:
            print(f"Search IJF: row={row} q='{q}'", flush=True)
            got = ijf_search_first_judoka_url(session, q)
            if got:
                break
            time.sleep(SLEEP_SEC)

        if got:
            found += 1
            updates_batch.append((row, got))
            print(f"FOUND: row={row} -> {got}", flush=True)
        else:
            print(f"NOT FOUND: row={row} name='{raw_name}'", flush=True)

        if len(updates_batch) >= BATCH_SIZE:
            update_output_cells(ws, OUTPUT_COL, updates_batch)
            updates_batch = []
            print(f"Progress: checked={checked} found={found}", flush=True)

    if updates_batch:
        update_output_cells(ws, OUTPUT_COL, updates_batch)

    print(f"=== DONE checked={checked} found={found} ===", flush=True)


if __name__ == "__main__":
    main()
