import os
import re
import time
from typing import Optional, List, Tuple, Dict
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


# =======================
# env
# =======================
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "").strip()
SHEET_NAME = os.environ.get("SHEET_NAME", "").strip()

START_ROW = int(os.environ.get("START_ROW", "2"))
END_ROW = int(os.environ.get("END_ROW", "1000"))

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))

SEARCH_COL = os.environ.get("SEARCH_COL", "F").strip().upper()   # e.g. F
OUTPUT_COL = os.environ.get("OUTPUT_COL", "Q").strip().upper()   # e.g. Q

# search pages p=1..N
IJF_MAX_PAGES = int(os.environ.get("IJF_MAX_PAGES", "3"))

# candidate selection to reduce mismatches
CAND_TOP_N = int(os.environ.get("CAND_TOP_N", "8"))              # compare top N results per page
MATCH_THRESHOLD = float(os.environ.get("MATCH_THRESHOLD", "0.82"))  # stricter => fewer mismatches, more blanks
MIN_TOKENS = int(os.environ.get("MIN_TOKENS", "2"))              # avoid weak matches on too-short names

IJF_SEARCH_URL = "https://www.ijf.org/search"
IJF_BASE = "https://www.ijf.org"
IJF_JUDOKA_PREFIX = "https://www.ijf.org/judoka/"


# =======================
# helpers
# =======================
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
    If raw is like 'SURNAME, Given ...', try both orders.
    Otherwise, return as-is (with commas removed).
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


def tokenise_name(s: str) -> List[str]:
    """
    Tokenize for rough matching:
    - uppercase
    - remove punctuation except spaces
    - split on spaces
    """
    s2 = (s or "").upper()
    # normalize apostrophes/hyphens into spaces, strip remaining punctuation
    s2 = re.sub(r"[-'’`]", " ", s2)
    s2 = re.sub(r"[^A-Z0-9 ]+", " ", s2)
    s2 = normalize_spaces(s2)
    toks = [t for t in s2.split(" ") if t]
    return toks


def name_match_score(input_name: str, candidate_name: str) -> float:
    """
    Fast-ish token overlap score (0..1) to reduce mismatches.
    - Primary: coverage of input tokens in candidate
    - Secondary: coverage of candidate tokens in input
    """
    a = tokenise_name(input_name)
    b = tokenise_name(candidate_name)

    if len(a) < MIN_TOKENS or len(b) < MIN_TOKENS:
        return 0.0

    set_a = set(a)
    set_b = set(b)

    inter = set_a & set_b
    if not inter:
        return 0.0

    # coverage from both sides (weighted)
    cov_a = len(inter) / max(1, len(set_a))
    cov_b = len(inter) / max(1, len(set_b))

    # reward strong overlap; penalize missing tokens a bit
    score = 0.7 * cov_a + 0.3 * cov_b
    return float(score)


def parse_ijf_candidates(html: str) -> List[Dict[str, str]]:
    """
    Return list of candidates with fields:
      - url: full IJF judoka URL
      - name: visible text for the link (best effort)
    """
    soup = BeautifulSoup(html, "html.parser")
    cands: List[Dict[str, str]] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        url = None
        if href.startswith("/judoka/"):
            url = IJF_BASE + href
        elif href.startswith(IJF_JUDOKA_PREFIX):
            url = href
        else:
            continue

        # Best-effort candidate label:
        # Use link text; if empty, try aria-label/title; else empty string.
        txt = normalize_spaces(a.get_text(" ", strip=True))
        if not txt:
            txt = normalize_spaces(a.get("aria-label", "") or a.get("title", "") or "")

        cands.append({"url": url, "name": txt})

    # de-duplicate by url (keep first)
    seen = set()
    uniq = []
    for c in cands:
        if c["url"] in seen:
            continue
        seen.add(c["url"])
        uniq.append(c)

    return uniq


def ijf_search_best_judoka_url(session: requests.Session, query: str) -> Optional[str]:
    """
    Search IJF and choose best candidate by name match score.
    - checks p=1..IJF_MAX_PAGES
    - evaluates top CAND_TOP_N candidates per page
    - requires MATCH_THRESHOLD to accept; otherwise returns None (avoid mismatches)
    """
    q = normalize_spaces(query)
    if not q:
        return None

    best_url = None
    best_score = 0.0

    for p in range(1, IJF_MAX_PAGES + 1):
        params = {"group": "competitors", "p": str(p), "q": q}
        url = IJF_SEARCH_URL + "?" + urlencode(params)

        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            # try next page
            continue

        candidates = parse_ijf_candidates(resp.text)

        # score top N
        for cand in candidates[:CAND_TOP_N]:
            cand_name = cand["name"] or ""
            # If IJF doesn't show usable names, we can only be conservative:
            # treat empty candidate name as non-match.
            if not cand_name:
                continue

            sc = name_match_score(q, cand_name)
            if sc > best_score:
                best_score = sc
                best_url = cand["url"]

        # If we already have a strong enough match, stop early
        if best_url and best_score >= MATCH_THRESHOLD:
            return best_url

        # small pause before next page
        time.sleep(SLEEP_SEC)

    # accept only if above threshold
    if best_url and best_score >= MATCH_THRESHOLD:
        return best_url
    return None


def open_worksheet(gc: gspread.Client) -> gspread.Worksheet:
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL が空です（GitHub Actions の inputs/env に設定してください）")
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


# =======================
# main
# =======================
def main():
    print("=== START main() ===", flush=True)
    print(
        f"Config: SEARCH_COL={SEARCH_COL} OUTPUT_COL={OUTPUT_COL} "
        f"START_ROW={START_ROW} END_ROW={END_ROW} "
        f"IJF_MAX_PAGES={IJF_MAX_PAGES} TOP_N={CAND_TOP_N} THRESHOLD={MATCH_THRESHOLD}",
        flush=True
    )

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
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; ijf-sheet-bot/1.1)"})

    updates_batch: List[Tuple[int, str]] = []
    checked = 0
    found = 0

    for row, raw_name in targets:
        checked += 1

        qs = queries_from_cell(raw_name)
        got = None

        for q in qs:
            print(f"Search IJF: row={row} q='{q}'", flush=True)
            got = ijf_search_best_judoka_url(session, q)
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
