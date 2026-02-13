import os
import re
import time
from typing import Optional, List, Tuple
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


# ====== 環境変数から設定（run.yml の env で渡す） ======
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "").strip()
SHEET_NAME = os.environ.get("SHEET_NAME", "").strip()            # 例: "データベース"（空なら先頭タブ）
START_ROW = int(os.environ.get("START_ROW", "2"))                # データ開始行（ヘッダー除外なら2）
END_ROW = int(os.environ.get("END_ROW", "1000"))                 # 例: 1000
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))            # 例: 100
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.8"))            # 連続アクセス抑制（0.5〜1.5推奨）

# 列（固定：F→検索値、Q→IJF URL）
COL_F = 6
COL_Q = 17

IJF_SEARCH_URL = "https://www.ijf.org/search"
IJF_BASE = "https://www.ijf.org"
IJF_JUDOKA_PREFIX = "https://www.ijf.org/judoka/"


# ====== ユーティリティ ======
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def queries_from_fcell(raw: str) -> List[str]:
    """
    ルール：
    - F列はアルファベット
    - F列は , を削除して検索
    - 可能なら姓名を入れ替えた検索も試す
      例: "SCUTTO, Assunta" -> ["SCUTTO Assunta", "Assunta SCUTTO"]
          "SILVA DE FREITAS, Bruna Vanessa" -> ["SILVA DE FREITAS Bruna Vanessa", "Bruna Vanessa SILVA DE FREITAS"]
    - カンマ無しは安全のため入れ替えはしない（複合姓で誤爆しやすい）
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

    # カンマ無し：そのまま（カンマ除去はしておく）
    return [normalize_spaces(s.replace(",", " "))]


def ijf_search_first_judoka_url(session: requests.Session, query: str) -> Optional[str]:
    """
    IJFのcompetitors検索を叩いて、最初に見つかった /judoka/<id> を返す。
    """
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
        raise RuntimeError("SPREADSHEET_URL が空です（run.yml の env に設定してください）")

    ss = gc.open_by_url(SPREADSHEET_URL)
    if SHEET_NAME:
        return ss.worksheet(SHEET_NAME)
    return ss.get_worksheet(0)


def get_column_values_padded(ws: gspread.Worksheet, col_letter: str, start_row: int, end_row: int) -> List[List[str]]:
    """
    ws.get("F2:F1000") などは末尾の空行を返さないことがあるため、
    指定行数まで [] でパディングして長さを揃える。
    戻り値は2次元リスト（例: [["A"], [], ["B"] ...]）
    """
    rng = f"{col_letter}{start_row}:{col_letter}{end_row}"
    vals = ws.get(rng)
    expected = end_row - start_row + 1
    if len(vals) < expected:
        vals = vals + ([[]] * (expected - len(vals)))
    return vals


def update_q_cells(ws: gspread.Worksheet, updates: List[Tuple[int, str]]) -> None:
    """
    updates: [(row, url), ...] を Q列に書き込み
    """
    if not updates:
        return

    cells = []
    for row, url in updates:
        # 既存セルオブジェクトを作って値をセット
        c = gspread.Cell(row, COL_Q, url)
        cells.append(c)

    ws.update_cells(cells, value_input_option="RAW")
    print(f"Updated {len(updates)} cells in Q", flush=True)


def main():
    print("=== START main() ===", flush=True)

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

    # F列とQ列を取得（末尾空行は返らないのでパディング）
    f_vals = get_column_values_padded(ws, "F", start, end)
    q_vals = get_column_values_padded(ws, "Q", start, end)

    expected_len = end - start + 1

    # 対象：Fがあり、Qが空
    targets: List[Tuple[int, str]] = []
    for i in range(expected_len):
        row = start + i
        fcell = (f_vals[i][0] if f_vals[i] else "").strip()
        qcell = (q_vals[i][0] if q_vals[i] else "").strip()

        if not fcell:
            continue
        # ヘッダー除外（Nameなど）
        if fcell.lower() == "name":
            continue
        if qcell:
            continue

        targets.append((row, fcell))

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
        qs = queries_from_fcell(raw_name)

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

        # バッチ書き込み
        if len(updates_batch) >= BATCH_SIZE:
            update_q_cells(ws, updates_batch)
            updates_batch = []
            print(f"Progress: checked={checked} found={found}", flush=True)

    # 残りを反映
    if updates_batch:
        update_q_cells(ws, updates_batch)

    print(f"=== DONE checked={checked} found={found} ===", flush=True)


if __name__ == "__main__":
    main()
