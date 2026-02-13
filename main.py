import os
import re
import time
from typing import Optional, List, Tuple
from urllib.parse import quote_plus

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup

# ====== 設定 ======
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "").strip()
SHEET_NAME = os.environ.get("SHEET_NAME", "").strip()  # 空なら先頭シート
START_ROW = int(os.environ.get("START_ROW", "2"))       # データ開始行（ヘッダ除外なら2）
END_ROW = int(os.environ.get("END_ROW", "1000"))        # 例: 1000
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))   # 100ずつ
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "1.0"))   # 連打しない

# 列指定（あなたの要件：F→検索値、Q→IJF URL）
COL_F = 6
COL_Q = 17

IJF_SEARCH_URL = "https://www.ijf.org/search"  # group=competitors&q=... を付ける  [oai_citation:1‡国際柔道連盟](https://www.ijf.org/search?group=competitors&p=1&q=)
IJF_JUDOKA_PREFIX = "https://www.ijf.org/judoka/"


def normalize_name(raw: str) -> str:
    """余計な空白を潰し、, を除去して単語列として整形"""
    s = (raw or "").strip()
    # 末尾/先頭空白を潰し、カンマは消す（ユーザー要件）
    s = s.replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_queries_from_fcell(raw_f: str) -> List[str]:
    """
    F列が "SURNAME, Given Names" か "SURNAME Given" か混在しても、
    できるだけ落とさずに検索語を作る。
    ルール：カンマ削除＋（入れ替え）も試す
    """
    s = (raw_f or "").strip()
    if not s:
        return []

    # まずユーザー要件通り「, を削除して検索」
    normalized = normalize_name(s)

    # もし元が "SURNAME, Given..." 形式なら、入れ替えも作る
    # 例: "SCUTTO, Assunta" -> ["SCUTTO Assunta", "Assunta SCUTTO"]
    # 例: "SILVA DE FREITAS, Bruna Vanessa" -> ["SILVA DE FREITAS Bruna Vanessa", "Bruna Vanessa SILVA DE FREITAS"]
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        surname = normalize_name(parts[0])
        given = normalize_name(parts[1]) if len(parts) > 1 else ""
        if surname and given:
            return [
                f"{surname} {given}".strip(),
                f"{given} {surname}".strip(),
            ]
        return [normalized]

    # カンマ無しの場合は、入れ替えは「単純に最後のトークンを姓とみなす」等は危険なので基本しない
    # （複合姓が多く、誤爆が増える）
    return [normalized]


def ijf_search_first_judoka_url(session: requests.Session, query: str) -> Optional[str]:
    """
    IJF公式検索(competitors)を叩き、最初の /judoka/<id> を返す。
    URL形式は /search?group=competitors&p=1&q=...  [oai_citation:2‡国際柔道連盟](https://www.ijf.org/search?group=competitors&p=1&q=)
    """
    q = (query or "").strip()
    if not q:
        return None

    params = {
        "group": "competitors",
        "p": "1",
        "q": q,
    }
    resp = session.get(IJF_SEARCH_URL, params=params, timeout=30)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # /judoka/<id> へのリンクを拾う
    # 例: https://www.ijf.org/judoka/51120 のような形式が実在  [oai_citation:3‡国際柔道連盟](https://www.ijf.org/judoka/51120)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/judoka/"):
            # /judoka/51120 -> https://www.ijf.org/judoka/51120
            return "https://www.ijf.org" + href
        if href.startswith(IJF_JUDOKA_PREFIX):
            return href

    return None


def open_worksheet(gc: gspread.Client):
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL が空です（GitHub Actions の env に設定してください）")
    ss = gc.open_by_url(SPREADSHEET_URL)
    if SHEET_NAME:
        return ss.worksheet(SHEET_NAME)
    return ss.get_worksheet(0)


def main():
    print("=== START main() ===")
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    print(f"Credential path: {cred_path}")
    print(f"Credential exists: {os.path.exists(cred_path)}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(creds)
    print("Authorized Google client")

    ws = open_worksheet(gc)
    print("Opened worksheet")

    # 読み取り範囲：F列とQ列
    end_row = max(START_ROW, END_ROW)
    rng_f = f"F{START_ROW}:F{end_row}"
    rng_q = f"Q{START_ROW}:Q{end_row}"
    f_vals = ws.get(rng_f)
    q_vals = ws.get(rng_q)

    # 対象行（Qが空で、Fがある）
    targets: List[Tuple[int, str]] = []
    for i in range(len(f_vals)):
        row = START_ROW + i
        fcell = (f_vals[i][0] if f_vals[i] else "").strip()
        qcell = (q_vals[i][0] if q_vals[i] else "").strip()
        if not fcell:
            continue
        # ヘッダっぽい行を除外（任意）
        if row == 2 and fcell.lower() == "name":
            continue
        if qcell:
            continue
        targets.append((row, fcell))

    print(f"Targets found: {len(targets)}")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; ijf-sheet-bot/1.0)"
    })

    updates = []
    checked = 0

    for row, raw_name in targets:
        checked += 1
        print(f"Processing row {row}: {raw_name}")

        queries = build_queries_from_fcell(raw_name)
        found_url = None

        for q in queries:
            print(f"Searching IJF competitors: {q}")
            found_url = ijf_search_first_judoka_url(session, q)
            if found_url:
                break
            time.sleep(SLEEP_SEC)

        if found_url:
            print(f"FOUND: {found_url}")
            updates.append((row, found_url))
        else:
            print("No match (leave blank)")

        # 100件ごとにまとめて書き込み（速度＆API節約）
        if len(updates) >= BATCH_SIZE:
            apply_updates(ws, updates)
            updates = []
            print(f"checked: {checked}")

    if updates:
        apply_updates(ws, updates)

    print("=== DONE ===")


def apply_updates(ws, updates: List[Tuple[int, str]]):
    # Q列に書き込む
    # gspread はまとめ書き込みが速いので、範囲ごとに分けずセル更新
    cells = []
    for row, url in updates:
        cell = ws.cell(row, COL_Q)
        cell.value = url
        cells.append(cell)
    ws.update_cells(cells, value_input_option="RAW")
    print(f"Updated {len(updates)} cells in Q")


if __name__ == "__main__":
    main()
