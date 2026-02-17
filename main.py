import os
import re
import time
from typing import Optional, List, Tuple
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


# =============================
# 環境変数設定
# =============================

SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "").strip()
SHEET_NAME = os.environ.get("SHEET_NAME", "データベース").strip()

SEARCH_COL = os.environ.get("SEARCH_COL", "F").strip()
IJF_COL = os.environ.get("OUTPUT_COL", "Q").strip()
JUDOINSIDE_COL = os.environ.get("JUDOINSIDE_COL", "P").strip()
BIRTH_COL = os.environ.get("BIRTH_COL", "M").strip()

START_ROW = int(os.environ.get("START_ROW", "2"))
END_ROW = int(os.environ.get("END_ROW", "3000"))

ENABLE_JUDOINSIDE = os.environ.get("ENABLE_JUDOINSIDE", "1") != "0"

SLEEP = 0.4


# =============================
# 共通関数
# =============================

def col_to_num(col: str) -> int:
    num = 0
    for c in col:
        num = num * 26 + (ord(c.upper()) - ord('A') + 1)
    return num


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z]", "", name.lower())


def split_name(name: str) -> Tuple[str, str]:
    if "," in name:
        last, first = name.split(",", 1)
        return last.strip(), first.strip()
    parts = name.strip().split()
    if len(parts) >= 2:
        return parts[-1], " ".join(parts[:-1])
    return name.strip(), ""


# =============================
# Google認証
# =============================

def get_worksheet():
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL 未設定")

    creds = Credentials.from_service_account_file(
        "credentials.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SPREADSHEET_URL)
    return sh.worksheet(SHEET_NAME)


# =============================
# IJF検索
# =============================

def ijf_search(name: str) -> Optional[str]:
    query = urlencode({"name": name})
    url = f"https://www.ijf.org/judoka?{query}"

    r = requests.get(url, timeout=10)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.select_one("a[href*='/judoka/']")
    if link:
        return "https://www.ijf.org" + link["href"]
    return None


# =============================
# JudoInside検索
# =============================

def judoinside_search(name: str) -> Tuple[Optional[str], Optional[str]]:
    query = urlencode({"q": name})
    url = f"https://judoinside.com/search?{query}"

    r = requests.get(url, timeout=10)
    if r.status_code != 200:
        return None, None

    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.select_one("a[href*='/judoka/']")
    if not link:
        return None, None

    profile_url = "https://judoinside.com" + link["href"]

    # 誕生日取得
    r2 = requests.get(profile_url, timeout=10)
    soup2 = BeautifulSoup(r2.text, "html.parser")

    birth = None
    for row in soup2.select("div.fact"):
        if "Born" in row.text:
            birth = row.text.replace("Born", "").strip()
            break

    return profile_url, birth


# =============================
# 色付け
# =============================

def color_cell(ws, row: int, col: str, rgb: Tuple[float, float, float]):
    ws.format(
        f"{col}{row}",
        {
            "backgroundColor": {
                "red": rgb[0],
                "green": rgb[1],
                "blue": rgb[2]
            }
        }
    )


# =============================
# メイン処理
# =============================

def main():
    print("=== START ===", flush=True)
    ws = get_worksheet()

    search_col_num = col_to_num(SEARCH_COL)
    ijf_col_num = col_to_num(IJF_COL)
    ji_col_num = col_to_num(JUDOINSIDE_COL)
    birth_col_num = col_to_num(BIRTH_COL)

    names = ws.col_values(search_col_num)

    checked = 0
    found = 0

    for row in range(START_ROW, min(len(names) + 1, END_ROW + 1)):
        name = names[row - 1].strip()
        if not name:
            continue

        checked += 1
        print(f"Row {row}: {name}", flush=True)

        last, first = split_name(name)
        queries = [
            f"{last} {first}".strip(),
            f"{first} {last}".strip()
        ]

        ijf_url = None
        for q in queries:
            print(f"  IJF search: {q}", flush=True)
            ijf_url = ijf_search(q)
            if ijf_url:
                break
            time.sleep(SLEEP)

        ji_url = None
        birth = None

        if ENABLE_JUDOINSIDE:
            for q in queries:
                print(f"  JudoInside search: {q}", flush=True)
                ji_url, birth = judoinside_search(q)
                if ji_url:
                    break
                time.sleep(SLEEP)

        # =============================
        # 書き込み
        # =============================

        if ijf_url:
            ws.update_cell(row, ijf_col_num, ijf_url)
            found += 1
        else:
            color_cell(ws, row, IJF_COL, (1, 0.8, 0.8))  # 赤系

        if ENABLE_JUDOINSIDE:
            if ji_url:
                ws.update_cell(row, ji_col_num, ji_url)
                if birth:
                    ws.update_cell(row, birth_col_num, birth)
            else:
                color_cell(ws, row, JUDOINSIDE_COL, (1, 1, 0.6))  # 黄色

        time.sleep(SLEEP)

    print(f"=== DONE checked={checked} found={found} ===", flush=True)


if __name__ == "__main__":
    main()import os
import re
import time
from typing import Optional, List, Tuple
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


# =============================
# 環境変数設定
# =============================

SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "").strip()
SHEET_NAME = os.environ.get("SHEET_NAME", "データベース").strip()

SEARCH_COL = os.environ.get("SEARCH_COL", "F").strip()
IJF_COL = os.environ.get("OUTPUT_COL", "Q").strip()
JUDOINSIDE_COL = os.environ.get("JUDOINSIDE_COL", "P").strip()
BIRTH_COL = os.environ.get("BIRTH_COL", "M").strip()

START_ROW = int(os.environ.get("START_ROW", "2"))
END_ROW = int(os.environ.get("END_ROW", "3000"))

ENABLE_JUDOINSIDE = os.environ.get("ENABLE_JUDOINSIDE", "1") != "0"

SLEEP = 0.4


# =============================
# 共通関数
# =============================

def col_to_num(col: str) -> int:
    num = 0
    for c in col:
        num = num * 26 + (ord(c.upper()) - ord('A') + 1)
    return num


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z]", "", name.lower())


def split_name(name: str) -> Tuple[str, str]:
    if "," in name:
        last, first = name.split(",", 1)
        return last.strip(), first.strip()
    parts = name.strip().split()
    if len(parts) >= 2:
        return parts[-1], " ".join(parts[:-1])
    return name.strip(), ""


# =============================
# Google認証
# =============================

def get_worksheet():
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL 未設定")

    creds = Credentials.from_service_account_file(
        "credentials.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SPREADSHEET_URL)
    return sh.worksheet(SHEET_NAME)


# =============================
# IJF検索
# =============================

def ijf_search(name: str) -> Optional[str]:
    query = urlencode({"name": name})
    url = f"https://www.ijf.org/judoka?{query}"

    r = requests.get(url, timeout=10)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.select_one("a[href*='/judoka/']")
    if link:
        return "https://www.ijf.org" + link["href"]
    return None


# =============================
# JudoInside検索
# =============================

def judoinside_search(name: str) -> Tuple[Optional[str], Optional[str]]:
    query = urlencode({"q": name})
    url = f"https://judoinside.com/search?{query}"

    r = requests.get(url, timeout=10)
    if r.status_code != 200:
        return None, None

    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.select_one("a[href*='/judoka/']")
    if not link:
        return None, None

    profile_url = "https://judoinside.com" + link["href"]

    # 誕生日取得
    r2 = requests.get(profile_url, timeout=10)
    soup2 = BeautifulSoup(r2.text, "html.parser")

    birth = None
    for row in soup2.select("div.fact"):
        if "Born" in row.text:
            birth = row.text.replace("Born", "").strip()
            break

    return profile_url, birth


# =============================
# 色付け
# =============================

def color_cell(ws, row: int, col: str, rgb: Tuple[float, float, float]):
    ws.format(
        f"{col}{row}",
        {
            "backgroundColor": {
                "red": rgb[0],
                "green": rgb[1],
                "blue": rgb[2]
            }
        }
    )


# =============================
# メイン処理
# =============================

def main():
    print("=== START ===", flush=True)
    ws = get_worksheet()

    search_col_num = col_to_num(SEARCH_COL)
    ijf_col_num = col_to_num(IJF_COL)
    ji_col_num = col_to_num(JUDOINSIDE_COL)
    birth_col_num = col_to_num(BIRTH_COL)

    names = ws.col_values(search_col_num)

    checked = 0
    found = 0

    for row in range(START_ROW, min(len(names) + 1, END_ROW + 1)):
        name = names[row - 1].strip()
        if not name:
            continue

        checked += 1
        print(f"Row {row}: {name}", flush=True)

        last, first = split_name(name)
        queries = [
            f"{last} {first}".strip(),
            f"{first} {last}".strip()
        ]

        ijf_url = None
        for q in queries:
            print(f"  IJF search: {q}", flush=True)
            ijf_url = ijf_search(q)
            if ijf_url:
                break
            time.sleep(SLEEP)

        ji_url = None
        birth = None

        if ENABLE_JUDOINSIDE:
            for q in queries:
                print(f"  JudoInside search: {q}", flush=True)
                ji_url, birth = judoinside_search(q)
                if ji_url:
                    break
                time.sleep(SLEEP)

        # =============================
        # 書き込み
        # =============================

        if ijf_url:
            ws.update_cell(row, ijf_col_num, ijf_url)
            found += 1
        else:
            color_cell(ws, row, IJF_COL, (1, 0.8, 0.8))  # 赤系

        if ENABLE_JUDOINSIDE:
            if ji_url:
                ws.update_cell(row, ji_col_num, ji_url)
                if birth:
                    ws.update_cell(row, birth_col_num, birth)
            else:
                color_cell(ws, row, JUDOINSIDE_COL, (1, 1, 0.6))  # 黄色

        time.sleep(SLEEP)

    print(f"=== DONE checked={checked} found={found} ===", flush=True)


if __name__ == "__main__":
    main()
