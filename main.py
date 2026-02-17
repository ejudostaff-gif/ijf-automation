import os
import re
import time
from typing import Optional
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


# ==============================
# 環境変数
# ==============================

SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
SHEET_NAME = os.getenv("SHEET_NAME")
START_ROW = int(os.getenv("START_ROW", "2"))
END_ROW = int(os.getenv("END_ROW", "1000"))

SEARCH_COL = os.getenv("SEARCH_COL", "F")
OUTPUT_COL = os.getenv("OUTPUT_COL", "Q")          # IJF
JUDOINSIDE_COL = os.getenv("JUDOINSIDE_COL", "P")  # JudoInside
BIRTH_COL = os.getenv("BIRTH_COL", "M")

ENABLE_JUDOINSIDE = os.getenv("ENABLE_JUDOINSIDE", "0") == "1"


# ==============================
# Google 接続
# ==============================

def open_worksheet():
    creds = Credentials.from_service_account_file(
        "credentials.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ],
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_url(SPREADSHEET_URL)
    ws = ss.worksheet(SHEET_NAME)
    return ws


# ==============================
# 名前整形
# ==============================

def normalize_name(name: str) -> str:
    name = name.replace(",", " ")
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def swap_name(name: str) -> str:
    if "," in name:
        parts = [p.strip() for p in name.split(",")]
        if len(parts) == 2:
            return f"{parts[1]} {parts[0]}"
    return name


# ==============================
# IJF検索
# ==============================

def search_ijf(name: str) -> Optional[str]:
    base = "https://www.ijf.org/judoka"
    params = urlencode({"q": name})
    url = f"{base}?{params}"

    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")
        link = soup.find("a", href=re.compile(r"/judoka/\d+"))
        if link:
            return "https://www.ijf.org" + link["href"]

    except Exception:
        return None

    return None


# ==============================
# JudoInside検索
# ==============================

def search_judoinside(name: str):
    search_url = "https://judoinside.com/search"
    params = urlencode({"q": name})
    url = f"{search_url}?{params}"

    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None, None

        soup = BeautifulSoup(r.text, "html.parser")
        link = soup.find("a", href=re.compile(r"/judoka/\d+/"))

        if not link:
            return None, None

        profile_url = "https://judoinside.com" + link["href"]

        # 生年月日取得
        r2 = requests.get(profile_url, timeout=10)
        soup2 = BeautifulSoup(r2.text, "html.parser")

        birth = None
        text = soup2.get_text()
        m = re.search(r"Born\s*:\s*(\d{4}-\d{2}-\d{2})", text)
        if m:
            birth = m.group(1).replace("-", "/")

        return profile_url, birth

    except Exception:
        return None, None


# ==============================
# メイン処理
# ==============================

def main():
    print("=== START main() ===")
    print(f"Config: SEARCH_COL={SEARCH_COL} OUTPUT_COL={OUTPUT_COL}")

    ws = open_worksheet()
    print("Opened worksheet")

    names = ws.get(f"{SEARCH_COL}{START_ROW}:{SEARCH_COL}{END_ROW}")
    q_vals = ws.get(f"{OUTPUT_COL}{START_ROW}:{OUTPUT_COL}{END_ROW}")
    p_vals = ws.get(f"{JUDOINSIDE_COL}{START_ROW}:{JUDOINSIDE_COL}{END_ROW}")
    m_vals = ws.get(f"{BIRTH_COL}{START_ROW}:{BIRTH_COL}{END_ROW}")

    total = len(names)
    found = 0

    for i in range(total):
        row = START_ROW + i

        if not names[i]:
            continue

        original_name = names[i][0].strip()
        if not original_name:
            continue

        # 既にIJFが入っている場合スキップ
        if i < len(q_vals) and q_vals[i] and q_vals[i][0].strip():
            continue

        name = normalize_name(original_name)
        swapped = swap_name(original_name)

        print(f"Search IJF: row={row} q='{name}'")

        ijf_url = search_ijf(name)
        if not ijf_url and swapped != name:
            print(f"Search IJF: row={row} q='{swapped}'")
            ijf_url = search_ijf(swapped)

        if ijf_url:
            ws.update_acell(f"{OUTPUT_COL}{row}", ijf_url)
            found += 1
            print(f"FOUND IJF row={row}")
        else:
            print(f"NOT FOUND IJF row={row}")

        if ENABLE_JUDOINSIDE:
            if i < len(p_vals) and p_vals[i] and p_vals[i][0].strip():
                continue

            ji_url, birth = search_judoinside(name)

            if ji_url:
                ws.update_acell(f"{JUDOINSIDE_COL}{row}", ji_url)
                print(f"FOUND JI row={row}")

            if birth:
                ws.update_acell(f"{BIRTH_COL}{row}", birth)
                print(f"BIRTH row={row}")

        time.sleep(0.5)

    print(f"=== DONE checked={total} found={found} ===")


if __name__ == "__main__":
    main()
