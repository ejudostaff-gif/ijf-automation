import os
import re
import time
from typing import Optional, List
from urllib.parse import urlencode

import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup


SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
SHEET_NAME = os.getenv("SHEET_NAME")

START_ROW = int(os.getenv("START_ROW", "2"))
END_ROW = int(os.getenv("END_ROW", "1000"))

FULLNAME_COL = os.getenv("FULLNAME_COL", "K")
FAMILY_COL = os.getenv("FAMILY_COL", "I")
GIVEN_COL = os.getenv("GIVEN_COL", "J")

OUTPUT_COL = os.getenv("OUTPUT_COL", "Q")


SESSION = requests.Session()

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def open_sheet():

    creds = Credentials.from_service_account_file(
        "credentials.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    gc = gspread.authorize(creds)

    ss = gc.open_by_url(SPREADSHEET_URL)

    return ss.worksheet(SHEET_NAME)


def normalize(s):

    s = (s or "").strip()

    s = s.replace(",", " ")

    s = re.sub(r"\s+", " ", s)

    return s


def collect_links(html):

    soup = BeautifulSoup(html, "html.parser")

    urls = []

    for a in soup.find_all("a", href=True):

        href = a["href"]

        if re.search(r"/judoka/\d+", href):

            urls.append("https://www.ijf.org" + href)

    return list(dict.fromkeys(urls))


def search_ijf(query):

    url = "https://www.ijf.org/judoka?" + urlencode({"q": query})

    r = SESSION.get(url, headers=HEADERS)

    if r.status_code != 200:

        return None

    links = collect_links(r.text)

    if not links:

        return None

    return links[0]


def main():

    print("START")

    ws = open_sheet()

    full_vals = ws.get(f"{FULLNAME_COL}{START_ROW}:{FULLNAME_COL}{END_ROW}")

    fam_vals = ws.get(f"{FAMILY_COL}{START_ROW}:{FAMILY_COL}{END_ROW}")

    giv_vals = ws.get(f"{GIVEN_COL}{START_ROW}:{GIVEN_COL}{END_ROW}")

    out_vals = ws.get(f"{OUTPUT_COL}{START_ROW}:{OUTPUT_COL}{END_ROW}")

    for i in range(len(full_vals)):

        row = START_ROW + i

        if out_vals[i] and out_vals[i][0]:

            continue

        fullname = normalize(full_vals[i][0]) if full_vals[i] else ""

        family = normalize(fam_vals[i][0]) if fam_vals[i] else ""

        given = normalize(giv_vals[i][0]) if giv_vals[i] else ""

        query = fullname if fullname else f"{given} {family}"

        print("search", query)

        url = search_ijf(query)

        if url:

            ws.update_acell(f"{OUTPUT_COL}{row}", url)

            print("found", url)

        else:

            print("not found")

        time.sleep(0.5)


if __name__ == "__main__":
    main()
