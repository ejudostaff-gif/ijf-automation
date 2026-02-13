import os
import time
import re
import json
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1SwjfDRfcikHrNo38CgFjwBa_oWQM5pnGynFeqikeVU4/edit?gid=249463213#gid=249463213"

def normalize(name):
    return name.replace(",", " ").strip()

def get_urls(name):
    ijf_url = None
    judo_url = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        search_url = "https://www.google.com/search?q=site:ijf.org/judoka+" + name
        page.goto(search_url)
        page.wait_for_timeout(4000)

        content = page.content()
        match = re.search(r'https://www\.ijf\.org/judoka/\d+', content)
        if match:
            ijf_url = match.group(0)
            judoka_id = ijf_url.split("/")[-1]
            judo_url = f"https://judobase.ijf.org/#/competitor/profile/{judoka_id}"

        browser.close()

    return ijf_url, judo_url


def main():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(SPREADSHEET_URL).sheet1

    for i in range(1, 5001):
        name = sheet.cell(i, 6).value
        if not name:
            continue

        name = normalize(name)

        q = sheet.cell(i, 17).value
        r = sheet.cell(i, 18).value

        if not q or not r:
            ijf, judo = get_urls(name)
            if ijf:
                sheet.update_cell(i, 17, ijf)
            if judo:
                sheet.update_cell(i, 18, judo)

            time.sleep(2)

if __name__ == "__main__":
    main()
