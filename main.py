import os
import re
import time
from typing import Optional, List, Tuple
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

# 検索用の列（固定運用にするなら env から外してもOK）
FULLNAME_COL = os.getenv("FULLNAME_COL", "K")  # フルネーム
FAMILY_COL = os.getenv("FAMILY_COL", "I")      # ファミリーネーム
GIVEN_COL = os.getenv("GIVEN_COL", "J")        # ファーストネーム

OUTPUT_COL = os.getenv("OUTPUT_COL", "Q")      # IJF URL 出力先


# ==============================
# HTTP 共通
# ==============================

SESSION = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


# ==============================
# Google 接続
# ==============================

def open_worksheet():
    if not SPREADSHEET_URL:
        raise RuntimeError("SPREADSHEET_URL が空です（GitHub Actions の env に設定してください）")
    if not SHEET_NAME:
        raise RuntimeError("SHEET_NAME が空です（GitHub Actions の env に設定してください）")

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
# 文字正規化
# ==============================

def normalize_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def norm_for_match(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ==============================
# IJF検索（誤マッチ削減）
# ==============================

def collect_judoka_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"^/judoka/\d+", href):
            urls.append("https://www.ijf.org" + href.split("#", 1)[0])

    # 重複除去（順序維持）
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def score_candidate(candidate_url: str, family: str, given: str) -> int:
    """
    URL文字列（slug等）に family/given が含まれそうなら加点。
    ※ URLだけでは決めきれないので、上位候補を「プロフィール検証」する前段。
    """
    score = 0
    cu = norm_for_match(candidate_url)
    fam = norm_for_match(family)
    giv = norm_for_match(given)

    if fam and fam in cu:
        score += 3
    if giv and giv in cu:
        score += 2
    return score


def profile_seems_match(profile_url: str, family: str, given: str) -> bool:
    """
    最終防波堤：
    プロフィールHTML本文に family / given が含まれるか（どちらか片方だけでもOKにしてます）。
    これで /judoka/3039 のような“関係ないリンク拾い”をかなり落とせます。
    """
    fam = norm_for_match(family)
    giv = norm_for_match(given)

    # どっちも無いと判定不能
    if not fam and not giv:
        return False

    try:
        r = SESSION.get(profile_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return False

        text = norm_for_match(r.text)

        # 厳しめにしたければ AND に変更
        if fam and fam in text:
            return True
        if giv and giv in text:
            return True

        return False
    except Exception:
        return False


def search_ijf(query: str, family: str, given: str) -> Optional[str]:
    """
    1) 検索ページから候補リンクを複数取得
    2) URL文字列で軽くスコアリング
    3) 上位候補だけプロフィール本文で検証して採用
    """
    base = "https://www.ijf.org/judoka"
    url = f"{base}?{urlencode({'q': query})}"

    try:
        r = SESSION.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None

        candidates = collect_judoka_links(r.text)
        if not candidates:
            return None

        scored = sorted(
            ((score_candidate(c, family, given), c) for c in candidates),
            key=lambda x: x[0],
            reverse=True
        )

        # 上位5件だけ検証（負荷を抑える）
        for sc, cand in scored[:5]:
            if profile_seems_match(cand, family, given):
                return cand

        return None

    except Exception:
        return None


# ==============================
# メイン処理
# ==============================

def main():
    print("=== START main() ===", flush=True)
    print(
        f"Config: FULLNAME_COL={FULLNAME_COL} FAMILY_COL={FAMILY_COL} GIVEN_COL={GIVEN_COL} "
        f"OUTPUT_COL={OUTPUT_COL} START_ROW={START_ROW} END_ROW={END_ROW}",
        flush=True
    )

    ws = open_worksheet()
    print("Opened worksheet", flush=True)

    # 読み込み（まとめて取得して高速化）
    full_vals = ws.get(f"{FULLNAME_COL}{START_ROW}:{FULLNAME_COL}{END_ROW}")
    fam_vals = ws.get(f"{FAMILY_COL}{START_ROW}:{FAMILY_COL}{END_ROW}")
    giv_vals = ws.get(f"{GIVEN_COL}{START_ROW}:{GIVEN_COL}{END_ROW}")
    out_vals = ws.get(f"{OUTPUT_COL}{START_ROW}:{OUTPUT_COL}{END_ROW}")

    total = max(len(full_vals), len(fam_vals), len(giv_vals))
    checked = 0
    found_ijf = 0
    skipped = 0

    for i in range(total):
        row = START_ROW + i

        # 既に出力があるならスキップ（監査モードを作りたいなら別途フラグ化）
        already_out = (i < len(out_vals) and out_vals[i] and str(out_vals[i][0]).strip())
        if already_out:
            skipped += 1
            continue

        fullname = (full_vals[i][0] if i < len(full_vals) and full_vals[i] else "")
        family = (fam_vals[i][0] if i < len(fam_vals) and fam_vals[i] else "")
        given = (giv_vals[i][0] if i < len(giv_vals) and giv_vals[i] else "")

        fullname = normalize_spaces(str(fullname))
        family = normalize_spaces(str(family))
        given = normalize_spaces(str(given))

        # 検索クエリの優先順位：
        # 1) K(フルネーム)があればそれ
        # 2) 無ければ "Given Family"
        query = fullname if fullname else normalize_spaces(f"{given} {family}")

        if not query:
            skipped += 1
            continue

        checked += 1
        print(f"Search IJF: row={row} q='{query}'", flush=True)

        ijf_url = search_ijf(query, family=family, given=given)

        # 追加フォールバック："Family Given" も試す（国/表記揺れ対策）
        if not ijf_url and family and given:
            query2 = normalize_spaces(f"{family} {given}")
            if query2 != query:
                print(f"Search IJF: row={row} q='{query2}'", flush=True)
                ijf_url = search_ijf(query2, family=family, given=given)

        if ijf_url:
            ws.update_acell(f"{OUTPUT_COL}{row}", ijf_url)
            found_ijf += 1
            print(f"FOUND IJF row={row} -> {ijf_url}", flush=True)
        else:
            print(f"NOT FOUND IJF row={row} query='{query}' family='{family}' given='{given}'", flush=True)

        time.sleep(0.5)

    print(f"=== DONE checked={checked} found={found_ijf} skipped={skipped} ===", flush=True)


if __name__ == "__main__":
    main()
