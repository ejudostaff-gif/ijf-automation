def judobase_search_id(page, query: str) -> Optional[str]:
    print(f"Searching judobase for: {query}", flush=True)

    try:
        page.goto("https://judobase.ijf.org/#/search",
                  wait_until="domcontentloaded",
                  timeout=NAV_TIMEOUT_MS)
    except PWTimeoutError:
        print("Timeout loading judobase search page", flush=True)
        return None

    page.wait_for_timeout(1500)

    # できるだけ「検索欄らしい」inputを拾う
    # （最初のinputだと別の入力欄を掴むことがあるため）
    input_box = None
    for sel in [
        "input[type='search']",
        "input[placeholder*='Search' i]",
        "input[aria-label*='Search' i]",
        "input",
    ]:
        try:
            cand = page.query_selector(sel)
            if cand:
                input_box = cand
                break
        except Exception:
            pass

    if not input_box:
        print("Search input not found", flush=True)
        return None

    # 入力→Enter
    try:
        input_box.click()
        input_box.fill(query)
        input_box.press("Enter")
    except Exception as e:
        print("Search input error:", e, flush=True)
        return None

    # 結果が描画されるのを待つ
    page.wait_for_timeout(2500)

    # 1) もし既にprofileへ遷移していたらURLから取る
    pid = extract_profile_id_from_url(page.url)
    if pid:
        print(f"Found ID from URL: {pid}", flush=True)
        return pid

    # 2) 「結果っぽいクリック可能要素」をクリックして遷移させる
    # judobaseは <a> でなく div クリックのことがあるので広めに拾う
    click_selectors = [
        "a:has-text('profile')",
        "a[href*='competitor/profile']",
        "[role='link']",
        "table a",
        "tbody tr",
        ".results a",
        ".result a",
        ".search-results a",
        "mat-row",          # Angular Materialの可能性
        "mat-list-item",
        "li",
        "div[onclick]",
    ]

    clicked = False
    for sel in click_selectors:
        try:
            el = page.query_selector(sel)
            if el:
                el.click()
                clicked = True
                break
        except Exception:
            pass

    if clicked:
        page.wait_for_timeout(2000)
        pid = extract_profile_id_from_url(page.url)
        if pid:
            print(f"Found ID after click (URL): {pid}", flush=True)
            return pid

        # URLに出なくてもHTMLに出る場合がある
        try:
            html = page.content()
            m = PROFILE_ID_RE.search(html)
            if m:
                print(f"Found ID after click (HTML): {m.group(1)}", flush=True)
                return m.group(1)
        except Exception:
            pass

    # 3) 最後の手段：ページHTMLからIDらしきものを拾う
    try:
        html = page.content()
        m = PROFILE_ID_RE.search(html)
        if m:
            print(f"Found ID from HTML: {m.group(1)}", flush=True)
            return m.group(1)
    except Exception:
        pass

    print("No ID found", flush=True)
    return None


def get_urls_from_judobase(page, raw_name: str) -> Tuple[Optional[str], Optional[str]]:
    name = normalize_name(raw_name)

    # 2行目の "Name" はヘッダーっぽいので無視（任意）
    if name.lower() in ("name",):
        return None, None

    if not name:
        return None, None

    for q in build_queries(name):
        pid = judobase_search_id(page, q)
        if pid:
            r_url = f"https://judobase.ijf.org/#/competitor/profile/{pid}"
            q_url = f"https://www.ijf.org/judoka/{pid}"
            return q_url, r_url

        page.wait_for_timeout(600)

    return None, None
