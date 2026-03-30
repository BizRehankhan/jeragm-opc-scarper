import time
import requests
from proxy import get_proxy, clear_pool

BASE_URL = "https://data.stats.gov.cn/dg/website/publicrelease/en/web/external"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*",
    "Referer": "https://data.stats.gov.cn/",
    "Content-Type": "application/json",
}
REQUEST_DELAY = 1.0


def _request(method: str, url: str, use_proxy: bool = False, retries: int = 3, **kwargs):
    last_err = None
    for attempt in range(retries):
        try:
            proxy = get_proxy() if use_proxy else None
            timeout = 30 * (attempt + 1)
            resp = requests.request(method, url, headers=HEADERS, proxies=proxy, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            if use_proxy:
                clear_pool()
            time.sleep(2 ** attempt)
    raise last_err


session = requests.Session()
session.headers.update(HEADERS)


# ─────────────────────────────────────────────
# STEP 1: TREE — get all leaf catalog nodes
# ─────────────────────────────────────────────

def get_tree(pid: str, code: int = 4, use_proxy: bool = False) -> list:
    url = f"{BASE_URL}/new/queryIndexTreeAsync"
    res = _request("GET", url, use_proxy=use_proxy, params={"pid": pid, "code": code})
    time.sleep(REQUEST_DELAY)
    return res.json().get("data", [])


def get_leaf_catalogs(pid: str, use_proxy: bool = False) -> list[dict]:
    """Recursively walk tree until isLeaf=True."""
    nodes = get_tree(pid, use_proxy=use_proxy)
    leaves = []
    for node in nodes:
        if node.get("isLeaf"):
            leaves.append({
                "id":   node["_id"],
                "name": node["name"],
                "pid":  node["treeinfo_pid"],
            })
        else:
            leaves.extend(get_leaf_catalogs(node["_id"], use_proxy=use_proxy))
    return leaves


# ─────────────────────────────────────────────
# STEP 2+3: REGIONS — get all provinces
# ─────────────────────────────────────────────

def get_region_catalog_id(indicator_cid: str, use_proxy: bool = False) -> str:
    """
    getDaCatalogTreeByIndicatorCid → find the 'National Total' catalog id
    which contains all provinces.
    """
    url = f"{BASE_URL}/getDaCatalogTreeByIndicatorCid"
    res = _request("GET", url, use_proxy=use_proxy, params={"indicatorCid": indicator_cid})
    time.sleep(REQUEST_DELAY)

    data = res.json().get("data", [])
    print(data)
    for item in data:
        # 'National Total' is level 1, no children — use it for all provinces
        if "children" not in item:
            return item["_id"]
    # fallback: return first item
    return data[0]["_id"] if data else ""


def get_regions(da_cid: str, use_proxy: bool = False) -> list[dict]:
    """getDasByDaCatalogId → list of {text, value} for all provinces."""
    url = f"{BASE_URL}/getDasByDaCatalogId"
    res = _request("GET", url, use_proxy=use_proxy, params={"daCid": da_cid})
    time.sleep(REQUEST_DELAY)

    return [
        {"text": item["show_name"], "value": item["name_value"]}
        for item in res.json().get("data", [])
    ]


# ─────────────────────────────────────────────
# STEP 4: INDICATORS — get indicator IDs for a catalog
# ─────────────────────────────────────────────

def get_indicators(cid: str, dt: str = "2025-2026", use_proxy: bool = False) -> list[dict]:
    """
    queryIndicatorsByCid → list of indicators with their _id fields.
    Returns: [{"id": "...", "name": "...", "unit": "..."}]
    """
    url = f"{BASE_URL}/new/queryIndicatorsByCid"
    res = _request("GET", url, use_proxy=use_proxy, params={"cid": cid, "dt": dt, "name": ""})
    time.sleep(REQUEST_DELAY)

    items = res.json().get("data", {}).get("list", [])
    return [
        {
            "id":   item["_id"],
            "name": item["i_showname"].strip(),
            "unit": item.get("du_name", ""),
        }
        for item in items
    ]


# ─────────────────────────────────────────────
# STEP 5: FETCH DATA
# ─────────────────────────────────────────────

def fetch_data(
    cid: str,
    indicator_ids: list[str],
    regions: list[dict],
    dts: list[str],
    root_id: str = "f4c6cd795fea436c807163397dd36b98",
    use_proxy: bool = False,
) -> dict:
    """
    POST getEsDataByCidAndDt → actual time-series data.

    dts format: ["202403MM-202602MM"]  (YYYYMMTT range, TT=MM for monthly)
    """
    url = f"{BASE_URL}/getEsDataByCidAndDt"
    payload = {
        "cid":          cid,
        "indicatorIds": indicator_ids,
        "daCatalogId":  "",
        "das":          regions,
        "showType":     3,
        "dts":          dts,
        "rootId":       root_id,
    }
    res = _request("POST", url, use_proxy=use_proxy, json=payload)
    time.sleep(REQUEST_DELAY)
    return res.json()


# ─────────────────────────────────────────────
# Single Energy Recode
# ─────────────────────────────────────────────

def single_energy_record(cid, indicator_type=0, use_proxy: bool = False):
    da_cid     = get_region_catalog_id(cid, use_proxy=use_proxy)
    regions    = get_regions(da_cid, use_proxy=use_proxy)
    indicators = get_indicators(cid, use_proxy=use_proxy)
    raw        = fetch_data(cid, [indicators[indicator_type]["id"]], regions, ["202508MM-202602MM"], use_proxy=use_proxy)
    return raw


# ─────────────────────────────────────────────
# FULL CRAWL
# ─────────────────────────────────────────────

def crawl(
    root_pid: str = "76e04d7533764d4384b0cd8d71deccbe",
    root_id:  str = "f4c6cd795fea436c807163397dd36b98",
    dt_range: str = "202403MM-202602MM",
    indicator_dt: str = "2025-2026",
):
    print("[1/4] Discovering leaf catalogs...")
    leaves = get_leaf_catalogs(root_pid)
    print(f"      Found {len(leaves)} leaf catalogs")

    all_results = {}

    for i, leaf in enumerate(leaves, 1):
        cid  = leaf["id"]
        name = leaf["name"]
        print(f"\n[{i}/{len(leaves)}] {name} (cid={cid})")

        # Step 2+3: regions
        print("      → fetching regions...")
        da_cid  = get_region_catalog_id(cid)
        regions = get_regions(da_cid)
        print(f"         {len(regions)} regions")

        # Step 4: indicators
        print("      → fetching indicators...")
        indicators = get_indicators(cid, dt=indicator_dt)
        print(f"         {len(indicators)} indicators: {[x['name'] for x in indicators]}")

        if not indicators:
            print("         ⚠ no indicators found, skipping")
            continue

        indicator_ids = [x["id"] for x in indicators]

        # Step 5: data
        print(f"      → fetching data ({dt_range})...")
        try:
            raw = fetch_data(
                cid=cid,
                indicator_ids=indicator_ids,
                regions=regions,
                dts=[dt_range],
                root_id=root_id,
            )
            all_results[name] = {
                "cid":        cid,
                "indicators": indicators,
                "regions":    regions,
                "raw":        raw,
            }
            print(f"         ✓ data fetched")
        except Exception as e:
            print(f"         ✗ Error: {e}")

    return all_results

