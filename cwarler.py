import time
import requests

BASE_URL = "https://data.stats.gov.cn/dg/website/publicrelease/en/web/external"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*",
    "Referer": "https://data.stats.gov.cn/",
    "Content-Type": "application/json",
}
REQUEST_DELAY = 1.0

session = requests.Session()
session.headers.update(HEADERS)


# ─────────────────────────────────────────────
# STEP 1: TREE — get all leaf catalog nodes
# ─────────────────────────────────────────────

def get_tree(pid: str, code: int = 4) -> list:
    url = f"{BASE_URL}/new/queryIndexTreeAsync"
    res = session.get(url, params={"pid": pid, "code": code})
    res.raise_for_status()
    time.sleep(REQUEST_DELAY)
    return res.json().get("data", [])


def get_leaf_catalogs(pid: str) -> list[dict]:
    """Recursively walk tree until isLeaf=True."""
    nodes = get_tree(pid)
    leaves = []
    for node in nodes:
        if node.get("isLeaf"):
            leaves.append({
                "id":   node["_id"],
                "name": node["name"],
                "pid":  node["treeinfo_pid"],
            })
        else:
            leaves.extend(get_leaf_catalogs(node["_id"]))
    return leaves


# ─────────────────────────────────────────────
# STEP 2+3: REGIONS — get all provinces
# ─────────────────────────────────────────────

def get_region_catalog_id(indicator_cid: str) -> str:
    """
    getDaCatalogTreeByIndicatorCid → find the 'National Total' catalog id
    which contains all provinces.
    """
    url = f"{BASE_URL}/getDaCatalogTreeByIndicatorCid"
    res = session.get(url, params={"indicatorCid": indicator_cid})
    res.raise_for_status()
    time.sleep(REQUEST_DELAY)

    data = res.json().get("data", [])
    print(data)
    for item in data:
        # 'National Total' is level 1, no children — use it for all provinces
        if "children" not in item:
            return item["_id"]
    # fallback: return first item
    return data[0]["_id"] if data else ""


def get_regions(da_cid: str) -> list[dict]:
    """getDasByDaCatalogId → list of {text, value} for all provinces."""
    url = f"{BASE_URL}/getDasByDaCatalogId"
    res = session.get(url, params={"daCid": da_cid})
    res.raise_for_status()
    time.sleep(REQUEST_DELAY)

    return [
        {"text": item["show_name"], "value": item["name_value"]}
        for item in res.json().get("data", [])
    ]


# ─────────────────────────────────────────────
# STEP 4: INDICATORS — get indicator IDs for a catalog
# ─────────────────────────────────────────────

def get_indicators(cid: str, dt: str = "2025-2026") -> list[dict]:
    """
    queryIndicatorsByCid → list of indicators with their _id fields.
    Returns: [{"id": "...", "name": "...", "unit": "..."}]
    """
    url = f"{BASE_URL}/new/queryIndicatorsByCid"
    res = session.get(url, params={"cid": cid, "dt": dt, "name": ""})
    res.raise_for_status()
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
    res = session.post(url, json=payload)
    res.raise_for_status()
    time.sleep(REQUEST_DELAY)
    return res.json()


# ─────────────────────────────────────────────
# Single Energy Recode
# ─────────────────────────────────────────────

def single_energy_record(cid):
    da_cid     = get_region_catalog_id(cid)
    regions    = get_regions(da_cid)
    indicators = get_indicators(cid)
    raw        = fetch_data(cid, [indicators[0]["id"]], regions, ["202503MM-202602MM"])    
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

