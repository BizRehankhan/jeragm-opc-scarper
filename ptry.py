import time
import random
import requests

BASE_URL = "https://data.stats.gov.cn/dg/website/publicrelease/en/web/external"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*",
    "Referer": "https://data.stats.gov.cn/",
    "Content-Type": "application/json",
}
REQUEST_DELAY = 1.0

session = requests.Session()
session.headers.update(HEADERS)


# ─────────────────────────────────────────────
# PROXY SUPPORT
# ─────────────────────────────────────────────

PROXY_SOURCES = [
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
]

_proxy_pool: list = []


def fetch_free_proxies() -> list:
    """Fetch fresh proxy list from public sources."""
    proxies = []
    for url in PROXY_SOURCES:
        try:
            res = requests.get(url, timeout=10)
            lines = res.text.strip().splitlines()
            for line in lines:
                line = line.strip()
                if line and ":" in line:
                    proxies.append(line)
            print(f"  [proxy] fetched {len(lines)} from {url}")
        except Exception as e:
            print(f"  [proxy] failed {url}: {e}")
    unique = list(set(proxies))
    print(f"  [proxy] {len(unique)} unique proxies total")
    return unique


def test_proxy(proxy: str, timeout: int = 5) -> bool:
    """Check if proxy is alive."""
    try:
        res = requests.get(
            "https://httpbin.org/ip",
            proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
            timeout=timeout,
        )
        return res.status_code == 200
    except Exception:
        return False


def build_proxy_pool(max_working: int = 10) -> list:
    """Fetch + test proxies, keep only working ones."""
    global _proxy_pool
    print("[proxy] Building proxy pool...")
    candidates = fetch_free_proxies()
    random.shuffle(candidates)

    working = []
    for proxy in candidates:
        if len(working) >= max_working:
            break
        if test_proxy(proxy):
            working.append(proxy)
            print(f"  [proxy] ✓ {proxy}")
        else:
            print(f"  [proxy] ✗ {proxy}")

    _proxy_pool = working
    print(f"[proxy] Pool ready — {len(_proxy_pool)} working proxies\n")
    return _proxy_pool


def get_proxy():
    """Pick a random proxy dict, or None if pool is empty."""
    if not _proxy_pool:
        return None
    proxy = random.choice(_proxy_pool)
    return {"http": f"http://{proxy}", "https": f"http://{proxy}"}


def rotate_request(method: str, url: str, retries: int = 3, **kwargs) -> requests.Response:
    """
    Request with proxy rotation + automatic fallback to direct.
    Dead proxies are removed from pool on failure.
    """
    for attempt in range(retries):
        proxy = get_proxy()
        label = list(proxy.values())[0] if proxy else "direct"
        try:
            res = session.request(method, url, proxies=proxy, timeout=15, **kwargs)
            
            return res
        except Exception as e:
            print(f"  [proxy] attempt {attempt+1} failed ({label}): {e}")
            if proxy:
                dead = proxy["http"].replace("http://", "")
                if dead in _proxy_pool:
                    _proxy_pool.remove(dead)
                    print(f"  [proxy] removed {dead}, pool={len(_proxy_pool)}")

    # fallback: direct connection
    print("  [proxy] all proxies failed — using direct connection")
    res = session.request(method, url, timeout=15, **kwargs)
    
    return res


# ─────────────────────────────────────────────
# STEP 1: STATIC LEAF CATALOGS (hardcoded — won't change)
# ─────────────────────────────────────────────

def get_leaf_catalogs(pid: str = None) -> list[dict]:
    """
    Static list of all leaf catalogs under 'Output of Energy Products'.
    Source: queryIndexTreeAsync — data is stable, no API call needed.
    parent pid: f46bf43e25374f5b9e2181676d582356
    """
    return [
        {"id": "9a59860aa4a9442e91d9aa01762e3284",  "name": "Coal"},
        {"id": "ed34745091424311b9d62bbc16c1ea3b",  "name": "Crude oil"},
        {"id": "e32908a742c14e46a1525ab9549d9d6a",  "name": "Natural Gas"},
        {"id": "9d550c7d47144421b950ece73418c0a5",  "name": "Coalbed Gas"},
        {"id": "1a740c9519b54bcaac1db54804415356",  "name": "LNG"},
        {"id": "7da537ad56414da9a33427d3da95e434",  "name": "Processing Volume of Crude oil"},
        {"id": "4e5c1ce4ff36454a8a2358c3c6b049d4",  "name": "Gasoline"},
        {"id": "50d9afcb5bce42cb8def855131785cbe",  "name": "Kerosene"},
        {"id": "534e11f26f95480582fe7b15acd09b03",  "name": "Diesel Oil"},
        {"id": "1541c7cffca64ed1865643f76e84e429",  "name": "Fuel Oil"},
        {"id": "14104b0fe87c4cce955f03bba283cbfd",  "name": "Naphtha"},
        {"id": "aff1ae7bcd2e4c90a11f3dd1965bce9e",  "name": "LPG"},
        {"id": "efd6dbad356b4928a5527b42e7c1093d",  "name": "Petroleum Coke"},
        {"id": "336384a105dd4ed3975a4334c91c4b8c",  "name": "Asphalt"},
        {"id": "0e83c61d3fe642b28b6294afcea3039b",  "name": "Coke"},
        {"id": "f76c5af9a1604d1b906463b208bdd675",  "name": "Output of Electricity"},
        {"id": "c81899a0508d4fdc9ad5786ef7810cca",  "name": "Thermal Power"},
        {"id": "5a58168195604a348e6a5d909552187e",  "name": "Hydro-electric Power"},
        {"id": "c8afa9ab62994303932ec9eee0b8833a",  "name": "Nuclear Power"},
        {"id": "35d85b5392be4ce19cae8d6ae83932aa",  "name": "Wind Power"},
        {"id": "0262c099768642d5a5dcbb5928f7c910",  "name": "Solar Power"},
        {"id": "bb881d7174db4fcc81f52159c5f0e0af",  "name": "Gas"},
    ]


# ─────────────────────────────────────────────
# STEP 2+3: REGIONS — get all provinces
# ─────────────────────────────────────────────

def get_region_catalog_id(indicator_cid: str) -> str:
    """
    getDaCatalogTreeByIndicatorCid → find the 'National Total' catalog id
    which contains all provinces.
    """
    url = f"{BASE_URL}/getDaCatalogTreeByIndicatorCid"
    res = rotate_request("GET", url, params={"indicatorCid": indicator_cid})
    
    time.sleep(REQUEST_DELAY)

    data = res.json().get("data", [])
    for item in data:
        # 'National Total' is level 1, no children — use it for all provinces
        if "children" not in item:
            return item["_id"]
    # fallback: return first item
    return data[0]["_id"] if data else ""


def get_regions(da_cid: str) -> list[dict]:
    """getDasByDaCatalogId → list of {text, value} for all provinces."""
    url = f"{BASE_URL}/getDasByDaCatalogId"
    res = rotate_request("GET", url, params={"daCid": da_cid})
    
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
    res = rotate_request("GET", url, params={"cid": cid, "dt": dt, "name": ""})
    
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
    res = rotate_request("POST", url, json=payload)
    
    time.sleep(REQUEST_DELAY)
    return res.json()


# ─────────────────────────────────────────────
# FULL CRAWL
# ─────────────────────────────────────────────

def crawl(
    root_pid: str = "76e04d7533764d4384b0cd8d71deccbe",
    root_id:  str = "f4c6cd795fea436c807163397dd36b98",
    dt_range: str = "202403MM-202602MM",
    indicator_dt: str = "2025-2026",
    use_proxy: bool = False,
    max_proxies: int = 10,
):
    if use_proxy:
        build_proxy_pool(max_working=max_proxies)
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
            break
        except Exception as e:
            print(f"         ✗ Error: {e}")

        

    return all_results


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NBS China Energy Crawler")
    parser.add_argument("--proxy",       action="store_true", help="Use free proxy pool")
    parser.add_argument("--max-proxies", type=int, default=10, help="Max working proxies to test (default: 10)")
    parser.add_argument("--dt-range",    default="202403MM-202602MM", help="Date range e.g. 202403MM-202602MM")
    parser.add_argument("--indicator-dt",default="2025-2026",        help="Indicator discovery year range")
    args = parser.parse_args()

    results = crawl(
        root_pid     = "76e04d7533764d4384b0cd8d71deccbe",
        root_id      = "f4c6cd795fea436c807163397dd36b98",
        dt_range     = args.dt_range,
        indicator_dt = args.indicator_dt,
        use_proxy    = args.proxy,
        max_proxies  = args.max_proxies,
    )
    print(f"\n✓ Done. {len(results)} catalogs crawled.")
