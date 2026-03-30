"""
crawler.py — Scraper for data.stats.gov.cn public release API.

Pipeline:
  1. Walk catalog tree → leaf nodes
  2. Resolve region catalog ID per leaf
  3. Fetch province list
  4. Fetch indicator list
  5. POST for time-series data
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.exceptions import HTTPError, RequestException, Timeout

from proxy import clear_pool, get_proxy
from log_config import setup_logging

setup_logging("DEBUG")

__all__ = [
    "crawl",
    "fetch_data",
    "get_indicators",
    "get_leaf_catalogs",
    "get_region_catalog_id",
    "get_regions",
    "single_energy_record",
    "SingleEnergyResult",
]

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL = "https://data.stats.gov.cn/dg/website/publicrelease/en/web/external"
HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*",
    "Referer": "https://data.stats.gov.cn/",
    "Content-Type": "application/json",
}

REQUEST_DELAY: float = 1.0
MAX_RETRIES: int = 3
BASE_TIMEOUT: int = 30
MAX_TREE_DEPTH: int = 20

DEFAULT_ROOT_PID: str = "76e04d7533764d4384b0cd8d71deccbe"
DEFAULT_ROOT_ID: str = "f4c6cd795fea436c807163397dd36b98"
DEFAULT_DT_RANGE: str = "202403MM-202602MM"
DEFAULT_SINGLE_DT_RANGE: str = "202508MM-202602MM"
DEFAULT_INDICATOR_DT: str = "2025-2026"

# Regex: YYYYMMTT-YYYYMMTT where TT is a 2-letter frequency code (MM=monthly, QQ=quarterly, etc.)
_DT_RANGE_RE = re.compile(r"^\d{4}\d{2}[A-Z]{2}-\d{4}\d{2}[A-Z]{2}$")


# ── Data models ────────────────────────────────────────────────────────────────
@dataclass
class Indicator:
    id: str
    name: str
    unit: str


@dataclass
class Region:
    text: str
    value: str


@dataclass
class CatalogResult:
    cid: str
    indicators: list[Indicator]
    regions: list[Region]
    raw: dict[str, Any]


@dataclass
class SingleEnergyResult:
    cid: str
    da_cid: str
    indicator: Indicator
    regions: list[Region]
    dt_range: str
    raw: dict[str, Any]


@dataclass
class LeafCatalog:
    id: str
    name: str
    pid: str


# ── Validators ────────────────────────────────────────────────────────────────
def _validate_dt_range(dt_range: str) -> None:
    """
    Validate dt_range format: YYYYMMTT-YYYYMMTT
      YYYY = 4-digit year
      MM   = 2-digit month (01-12)
      TT   = 2-letter frequency code (MM=monthly, QQ=quarterly, YY=annual)

    Valid examples:   "202403MM-202602MM", "202401QQ-202604QQ"
    Invalid examples: "2024-03-MM", "20243MM-20262MM", "202413MM-202602MM"
    """
    if not _DT_RANGE_RE.match(dt_range):
        raise ValueError(
            f"Invalid dt_range format {dt_range!r}. "
            "Expected YYYYMMTT-YYYYMMTT (e.g. '202403MM-202602MM')."
        )

    start, end = dt_range.split("-")
    start_year, start_month = int(start[:4]), int(start[4:6])
    end_year, end_month = int(end[:4]), int(end[4:6])

    if not (1 <= start_month <= 12):
        raise ValueError(f"Invalid start month {start_month} in dt_range {dt_range!r}")
    if not (1 <= end_month <= 12):
        raise ValueError(f"Invalid end month {end_month} in dt_range {dt_range!r}")

    start_tt, end_tt = start[6:], end[6:]
    if start_tt != end_tt:
        raise ValueError(
            f"Frequency code mismatch in dt_range {dt_range!r}: "
            f"{start_tt!r} vs {end_tt!r} — both sides must use the same code."
        )

    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(
            f"dt_range start {start[:6]} is after end {end[:6]} in {dt_range!r}"
        )


# ── HTTP layer ─────────────────────────────────────────────────────────────────
def _request(
    method: str,
    url: str,
    use_proxy: bool = False,
    retries: int = MAX_RETRIES,
    **kwargs: Any,
) -> requests.Response:
    """Retry-aware HTTP request with exponential back-off."""
    last_err: RequestException | None = None

    for attempt in range(retries):
        try:
            proxy = get_proxy() if use_proxy else None
            timeout = BASE_TIMEOUT * (attempt + 1)
            resp = requests.request(
                method, url, headers=HEADERS, proxies=proxy, timeout=timeout, **kwargs
            )
            resp.raise_for_status()
            return resp
        except (HTTPError, Timeout, RequestException) as exc:
            last_err = exc
            logger.warning(
                "Request failed (attempt %d/%d): %s — %s",
                attempt + 1,
                retries,
                url,
                exc,
            )
            if use_proxy:
                clear_pool()
            time.sleep(2**attempt)

    raise last_err  # type: ignore[misc]


def _get_json(url: str, params: dict[str, Any], use_proxy: bool) -> Any:
    """GET → parse JSON, apply rate-limit delay."""
    resp = _request("GET", url, use_proxy=use_proxy, params=params)
    time.sleep(REQUEST_DELAY)
    return resp.json()


# ── Step 1: Catalog tree ───────────────────────────────────────────────────────
def get_tree(pid: str, code: int = 4, use_proxy: bool = False) -> list[dict]:
    data = _get_json(
        f"{BASE_URL}/new/queryIndexTreeAsync",
        {"pid": pid, "code": code},
        use_proxy,
    )
    return data.get("data", [])


def get_leaf_catalogs(
    pid: str,
    use_proxy: bool = False,
    _depth: int = 0,
) -> list[LeafCatalog]:
    """Recursively walk tree until isLeaf=True, guarded by MAX_TREE_DEPTH."""
    if _depth > MAX_TREE_DEPTH:
        logger.error("Max recursion depth reached at pid=%s", pid)
        return []

    nodes = get_tree(pid, use_proxy=use_proxy)
    leaves: list[LeafCatalog] = []

    for node in nodes:
        if node.get("isLeaf"):
            leaves.append(
                LeafCatalog(
                    id=node["_id"],
                    name=node["name"],
                    pid=node["treeinfo_pid"],
                )
            )
        else:
            leaves.extend(
                get_leaf_catalogs(node["_id"], use_proxy=use_proxy, _depth=_depth + 1)
            )

    return leaves


# ── Step 2+3: Regions ──────────────────────────────────────────────────────────
def get_region_catalog_id(indicator_cid: str, use_proxy: bool = False) -> str:
    """Return the 'National Total' catalog ID (leaf-level, no children)."""
    data: list[dict] = _get_json(
        f"{BASE_URL}/getDaCatalogTreeByIndicatorCid",
        {"indicatorCid": indicator_cid},
        use_proxy,
    ).get("data", [])

    for item in data:
        if "children" not in item:
            return item["_id"]

    if data:
        logger.warning("No childless item found for cid=%s; using first item", indicator_cid)
        return data[0]["_id"]

    return ""


def get_regions(da_cid: str, use_proxy: bool = False) -> list[Region]:
    """Return province list for a given data-catalog ID."""
    items: list[dict] = _get_json(
        f"{BASE_URL}/getDasByDaCatalogId",
        {"daCid": da_cid},
        use_proxy,
    ).get("data", [])

    return [Region(text=item["show_name"], value=item["name_value"]) for item in items]


# ── Step 4: Indicators ─────────────────────────────────────────────────────────
def get_indicators(
    cid: str,
    dt: str = DEFAULT_INDICATOR_DT,
    use_proxy: bool = False,
) -> list[Indicator]:
    """Return indicators for a catalog node."""
    items: list[dict] = (
        _get_json(
            f"{BASE_URL}/new/queryIndicatorsByCid",
            {"cid": cid, "dt": dt, "name": ""},
            use_proxy,
        )
        .get("data", {})
        .get("list", [])
    )

    return [
        Indicator(
            id=item["_id"],
            name=item["i_showname"].strip(),
            unit=item.get("du_name", ""),
        )
        for item in items
    ]


# ── Step 5: Fetch data ─────────────────────────────────────────────────────────
def fetch_data(
    cid: str,
    indicator_ids: list[str],
    regions: list[Region],
    dts: list[str],
    root_id: str = DEFAULT_ROOT_ID,
    use_proxy: bool = False,
) -> dict[str, Any]:
    """POST time-series data for given indicators, regions, and date range.

    dts format: ["202403MM-202602MM"]  (YYYYMMTT range, TT=MM for monthly)
    """
    for dt in dts:
        _validate_dt_range(dt)
    payload = {
        "cid": cid,
        "indicatorIds": indicator_ids,
        "daCatalogId": "",
        "das": [{"text": r.text, "value": r.value} for r in regions],
        "showType": 3,
        "dts": dts,
        "rootId": root_id,
    }
    resp = _request("POST", f"{BASE_URL}/getEsDataByCidAndDt", use_proxy=use_proxy, json=payload)
    time.sleep(REQUEST_DELAY)
    return resp.json()


# ── Convenience helper ─────────────────────────────────────────────────────────
def single_energy_record(
    cid: str,
    indicator_type: int = 0,
    dt_range: str = DEFAULT_SINGLE_DT_RANGE,
    use_proxy: bool = False,
) -> SingleEnergyResult:
    """
    Fetch time-series data for one indicator of a catalog node.

    Pipeline:
      1. Resolve region-catalog ID from indicator CID
      2. Fetch province list for that region catalog
      3. Fetch all indicators for the CID
      4. Select indicator at `indicator_type` index
      5. POST data for selected indicator + all regions + dt_range

    Args:
        cid:            Catalog node ID.
        indicator_type: Index into the indicator list (default 0 = first).
        dt_range:       Date range string, e.g. "202508MM-202602MM".
        use_proxy:      Route requests through proxy pool.

    Returns:
        SingleEnergyResult with all resolved context + raw API response.

    Raises:
        ValueError:  No indicators found for the given cid.
        IndexError:  indicator_type exceeds available indicators.
        RequestException: Network/HTTP failure after all retries.
    """
    logger.info("[single_energy_record] START cid=%s indicator_type=%d dt_range=%s", cid, indicator_type, dt_range)

    logger.info("[1/4] Resolving region catalog ID for cid=%s", cid)
    da_cid = get_region_catalog_id(cid, use_proxy=use_proxy)
    if not da_cid:
        raise ValueError(f"Could not resolve region catalog ID for cid={cid!r}")
    logger.info("      da_cid=%s", da_cid)

    logger.info("[2/4] Fetching regions for da_cid=%s", da_cid)
    regions = get_regions(da_cid, use_proxy=use_proxy)
    if not regions:
        raise ValueError(f"No regions returned for da_cid={da_cid!r}")
    logger.info("      %d regions: %s", len(regions), [r.text for r in regions])

    logger.info("[3/4] Fetching indicators for cid=%s", cid)
    indicators = get_indicators(cid, use_proxy=use_proxy)
    if not indicators:
        raise ValueError(f"No indicators found for cid={cid!r}")
    logger.info("      %d indicators available: %s", len(indicators), [i.name for i in indicators])

    if indicator_type >= len(indicators):
        raise IndexError(
            f"indicator_type={indicator_type} out of range — "
            f"only {len(indicators)} indicator(s) available: "
            f"{[i.name for i in indicators]}"
        )

    chosen = indicators[indicator_type]
    logger.info("      Selected [%d] name=%r unit=%r id=%s", indicator_type, chosen.name, chosen.unit, chosen.id)

    logger.info("[4/4] Fetching data — indicator=%r regions=%d dt_range=%s", chosen.name, len(regions), dt_range)
    raw = fetch_data(cid, [chosen.id], regions, [dt_range], use_proxy=use_proxy)
    logger.info("[single_energy_record] DONE cid=%s", cid)

    return SingleEnergyResult(
        cid=cid,
        da_cid=da_cid,
        indicator=chosen,
        regions=regions,
        dt_range=dt_range,
        raw=raw,
    )


# ── Full crawl ─────────────────────────────────────────────────────────────────
def crawl(
    root_pid: str = DEFAULT_ROOT_PID,
    root_id: str = DEFAULT_ROOT_ID,
    dt_range: str = DEFAULT_DT_RANGE,
    indicator_dt: str = DEFAULT_INDICATOR_DT,
) -> dict[str, CatalogResult]:
    """Walk the full catalog tree and fetch all time-series data."""
    logger.info("Discovering leaf catalogs from pid=%s", root_pid)
    leaves = get_leaf_catalogs(root_pid)
    logger.info("Found %d leaf catalogs", len(leaves))

    all_results: dict[str, CatalogResult] = {}
    total = len(leaves)

    for i, leaf in enumerate(leaves, 1):
        cid, name = leaf.id, leaf.name
        logger.info("[%d/%d] %s (cid=%s)", i, total, name, cid)

        da_cid = get_region_catalog_id(cid)
        regions = get_regions(da_cid)
        logger.debug("  %d regions", len(regions))

        indicators = get_indicators(cid, dt=indicator_dt)
        logger.debug("  %d indicators: %s", len(indicators), [x.name for x in indicators])

        if not indicators:
            logger.warning("  No indicators for cid=%s — skipping", cid)
            continue

        try:
            raw = fetch_data(
                cid=cid,
                indicator_ids=[x.id for x in indicators],
                regions=regions,
                dts=[dt_range],
                root_id=root_id,
            )
            all_results[name] = CatalogResult(
                cid=cid,
                indicators=indicators,
                regions=regions,
                raw=raw,
            )
            logger.info("  ✓ data fetched for %s", name)
        except RequestException as exc:
            logger.error("  ✗ Failed to fetch data for %s: %s", name, exc)

    return all_results
