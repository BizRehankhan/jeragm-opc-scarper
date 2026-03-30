import random
import requests

_proxy_pool: list[str] = []


def load_proxies() -> list[str]:
    try:
        resp = requests.get(
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
            timeout=10,
        )
        return [f"http://{line.strip()}" for line in resp.text.strip().splitlines() if line.strip()]
    except Exception:
        return []


def get_proxy() -> dict | None:
    global _proxy_pool
    if not _proxy_pool:
        _proxy_pool = load_proxies()
    if not _proxy_pool:
        return None
    p = random.choice(_proxy_pool)
    return {"http": p, "https": p}


def clear_pool():
    _proxy_pool.clear()
