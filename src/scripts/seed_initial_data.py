import os
from typing import List, Dict
from supabase import create_client


def get_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY")
    return create_client(url, key)


def upsert_exchange_rates(client, rows: List[Dict]):
    for r in rows:
        client.table("exchange_rates").upsert(r, on_conflict="currency").execute()


def upsert_sites(client, rows: List[Dict]):
    for r in rows:
        client.table("sites").upsert(r, on_conflict="domain").execute()


def main():
    client = get_client()

    exchange_rates = [
        {"currency": "USD", "rate_to_usd": 1.0},
        {"currency": "EUR", "rate_to_usd": 1.08},
        {"currency": "GBP", "rate_to_usd": 1.27},
        {"currency": "JPY", "rate_to_usd": 0.0069},
        {"currency": "CNY", "rate_to_usd": 0.14},
        {"currency": "CAD", "rate_to_usd": 0.73},
        {"currency": "AUD", "rate_to_usd": 0.66},
        {"currency": "INR", "rate_to_usd": 0.012},
        {"currency": "CHF", "rate_to_usd": 1.11},
    ]

    sites = [
        {"domain": "www.amazon.com", "name": "Amazon US", "region_code": "US", "currency": "USD"},
        {"domain": "www.amazon.co.uk", "name": "Amazon UK", "region_code": "GB", "currency": "GBP"},
        {"domain": "www.amazon.de", "name": "Amazon DE", "region_code": "DE", "currency": "EUR"},
        {"domain": "www.amazon.fr", "name": "Amazon FR", "region_code": "FR", "currency": "EUR"},
        {"domain": "www.amazon.it", "name": "Amazon IT", "region_code": "IT", "currency": "EUR"},
        {"domain": "www.amazon.es", "name": "Amazon ES", "region_code": "ES", "currency": "EUR"},
        {"domain": "www.amazon.ca", "name": "Amazon CA", "region_code": "CA", "currency": "CAD"},
        {"domain": "www.amazon.co.jp", "name": "Amazon JP", "region_code": "JP", "currency": "JPY"},
        {"domain": "www.amazon.com.au", "name": "Amazon AU", "region_code": "AU", "currency": "AUD"},
        {"domain": "www.amazon.in", "name": "Amazon IN", "region_code": "IN", "currency": "INR"},
        {"domain": "www.amazon.cn", "name": "Amazon CN", "region_code": "CN", "currency": "CNY"},
    ]

    upsert_exchange_rates(client, exchange_rates)
    upsert_sites(client, sites)


if __name__ == "__main__":
    main()

