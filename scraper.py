import os
import asyncio
import aiohttp
import requests
import pandas as pd
import re
import hashlib
from bs4 import BeautifulSoup
from typing import Any, cast, List, Dict
from dotenv import load_dotenv
from supabase import create_client, Client

BASE = "https://stateexcise.assam.gov.in/index.php"
PRICE_PAGE = f"{BASE}/PriceList/priceListView"
SEGMENT_API = f"{BASE}/PriceList/getSegmentDetails"
PRICE_API = f"{BASE}/PriceList/getPriceList"
FINANCIAL_YEAR = "2025-2026"
LIQUOR_TYPES = ["IML", "FL", "BEER", "CS", "HL"]
MAX_CONCURRENT_REQUESTS = 10

load_dotenv()


def hash_id(text):
    return hashlib.md5(text.encode()).hexdigest()


def parse_pack(pack):
    m = re.search(r"(\d+)\s*X\s*(\d+)", pack)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def normalize_company(name):
    name = name.upper()
    name = re.sub(r"TIE-UP HOLDER.*", "", name)
    name = re.sub(r"PVT\.? LTD\.?", "", name)
    return name.strip()


def infer_strength(segment):
    spirit_segments = ["Y", "G", "L", "R", "S", "P", "T", "V", "W"]
    if segment in spirit_segments:
        return 42.8
    if segment in ["B", "M"]:
        return 5
    if segment == "E":
        return 12
    return None


def get_csrf(session):
    r = session.get(PRICE_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if name and str(name).startswith("csrf"):
            return str(name), str(inp.get("value"))
    raise Exception("CSRF token not found")


def get_segments(session, csrf_name, csrf_value, liquor_type):
    payload = {"type": liquor_type, csrf_name: csrf_value}
    headers = {"X-Requested-With": "XMLHttpRequest", "Referer": PRICE_PAGE}
    r = session.post(SEGMENT_API, data=payload, headers=headers)
    data = r.json()
    segments = []
    for item in data:
        segments.append(
            {"code": item["CODE"], "name": item["NAME"], "liquor_type": liquor_type}
        )
    return segments


async def get_prices_async(
    session, sem, csrf_name, csrf_value, liquor_type, segment_code, segment_name
):
    payload = {
        "segment": segment_code,
        "financialYear": FINANCIAL_YEAR,
        "acttype": "list",
        "liquorType": liquor_type,
        csrf_name: csrf_value,
    }
    headers = {"X-Requested-With": "XMLHttpRequest", "Referer": PRICE_PAGE}
    async with sem:
        async with session.post(PRICE_API, data=payload, headers=headers) as r:
            text = await r.text()
    soup = BeautifulSoup(text, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cols) >= 7:
            try:
                mrp_val = float(cols[6])
            except:
                continue
            if mrp_val == 0:
                continue
            rows.append(
                {
                    "supplier": cols[1],
                    "category": cols[3],
                    "brand_name": cols[4],
                    "pack_size": cols[5],
                    "mrp": mrp_val,
                    "segment_code": segment_code,
                    "segment_name": segment_name,
                    "liquor_type": liquor_type,
                }
            )
    return rows


def normalize(products):
    companies = {}
    brands = {}
    variants = []
    for _, row in products.iterrows():
        company = normalize_company(row["supplier"])
        if company not in companies:
            companies[company] = {"id": hash_id(company), "name": company}
        company_id = companies[company]["id"]
        brand_key = company + row["brand_name"]
        if brand_key not in brands:
            brands[brand_key] = {
                "id": hash_id(brand_key),
                "company_id": company_id,
                "brand_name": row["brand_name"],
                "category": row["category"],
            }
        brand_id = brands[brand_key]["id"]
        size_ml, case_size = parse_pack(row["pack_size"])
        strength = infer_strength(row["segment_code"])
        variants.append(
            {
                "id": hash_id(brand_key + str(size_ml)),
                "brand_id": brand_id,
                "size_ml": size_ml,
                "case_size": case_size,
                "mrp": row["mrp"],
                "liquor_type": row["liquor_type"],
                "segment_code": row["segment_code"],
                "segment_name": row["segment_name"],
                "strength_percent": strength,
            }
        )
    variants_df = pd.DataFrame(variants).drop_duplicates()
    return (
        pd.DataFrame(companies.values()),
        pd.DataFrame(brands.values()),
        variants_df,
    )


def push_to_supabase(companies_df, brands_df, variants_df):
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Supabase credentials missing")
    supabase: Client = create_client(url, key)
    companies_df = companies_df.drop_duplicates(subset=["id"])
    brands_df = brands_df.drop_duplicates(subset=["id"])
    variants_df = variants_df.drop_duplicates(subset=["id"])

    def clean_records(df):
        records: List[Dict[str, Any]] = [
            {k: (None if pd.isna(v) else v) for k, v in row.items()}
            for row in df.to_dict(orient="records")
        ]
        return cast(Any, records)

    comp_res = supabase.table("global_companies").select("id").limit(10000).execute()
    existing_companies = {
        str(row["id"]) for row in cast(List[Dict[str, Any]], comp_res.data)
    }
    new_companies = companies_df[~companies_df["id"].isin(existing_companies)]
    if not new_companies.empty:
        supabase.table("global_companies").upsert(
            clean_records(new_companies)
        ).execute()
    brand_res = supabase.table("global_brands").select("id").limit(10000).execute()
    existing_brands = {
        str(row["id"]) for row in cast(List[Dict[str, Any]], brand_res.data)
    }
    new_brands = brands_df[~brands_df["id"].isin(existing_brands)]
    if not new_brands.empty:
        supabase.table("global_brands").upsert(clean_records(new_brands)).execute()
    var_res = supabase.table("global_variants").select("id, mrp").limit(10000).execute()
    existing_variants = {
        str(row["id"]): row for row in cast(List[Dict[str, Any]], var_res.data)
    }
    variants_to_push = []
    for row in variants_df.to_dict(orient="records"):
        vid = str(row["id"])
        if vid not in existing_variants:
            variants_to_push.append(row)
        else:
            old_row = cast(Dict[str, Any], existing_variants[vid])
            if old_row.get("mrp") != row.get("mrp"):
                variants_to_push.append(row)
    if variants_to_push:
        supabase.table("global_variants").upsert(
            clean_records(pd.DataFrame(variants_to_push))
        ).execute()
    print("Supabase sync complete")


async def main_async():
    session = requests.Session()
    csrf_name, csrf_value = get_csrf(session)
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []
    async with aiohttp.ClientSession() as async_session:
        for liquor_type in LIQUOR_TYPES:
            segments = get_segments(session, csrf_name, csrf_value, liquor_type)
            print(liquor_type, "segments:", len(segments))
            for seg in segments:
                tasks.append(
                    get_prices_async(
                        async_session,
                        sem,
                        csrf_name,
                        csrf_value,
                        liquor_type,
                        seg["code"],
                        seg["name"],
                    )
                )
        results = await asyncio.gather(*tasks)
    all_rows = []
    for r in results:
        all_rows.extend(r)
    products = pd.DataFrame(all_rows)
    print("Products scraped:", len(products))
    products.to_csv("products_raw.csv", index=False)
    companies, brands, variants = normalize(products)
    push_to_supabase(companies, brands, variants)
    print("Done — normalized datasets pushed to Supabase!")


if __name__ == "__main__":
    asyncio.run(main_async())
