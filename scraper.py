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

# CRITICAL FIX: Lowered concurrency to avoid triggering the state firewall
MAX_CONCURRENT_REQUESTS = 2

# CRITICAL FIX: Added a standard browser User-Agent
STD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

load_dotenv()


def hash_id(text):
    return hashlib.md5(text.encode()).hexdigest()


def parse_pack(pack):
    m = re.search(r"(\d+)\s*[xX]\s*(\d+)", pack)
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
    headers = {
        **STD_HEADERS,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PRICE_PAGE,
    }
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
    headers = {
        **STD_HEADERS,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PRICE_PAGE,
    }

    # CRITICAL FIX: Robust retry logic for fragile servers
    max_retries = 3
    text = ""
    for attempt in range(max_retries):
        try:
            async with sem:
                # Be polite to the server
                await asyncio.sleep(0.5)
                async with session.post(PRICE_API, data=payload, headers=headers) as r:
                    r.raise_for_status()
                    text = await r.text()
                    break  # Success! Exit the retry loop
        except Exception as e:
            if attempt == max_retries - 1:
                print(
                    f"Failed to fetch {liquor_type} - {segment_name} after {max_retries} attempts: {e}"
                )
                return []
            print(
                f"Timeout on {segment_name}, retrying... ({attempt + 1}/{max_retries})"
            )
            await asyncio.sleep(2)  # Wait a bit before retrying

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

    print("Pushing Companies...")
    comp_res = supabase.table("global_companies").select("id").limit(10000).execute()
    existing_companies = {
        str(row["id"]) for row in cast(List[Dict[str, Any]], comp_res.data)
    }
    new_companies = companies_df[~companies_df["id"].isin(existing_companies)]
    if not new_companies.empty:
        supabase.table("global_companies").upsert(
            clean_records(new_companies)
        ).execute()

    print("Pushing Brands...")
    brand_res = supabase.table("global_brands").select("id").limit(10000).execute()
    existing_brands = {
        str(row["id"]) for row in cast(List[Dict[str, Any]], brand_res.data)
    }
    new_brands = brands_df[~brands_df["id"].isin(existing_brands)]
    if not new_brands.empty:
        supabase.table("global_brands").upsert(clean_records(new_brands)).execute()

    print("Pushing Variants...")
    var_res = (
        supabase.table("global_variants")
        .select("id, mrp, mapped_barcode")
        .limit(10000)
        .execute()
    )
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
            if old_row.get("mrp") != row.get("mrp") or old_row.get(
                "mapped_barcode"
            ) != row.get("mapped_barcode"):
                row["mapped_barcode"] = old_row.get("mapped_barcode")
                variants_to_push.append(row)

    if variants_to_push:
        # Pushing in smaller chunks just in case Supabase complains about payload size
        chunk_size = 500
        for i in range(0, len(variants_to_push), chunk_size):
            chunk = variants_to_push[i : i + chunk_size]
            supabase.table("global_variants").upsert(
                clean_records(pd.DataFrame(chunk))
            ).execute()

    print("Supabase sync complete! 🎉")


async def main_async():
    session = requests.Session()
    session.headers.update(STD_HEADERS)

    csrf_name, csrf_value = get_csrf(session)
    site_cookies = session.cookies.get_dict()

    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []

    # CRITICAL FIX: Extend aiohttp timeout to 90 seconds
    timeout = aiohttp.ClientTimeout(total=90)

    async with aiohttp.ClientSession(
        cookies=site_cookies, headers=STD_HEADERS, timeout=timeout
    ) as async_session:
        for liquor_type in LIQUOR_TYPES:
            segments = get_segments(session, csrf_name, csrf_value, liquor_type)
            print(f"Found {len(segments)} segments for {liquor_type}")
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
        print(f"Executing {len(tasks)} requests to the State Portal...")
        results = await asyncio.gather(*tasks)

    all_rows = []
    for r in results:
        all_rows.extend(r)

    products = pd.DataFrame(all_rows)
    print("Products scraped successfully:", len(products))

    if len(products) > 0:
        products.to_csv("products_raw.csv", index=False)
        companies, brands, variants = normalize(products)
        push_to_supabase(companies, brands, variants)
    else:
        print(
            "Warning: No products found. Check your network or the state portal's status."
        )


if __name__ == "__main__":
    asyncio.run(main_async())
