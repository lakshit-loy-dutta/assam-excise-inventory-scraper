import os
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

EXCISE_TABLE_URL = "https://excise.assam.gov.in/information-services/excise-duty-rates"

FINANCIAL_YEAR = "2025-2026"

LIQUOR_TYPES = ["IML", "FL", "BEER", "CS", "HL"]

load_dotenv()

# -----------------------
# utilities
# -----------------------


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


def clean_number(text):
    if not text:
        return None
    match = re.search(r"\d+(\.\d+)?", text)
    if match:
        return float(match.group())
    return None


# -----------------------
# csrf token
# -----------------------


def get_csrf(session):

    r = session.get(PRICE_PAGE)
    soup = BeautifulSoup(r.text, "html.parser")

    for inp in soup.find_all("input"):
        name = inp.get("name")
        if name and str(name).startswith("csrf"):
            return str(name), str(inp.get("value"))

    raise Exception("CSRF token not found")


# -----------------------
# get segments
# -----------------------


def get_segments(session, csrf_name, csrf_value, liquor_type):

    payload = {"type": liquor_type, csrf_name: csrf_value}

    headers = {"X-Requested-With": "XMLHttpRequest", "Referer": PRICE_PAGE}

    r = session.post(SEGMENT_API, data=payload, headers=headers)

    data = r.json()

    segments = []

    for item in data:
        segments.append(item["CODE"])

    return segments


# -----------------------
# get price rows
# -----------------------


def get_prices(session, csrf_name, csrf_value, liquor_type, segment):

    payload = {
        "segment": segment,
        "financialYear": FINANCIAL_YEAR,
        "acttype": "list",
        "liquorType": liquor_type,
        csrf_name: csrf_value,
    }

    headers = {"X-Requested-With": "XMLHttpRequest", "Referer": PRICE_PAGE}

    r = session.post(PRICE_API, data=payload, headers=headers)

    soup = BeautifulSoup(r.text, "html.parser")

    rows = []

    for tr in soup.find_all("tr"):

        cols = [c.get_text(strip=True) for c in tr.find_all("td")]

        if len(cols) >= 7:

            mrp = cols[6]

            try:
                mrp_val = float(mrp)
            except:
                continue

            # remove export rows
            if mrp_val == 0:
                continue

            rows.append(
                {
                    "supplier": cols[1],
                    "category": cols[3],
                    "brand_name": cols[4],
                    "pack_size": cols[5],
                    "mrp": mrp_val,
                    "segment": segment,
                }
            )

    return rows


# -----------------------
# scrape excise duty
# -----------------------


def scrape_tax_table():

    r = requests.get(EXCISE_TABLE_URL)

    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.find("table")

    if table is None:
        raise Exception("No table found on excise duty rates page.")

    rows = table.find_all("tr")

    data = []

    current_liquor = None
    current_gallonage = None

    for row in rows[1:]:

        cols = [c.get_text(strip=True) for c in row.find_all("td")]

        if len(cols) == 5:

            current_liquor = cols[1]
            brand = cols[2]
            excise = cols[3]
            gallonage = cols[4]

            current_gallonage = gallonage

        elif len(cols) == 4:

            brand = cols[0]
            excise = cols[1]
            gallonage = current_gallonage

        else:
            continue

        data.append(
            {
                "category": brand,
                "excise_per_case": clean_number(excise),
                "gallonage_fee": clean_number(gallonage),
            }
        )

    return pd.DataFrame(data)


# -----------------------
# normalize dataset
# -----------------------


def normalize(products, tax_table):

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

        excise_case = None

        tax_match = tax_table[
            tax_table["category"].str.contains(row["category"], case=False, na=False)
        ]

        if not tax_match.empty:
            excise_case = tax_match.iloc[0]["excise_per_case"]

        excise_bottle = None

        if excise_case and case_size:
            excise_bottle = excise_case / case_size

        variants.append(
            {
                "id": hash_id(brand_key + str(size_ml)),
                "brand_id": brand_id,
                "size_ml": size_ml,
                "case_size": case_size,
                "mrp": row["mrp"],
                "excise_per_bottle": excise_bottle,
                "segment": row["segment"],
            }
        )

    variants_df = pd.DataFrame(variants).drop_duplicates()

    return (
        pd.DataFrame(companies.values()),
        pd.DataFrame(brands.values()),
        variants_df,
    )


# -----------------------
# push to supabase
# -----------------------


def push_to_supabase(companies_df, brands_df, variants_df):

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError(
            "CRITICAL: Supabase URL and Key are missing from environment variables!"
        )
    supabase: Client = create_client(url, key)
    # -----------------------------------------
    # 1. UPSERT COMPANIES (Only if new)
    # -----------------------------------------
    print("Checking Companies...")
    comp_res = supabase.table("global_companies").select("id").execute()
    existing_companies = cast(List[Dict[str, Any]], comp_res.data)

    existing_company_ids = {str(row["id"]) for row in existing_companies}

    new_companies = companies_df[~companies_df["id"].isin(existing_company_ids)]
    if not new_companies.empty:
        print(f"Pushing {len(new_companies)} NEW companies...")
        supabase.table("global_companies").upsert(
            new_companies.to_dict(orient="records")
        ).execute()

    # -----------------------------------------
    # 2. UPSERT BRANDS (Only if new)
    # -----------------------------------------
    print("Checking Brands...")
    brand_res = supabase.table("global_brands").select("id").execute()
    existing_brands = cast(List[Dict[str, Any]], brand_res.data)

    existing_brand_ids = {str(row["id"]) for row in existing_brands}

    new_brands = brands_df[~brands_df["id"].isin(existing_brand_ids)]
    if not new_brands.empty:
        print(f"Pushing {len(new_brands)} NEW brands...")
        supabase.table("global_brands").upsert(
            new_brands.to_dict(orient="records")
        ).execute()

    # -----------------------------------------
    # 3. UPSERT VARIANTS (New items OR Price changes)
    # -----------------------------------------
    print("Checking Variants for changes...")
    var_res = (
        supabase.table("global_variants").select("id, mrp, excise_per_bottle").execute()
    )
    existing_variants = cast(List[Dict[str, Any]], var_res.data)

    variant_lookup = {str(item["id"]): item for item in existing_variants}

    variants_to_push = []
    variant_records = variants_df.to_dict(orient="records")

    for row in variant_records:
        vid = str(row["id"])

        if vid not in variant_lookup:
            variants_to_push.append(row)
        else:
            old_mrp = variant_lookup[vid]["mrp"]
            old_excise = variant_lookup[vid]["excise_per_bottle"]

            new_mrp = row["mrp"]
            new_excise = row["excise_per_bottle"]

            if old_mrp != new_mrp or old_excise != new_excise:
                variants_to_push.append(row)

    if variants_to_push:
        print(f"Pushing {len(variants_to_push)} NEW or UPDATED variants...")
        clean_push = [
            {k: (None if pd.isna(v) else v) for k, v in item.items()}
            for item in variants_to_push
        ]
        supabase.table("global_variants").upsert(clean_push).execute()
    else:
        print("No changes detected in Variants. Database is up to date!")
