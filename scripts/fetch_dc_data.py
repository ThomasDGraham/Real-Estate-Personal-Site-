#!/usr/bin/env python3
"""
DC Multifamily Property Fetcher
Pulls all multifamily properties (2+ units) from DC Open Data ArcGIS REST API.
Uses only built-in Python libraries — no pip installs needed.

Strategy:
  1. Fetch SSLs + building data from CAMA layer (MapServer/25)
     - Only has building fields: SSL, NUM_UNITS, GBA, GRADE_D, CNDTN_D,
       EXTWALL_D, USECODE, LANDAREA, AYB, PRICE, SALEDATE, etc.
  2. Enrich with address, ward, assessed value, and owner from
     the Tax Extract layer (MapServer/53) — which has everything.

Output: data/properties.json
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE   = os.path.join(SCRIPT_DIR, "..", "data", "properties.json")

PAGE_SIZE = 2000
WHERE     = "NUM_UNITS >= 2"

# Fields that ACTUALLY EXIST on MapServer/25 (CAMA polygon layer)
CAMA_FIELDS = ",".join([
    "SSL", "NUM_UNITS", "GBA", "LANDAREA",
    "GRADE", "GRADE_D", "CNDTN", "CNDTN_D", "EXTWALL_D",
    "USECODE", "AYB", "PRICE", "SALEDATE"
])

# Fields to pull from Tax Extract (MapServer/53) for enrichment
TAX_FIELDS = "SSL,PREMISEADD,WARD,ASSESSMENT,OWNERNAME"

# Primary: CAMA Polygon layer (MapServer/25)
POLY_URL  = ("https://maps2.dcgis.dc.gov/dcgis/rest/services/"
             "DCGIS_DATA/Property_and_Land_WebMercator/MapServer/25/query")

# Fallback: CamaResPt Point layer (MapServer/4)
POINT_URL = ("https://maps2.dcgis.dc.gov/dcgis/rest/services/"
             "DCGIS_APPS/Real_Property_Application/MapServer/4/query")

# Enrichment: ITS Tax Extract (MapServer/53) — has address, ward, assessed, owner
TAX_URL   = ("https://maps2.dcgis.dc.gov/dcgis/rest/services/"
             "DCGIS_DATA/Property_and_Land_WebMercator/MapServer/53/query")

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def fetch_json(url, retries=3, timeout=60):
    """Fetch a URL and return parsed JSON, with retries."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "DC-Property-Fetcher/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"    Retry {attempt+1}/{retries} in {wait}s: {e}")
                time.sleep(wait)
            else:
                raise


def build_url(base_url, fields, offset):
    """Build query URL — f=json, returnGeometry=false."""
    qs = (
        "where="              + urllib.parse.quote(WHERE, safe="")
        + "&outFields="       + urllib.parse.quote(fields, safe="")
        + "&returnGeometry=false"
        + "&resultRecordCount=" + str(PAGE_SIZE)
        + "&resultOffset="    + str(offset)
        + "&orderByFields="   + urllib.parse.quote("SSL ASC", safe="")
        + "&f=json"
    )
    return base_url + "?" + qs


def attr(feature, *keys):
    """Get first non-None attribute value from feature."""
    props = feature.get("attributes") or feature.get("properties") or {}
    for k in keys:
        v = props.get(k)
        if v is not None and v != "":
            return v
    return None

# ---------------------------------------------------------------------------
# MAIN FETCH
# ---------------------------------------------------------------------------

def fetch_all_features(base_url, fields, label):
    """Page through all results from a given endpoint."""
    features = []
    offset = 0

    while True:
        url = build_url(base_url, fields, offset)
        print(f"  {label} page (offset {offset}, fetched {len(features)} so far)...")

        data = fetch_json(url)

        # Check for API error
        if "error" in data:
            raise Exception(f"API error: {data['error']}")

        batch = data.get("features", [])
        if not batch:
            break

        features.extend(batch)
        offset += PAGE_SIZE

        # If we got fewer than PAGE_SIZE, we've reached the end
        exceeded = data.get("exceededTransferLimit", False)
        if len(batch) < PAGE_SIZE and not exceeded:
            break

    return features


def process_features(features):
    """Convert raw CAMA features into property dicts (no address yet)."""
    properties = []
    skipped = 0

    for f in features:
        ssl = attr(f, "SSL")
        if not ssl:
            skipped += 1
            continue

        units      = attr(f, "NUM_UNITS") or 0
        bldg_area  = attr(f, "GBA") or 0
        land_area  = attr(f, "LANDAREA") or 0
        grade_d    = attr(f, "GRADE_D") or ""
        cndtn_d    = attr(f, "CNDTN_D") or ""
        usecode    = attr(f, "USECODE") or ""
        year_built = attr(f, "AYB") or 0

        # Combine grade + condition using the descriptive fields
        cond_grade = ""
        if grade_d and cndtn_d:
            cond_grade = f"{grade_d} / {cndtn_d}"
        elif grade_d:
            cond_grade = str(grade_d)
        elif cndtn_d:
            cond_grade = str(cndtn_d)

        properties.append({
            "ssl":        ssl.strip(),
            "address":    "",       # filled by enrichment
            "ward":       "",       # filled by enrichment
            "units":      int(units) if units else 0,
            "assessed":   0,        # filled by enrichment
            "landArea":   int(land_area) if land_area else 0,
            "bldgArea":   int(bldg_area) if bldg_area else 0,
            "condGrade":  cond_grade,
            "useCode":    str(int(usecode)).strip() if usecode else "",
            "yearBuilt":  int(year_built) if year_built else 0,
            "owner":      "",       # filled by enrichment
        })

    return properties, skipped


def enrich_from_tax(properties):
    """Batch-query the Tax Extract for address, ward, assessed value, and owner."""
    ssl_map = {}
    for p in properties:
        if p["ssl"]:
            ssl_map[p["ssl"]] = p

    if not ssl_map:
        return 0

    ssl_list = list(ssl_map.keys())
    enriched = 0
    batch_size = 50

    print(f"  Querying Tax Extract for {len(ssl_list)} SSLs in batches of {batch_size}...")

    for i in range(0, len(ssl_list), batch_size):
        batch = ssl_list[i:i+batch_size]
        ssl_clause = ",".join(f"'{s}'" for s in batch)
        where = f"SSL IN ({ssl_clause})"

        qs = (
            "where="           + urllib.parse.quote(where, safe="")
            + "&outFields="    + urllib.parse.quote(TAX_FIELDS, safe="")
            + "&returnGeometry=false"
            + "&resultRecordCount=" + str(batch_size)
            + "&f=json"
        )
        url = TAX_URL + "?" + qs

        try:
            data = fetch_json(url)
            feats = data.get("features", [])
            for f in feats:
                a = f.get("attributes") or f.get("properties") or {}
                ssl = (a.get("SSL") or "").strip()
                if ssl not in ssl_map:
                    continue

                p = ssl_map[ssl]
                addr  = (a.get("PREMISEADD") or "").strip()
                ward  = a.get("WARD")
                asmnt = a.get("ASSESSMENT")
                owner = (a.get("OWNERNAME") or "").strip()

                if addr:  p["address"]  = addr
                if ward:  p["ward"]     = str(ward).strip()
                if asmnt: p["assessed"] = int(asmnt)
                if owner: p["owner"]    = owner

                enriched += 1

        except Exception as e:
            print(f"    Batch failed (offset {i}): {e}")
            continue

        # Progress indicator
        done = min(i + batch_size, len(ssl_list))
        if done % 500 < batch_size or done == len(ssl_list):
            print(f"    Progress: {done}/{len(ssl_list)} SSLs queried")

    return enriched

# ---------------------------------------------------------------------------
# RUN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("DC Multifamily Property Fetcher")
    print("=" * 60)
    print()

    # Step 1: Fetch building data from CAMA layer
    print("[1/3] Fetching CAMA residential data...")
    features = []

    try:
        print(f"  Trying polygon layer (MapServer/25)...")
        features = fetch_all_features(POLY_URL, CAMA_FIELDS, "Polygon")
        print(f"  Got {len(features)} features from polygon layer")
    except Exception as e:
        print(f"  Polygon layer failed: {e}")

    if not features:
        try:
            print(f"  Trying point layer (MapServer/4)...")
            features = fetch_all_features(POINT_URL, CAMA_FIELDS, "Point")
            print(f"  Got {len(features)} features from point layer")
        except Exception as e:
            print(f"  Point layer also failed: {e}")
            print("  ERROR: Could not fetch data from either endpoint.")
            sys.exit(1)

    print(f"  Total raw features: {len(features)}")
    print()

    # Step 2: Process into property records
    print("[2/3] Processing features...")
    properties, skipped = process_features(features)
    print(f"  Valid properties: {len(properties)}")
    print(f"  Skipped (no SSL): {skipped}")
    print()

    # Step 3: Enrich with address, ward, assessed value, owner from Tax Extract
    print(f"[3/3] Enriching {len(properties)} properties from Tax Extract...")
    enriched = enrich_from_tax(properties)
    print(f"  Enriched {enriched} properties with address/ward/assessed/owner")

    # Remove properties that didn't get an address (no matching tax record)
    before = len(properties)
    properties = [p for p in properties if p["address"]]
    removed = before - len(properties)
    if removed:
        print(f"  Removed {removed} properties with no address match")
    print()

    # Save
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(properties),
        "source": "DC Open Data — CAMA Residential (MapServer/25) + Tax Extract (MapServer/53)",
        "properties": properties,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(",", ":"))

    file_size = os.path.getsize(OUT_FILE) / 1024
    print("=" * 60)
    print(f"Saved {len(properties)} properties -> {OUT_FILE}")
    print(f"  File size: {file_size:.0f} KB")
    print(f"  Updated:   {output['updated']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
