#!/usr/bin/env python3
"""
DC Multifamily Map Builder

Strategy:
  1. Read local CAMA GeoJSON for building details (bedrooms, stories, grade, etc.)
  2. Query Tax Extract (MapServer/53) for address, owner, ward, assessment (no geometry)
  3. Geocode addresses using DC Geocoder to get lat/lng coordinates
  4. Merge everything and output a point GeoJSON

Uses only built-in Python libraries — no pip installs needed.
Run on your Windows machine (the VM proxy blocks DC GIS APIs).

Output: data/multifamily_map.geojson
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
REPO_ROOT  = os.path.join(SCRIPT_DIR, "..")
CAMA_FILE  = os.path.join(REPO_ROOT, "Computer_Assisted_Mass_Appraisal_-_Residential.geojson")
OUT_FILE   = os.path.join(REPO_ROOT, "data", "multifamily_map.geojson")

MIN_UNITS  = 3
BATCH_SIZE = 50

# MapServer/53 — ITS Tax Extract (address, owner, ward, assessment)
TAX_URL = ("https://maps2.dcgis.dc.gov/dcgis/rest/services/"
           "DCGIS_DATA/Property_and_Land_WebMercator/MapServer/53/query")
TAX_FIELDS = "SSL,PREMISEADD,PRMS_WARD,ASSESSMENT,OWNERNAME"

# DC Address Points layer — has SSL + point geometry
ADDR_URL = ("https://maps2.dcgis.dc.gov/dcgis/rest/services/"
            "DCGIS_DATA/Location_WebMercator/MapServer/0/query")

# DC Geocoder — fallback for addresses not found in Address Points
GEOCODER_URL = ("https://maps2.dcgis.dc.gov/dcgis/rest/services/"
                "DCGIS_APPS/DC_Geocoder/GeocodeServer/findAddressCandidates")

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


def extract_coords(geometry):
    """Extract [lng, lat] from various ArcGIS geometry types."""
    if not geometry or not isinstance(geometry, dict):
        return None
    if "x" in geometry and "y" in geometry:
        x, y = geometry["x"], geometry["y"]
        if x is not None and y is not None:
            return [float(x), float(y)]
    rings = geometry.get("rings")
    if rings and rings[0]:
        ring = rings[0]
        n = len(ring)
        if n > 0:
            return [sum(p[0] for p in ring) / n, sum(p[1] for p in ring) / n]
    return None


# ---------------------------------------------------------------------------
# STEP 1: Read CAMA GeoJSON for building attributes
# ---------------------------------------------------------------------------

def load_cama_data():
    """Read the local CAMA GeoJSON, filter for MIN_UNITS+, return dict keyed by SSL."""
    print(f"  Reading {CAMA_FILE}...")
    print(f"  (This is ~84MB, may take a moment)")

    with open(CAMA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    print(f"  Total CAMA features: {len(features):,}")

    props_by_ssl = {}
    for feat in features:
        p = feat.get("properties", {})
        units = p.get("NUM_UNITS") or 0
        if units < MIN_UNITS:
            continue
        ssl = (p.get("SSL") or "").strip()
        if not ssl:
            continue
        if ssl in props_by_ssl and units <= (props_by_ssl[ssl].get("NUM_UNITS") or 0):
            continue

        props_by_ssl[ssl] = {
            "SSL": ssl, "NUM_UNITS": int(units),
            "BEDRM": p.get("BEDRM") or 0, "BATHRM": p.get("BATHRM") or 0,
            "HF_BATHRM": p.get("HF_BATHRM") or 0, "ROOMS": p.get("ROOMS") or 0,
            "STORIES": p.get("STORIES") or 0, "GBA": p.get("GBA") or 0,
            "LANDAREA": p.get("LANDAREA") or 0, "AYB": p.get("AYB") or 0,
            "YR_RMDL": p.get("YR_RMDL") or 0, "EYB": p.get("EYB") or 0,
            "PRICE": p.get("PRICE") or 0,
            "SALEDATE": (p.get("SALEDATE") or ""),
            "QUALIFIED": (p.get("QUALIFIED") or "").strip(),
            "GRADE_D": (p.get("GRADE_D") or "").strip(),
            "CNDTN_D": (p.get("CNDTN_D") or "").strip(),
            "EXTWALL_D": (p.get("EXTWALL_D") or "").strip(),
            "ROOF_D": (p.get("ROOF_D") or "").strip(),
            "INTWALL_D": (p.get("INTWALL_D") or "").strip(),
            "KITCHENS": p.get("KITCHENS") or 0,
            "FIREPLACES": p.get("FIREPLACES") or 0,
            "USECODE": p.get("USECODE") or "",
            "STYLE_D": (p.get("STYLE_D") or "").strip(),
            "STRUCT_D": (p.get("STRUCT_D") or "").strip(),
            "HEAT_D": (p.get("HEAT_D") or "").strip(),
            "AC": (p.get("AC") or "").strip(),
        }

    print(f"  Properties with {MIN_UNITS}+ units: {len(props_by_ssl):,}")
    return props_by_ssl


# ---------------------------------------------------------------------------
# STEP 2: Fetch address, owner from Tax Extract (no geometry)
# ---------------------------------------------------------------------------

def fetch_tax_data(ssl_list):
    """Batch-query Tax Extract for address, ward, assessment, owner."""
    tax_by_ssl = {}
    total = len(ssl_list)

    print(f"  Querying Tax Extract for {total:,} SSLs in batches of {BATCH_SIZE}...")

    for i in range(0, total, BATCH_SIZE):
        batch = ssl_list[i:i+BATCH_SIZE]
        ssl_clause = ",".join(f"'{s}'" for s in batch)
        where = f"SSL IN ({ssl_clause})"

        qs = (
            "where=" + urllib.parse.quote(where, safe="")
            + "&outFields=" + urllib.parse.quote(TAX_FIELDS, safe="")
            + "&returnGeometry=false"
            + "&resultRecordCount=" + str(BATCH_SIZE)
            + "&f=json"
        )

        try:
            data = fetch_json(TAX_URL + "?" + qs)
            if "error" in data:
                if i == 0:
                    print(f"    Tax Extract error: {data['error']}")
                continue

            for feat in data.get("features", []):
                a = feat.get("attributes", {})
                ssl = (a.get("SSL") or "").strip()
                if not ssl:
                    continue
                tax_by_ssl[ssl] = {
                    "address":  (a.get("PREMISEADD") or "").strip(),
                    "ward":     str(a.get("PRMS_WARD") or "").strip(),
                    "assessed": a.get("ASSESSMENT") or 0,
                    "owner":    (a.get("OWNERNAME") or "").strip(),
                }
        except Exception as e:
            if i == 0:
                print(f"    First batch error: {e}")
            continue

        done = min(i + BATCH_SIZE, total)
        if done % 500 < BATCH_SIZE or done == total:
            print(f"    Progress: {done:,}/{total:,} SSLs | {len(tax_by_ssl):,} matched")

    print(f"  Matched: {len(tax_by_ssl):,}")
    return tax_by_ssl


# ---------------------------------------------------------------------------
# STEP 3: Get coordinates — try Address Points by SSL, then geocode
# ---------------------------------------------------------------------------

def fetch_coords_address_points(ssl_list):
    """Try to get point geometry from the DC Address Points layer by SSL."""
    coords_by_ssl = {}
    total = len(ssl_list)

    # First, probe the layer to see if it has SSL field and geometry
    print(f"  Probing Address Points layer...")
    probe_url = (
        ADDR_URL + "?"
        + "where=" + urllib.parse.quote("1=1", safe="")
        + "&outFields=*"
        + "&returnGeometry=true"
        + "&outSR=4326"
        + "&resultRecordCount=1"
        + "&f=json"
    )

    try:
        probe = fetch_json(probe_url)
        if "error" in probe:
            print(f"    Address Points probe error: {probe['error']}")
            return coords_by_ssl

        feats = probe.get("features", [])
        if not feats:
            print(f"    Address Points returned no features")
            return coords_by_ssl

        sample = feats[0]
        attrs = sample.get("attributes", {})
        geo = sample.get("geometry")
        field_names = list(attrs.keys())

        print(f"    Fields: {field_names[:15]}...")
        print(f"    Geometry: {str(geo)[:150]}")

        # Check if SSL field exists
        ssl_field = None
        for fn in field_names:
            if fn.upper() == "SSL":
                ssl_field = fn
                break

        if not ssl_field:
            print(f"    No SSL field found in Address Points — will use geocoder instead")
            return coords_by_ssl

        has_geo = geo is not None and extract_coords(geo) is not None
        if not has_geo:
            print(f"    Address Points has no usable geometry — will use geocoder instead")
            return coords_by_ssl

        print(f"    Found SSL field '{ssl_field}' with geometry — querying {total:,} SSLs...")

    except Exception as e:
        print(f"    Address Points probe failed: {e}")
        return coords_by_ssl

    # Query in batches by SSL
    for i in range(0, total, BATCH_SIZE):
        batch = ssl_list[i:i+BATCH_SIZE]
        ssl_clause = ",".join(f"'{s}'" for s in batch)
        where = f"{ssl_field} IN ({ssl_clause})"

        qs = (
            "where=" + urllib.parse.quote(where, safe="")
            + "&outFields=" + urllib.parse.quote(ssl_field, safe="")
            + "&returnGeometry=true"
            + "&outSR=4326"
            + "&resultRecordCount=" + str(BATCH_SIZE)
            + "&f=json"
        )

        try:
            data = fetch_json(ADDR_URL + "?" + qs)
            if "error" in data:
                continue
            for feat in data.get("features", []):
                a = feat.get("attributes", {})
                ssl = (a.get(ssl_field) or "").strip()
                if not ssl:
                    continue
                coords = extract_coords(feat.get("geometry"))
                if coords:
                    coords_by_ssl[ssl] = coords
        except Exception:
            continue

        done = min(i + BATCH_SIZE, total)
        if done % 500 < BATCH_SIZE or done == total:
            print(f"    Address Points progress: {done:,}/{total:,} | {len(coords_by_ssl):,} with coords")

    print(f"  Address Points: {len(coords_by_ssl):,} coordinates found")
    return coords_by_ssl


def geocode_addresses(address_list):
    """
    Geocode addresses using the DC Geocoder.
    address_list: list of (ssl, address) tuples
    Returns dict: ssl -> [lng, lat]
    """
    coords_by_ssl = {}
    total = len(address_list)
    errors = 0

    print(f"  Geocoding {total:,} addresses via DC Geocoder...")

    for idx, (ssl, address) in enumerate(address_list):
        qs = (
            "SingleLine=" + urllib.parse.quote(address + ", Washington, DC", safe="")
            + "&outSR=4326"
            + "&maxLocations=1"
            + "&f=json"
        )

        try:
            data = fetch_json(GEOCODER_URL + "?" + qs, retries=2, timeout=30)
            candidates = data.get("candidates", [])
            if candidates:
                loc = candidates[0].get("location", {})
                x, y = loc.get("x"), loc.get("y")
                if x is not None and y is not None:
                    coords_by_ssl[ssl] = [float(x), float(y)]
        except Exception:
            errors += 1

        # Rate limit: small pause every request to be respectful
        if (idx + 1) % 10 == 0:
            time.sleep(0.2)

        done = idx + 1
        if done % 200 == 0 or done == total:
            print(f"    Geocoded: {done:,}/{total:,} | {len(coords_by_ssl):,} resolved | {errors} errors")

    print(f"  Geocoder: {len(coords_by_ssl):,} coordinates found ({errors} errors)")
    return coords_by_ssl


# ---------------------------------------------------------------------------
# STEP 4: Merge and output GeoJSON
# ---------------------------------------------------------------------------

def build_geojson(cama_data, tax_data, coords_data):
    """Merge CAMA + Tax + coords into a point GeoJSON."""
    features = []

    for ssl, props in cama_data.items():
        tax = tax_data.get(ssl)
        if not tax:
            continue
        coords = coords_data.get(ssl)
        if not coords:
            continue
        address = tax.get("address", "")
        if not address:
            continue

        lng, lat = coords[0], coords[1]
        if lat < 38.7 or lat > 39.1 or lng < -77.2 or lng > -76.8:
            continue

        saledate = props["SALEDATE"]
        if saledate and "T" in saledate:
            saledate = saledate.split("T")[0]

        feature_props = {
            "address": address, "ward": tax.get("ward", ""),
            "units": props["NUM_UNITS"], "bedrooms": props["BEDRM"],
            "bathrooms": props["BATHRM"], "halfBaths": props["HF_BATHRM"],
            "rooms": props["ROOMS"], "stories": props["STORIES"],
            "gba": props["GBA"], "landArea": props["LANDAREA"],
            "yearBuilt": props["AYB"], "yrRemodel": props["YR_RMDL"],
            "effYrBuilt": props["EYB"], "price": props["PRICE"],
            "saleDate": saledate, "qualified": props["QUALIFIED"],
            "grade": props["GRADE_D"], "condition": props["CNDTN_D"],
            "extWall": props["EXTWALL_D"], "roof": props["ROOF_D"],
            "intWall": props["INTWALL_D"], "style": props["STYLE_D"],
            "structure": props["STRUCT_D"], "heat": props["HEAT_D"],
            "ac": props["AC"], "kitchens": props["KITCHENS"],
            "fireplaces": props["FIREPLACES"],
            "useCode": str(props["USECODE"]).strip(),
            "assessed": tax.get("assessed", 0),
            "owner": tax.get("owner", ""),
            "ssl": ssl,
        }

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": feature_props
        })

    return {
        "type": "FeatureCollection",
        "metadata": {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "count": len(features),
            "source": "DC Open Data — CAMA Residential + Tax Extract + Address Points/Geocoder",
            "filter": f"NUM_UNITS >= {MIN_UNITS}"
        },
        "features": features
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("DC Multifamily Map Builder")
    print("=" * 60)
    print()

    # Step 1: Load local CAMA data
    print("[1/4] Loading CAMA building attributes from local GeoJSON...")
    cama_data = load_cama_data()
    if not cama_data:
        print("ERROR: No properties found with 3+ units in CAMA file.")
        sys.exit(1)
    print()

    # Step 2: Fetch address + owner from Tax Extract
    ssl_list = list(cama_data.keys())
    print(f"[2/4] Fetching address & owner from Tax Extract...")
    tax_data = fetch_tax_data(ssl_list)
    if not tax_data:
        print("ERROR: Could not fetch data from Tax Extract.")
        sys.exit(1)
    print()

    # Step 3a: Try Address Points layer for coordinates
    ssls_with_address = [ssl for ssl in ssl_list if ssl in tax_data and tax_data[ssl]["address"]]
    print(f"[3/4] Getting coordinates for {len(ssls_with_address):,} properties...")
    coords_data = fetch_coords_address_points(ssls_with_address)

    # Step 3b: Geocode any remaining addresses that didn't get coords
    missing = [(ssl, tax_data[ssl]["address"])
               for ssl in ssls_with_address
               if ssl not in coords_data and tax_data[ssl]["address"]]

    if missing:
        print(f"\n  {len(missing):,} addresses still need geocoding...")
        geocoded = geocode_addresses(missing)
        coords_data.update(geocoded)
        print(f"  Total coordinates: {len(coords_data):,}")
    print()

    # Step 4: Build and save GeoJSON
    print("[4/4] Building GeoJSON...")
    geojson = build_geojson(cama_data, tax_data, coords_data)
    count = len(geojson["features"])

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, separators=(",", ":"))

    file_size = os.path.getsize(OUT_FILE) / 1024
    print()
    print("=" * 60)
    print(f"Saved {count:,} properties -> {OUT_FILE}")
    print(f"  File size: {file_size:,.0f} KB")
    print(f"  Updated:   {geojson['metadata']['updated']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
