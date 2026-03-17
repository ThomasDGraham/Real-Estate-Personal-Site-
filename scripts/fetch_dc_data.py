#!/usr/bin/env python3
"""
DC Multifamily Property Data Fetcher
=====================================
Fetches all multifamily properties from DC Open Data (no API key needed)
and saves them as data/properties.json for the tgrepe.com dashboard.

USAGE (Windows PowerShell):
  python scripts/fetch_dc_data.py
    -- or --
  py scripts/fetch_dc_data.py

NO external libraries needed — uses only Python's built-in urllib.

OUTPUT:
  data/properties.json  (committed to your GitHub repo)
"""

import json
import time
import sys
import os
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# ── API Endpoints ────────────────────────────────────────────────────────────
# Primary: CamaResPt — native POINT layer (direct x/y coordinates)
POINT_URL  = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_APPS/Real_Property_Application/MapServer/4/query"
# Fallback: CAMA WebMercator polygon layer
POLY_URL   = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/MapServer/25/query"
# Tax Extract: owner name, address, ward, zoning, assessment
TAX_URL    = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/MapServer/53/query"

PAGE_SIZE  = 1000
WHERE      = "NUM_UNITS >= 2 OR USECODE IN (21,22,23,24,25,29,31,32,39,41,42,89,91,92)"
FIELDS     = "SSL,NUM_UNITS,AYB,GBA,LANDAREA,PRICE,SALEDATE,USECODE,GRADE_D,CNDTN_D,STORIES,BLDG_NUM"

USECODE_MAP = {
    13: "Cooperative Apartment",
    21: "Duplex",         22: "Duplex/Flat",
    23: "Triplex",        24: "Row House (3+ units)",
    25: "Semi-Detached (3+ units)", 29: "Walk-Up Apartment",
    31: "Elevator Apartment",       32: "Apartment Conversion",
    39: "Row/Apt Combo",  41: "Mixed Use (Residential)",
    42: "Mixed Use (Commercial)",   89: "Vacant w/ Improvements",
    91: "Vacant Lot",     92: "Vacant Residential",
}

# DC metro stations [name, lat, lng]
METROS = [
    ["Anacostia",        38.8633, -76.9953],
    ["Archives",         38.8937, -77.0211],
    ["Benning Road",     38.8904, -76.9381],
    ["Bethesda",         38.9842, -77.0940],
    ["Capitol South",    38.8850, -77.0050],
    ["Columbia Heights", 38.9282, -77.0326],
    ["Congress Heights", 38.8455, -76.9953],
    ["Dupont Circle",    38.9096, -77.0434],
    ["Eastern Market",   38.8844, -76.9963],
    ["Farragut North",   38.9031, -77.0397],
    ["Federal Triangle", 38.8933, -77.0280],
    ["Foggy Bottom",     38.9001, -77.0502],
    ["Fort Totten",      38.9519, -77.0023],
    ["Friendship Heights",38.9606,-77.0856],
    ["Gallery Place",    38.8982, -77.0219],
    ["Georgia Ave",      38.9373, -77.0243],
    ["H St/Benning",     38.8997, -76.9763],
    ["Howard University",38.9281, -77.0201],
    ["L'Enfant Plaza",   38.8846, -77.0215],
    ["Metro Center",     38.8983, -77.0283],
    ["Navy Yard",        38.8763, -77.0055],
    ["NoMa",             38.9082, -77.0042],
    ["Pentagon City",    38.8631, -77.0598],
    ["Petworth",         38.9371, -77.0192],
    ["Rhode Island Ave", 38.9207, -76.9958],
    ["Shaw",             38.9125, -77.0218],
    ["Silver Spring",    38.9940, -77.0310],
    ["Stadium-Armory",   38.8853, -76.9766],
    ["Takoma",           38.9762, -77.0127],
    ["Tenleytown",       38.9477, -77.0795],
    ["Union Station",    38.8973, -77.0063],
    ["U Street",         38.9167, -77.0289],
    ["Van Ness",         38.9440, -77.0635],
    ["Waterfront",       38.8763, -77.0168],
    ["Woodley Park",     38.9258, -77.0543],
]

def poly_center(rings):
    """Compute centroid of a polygon from its ring coordinates."""
    ring = rings[0]
    if not ring:
        return None
    x = sum(pt[0] for pt in ring) / len(ring)
    y = sum(pt[1] for pt in ring) / len(ring)
    return (x, y)

def nearest_metro(lat, lng):
    """Return (name, miles) for the closest metro station."""
    best_name, best_dist = "Unknown", float("inf")
    for name, mlat, mlng in METROS:
        dlat = (lat - mlat) * 69.0
        dlng = (lng - mlng) * 69.0 * 0.8  # rough cosine correction
        d = (dlat**2 + dlng**2) ** 0.5
        if d < best_dist:
            best_dist, best_name = d, name
    return best_name, round(best_dist, 2)

def build_url(base_url, offset):
    """Build ArcGIS query URL explicitly — no dict spread, returnGeometry always included."""
    qs = (
        "where="            + urllib.parse.quote(WHERE, safe="")
        + "&outFields="     + urllib.parse.quote(FIELDS, safe="")
        + "&returnGeometry=true"
        + "&resultRecordCount=" + str(PAGE_SIZE)
        + "&resultOffset="  + str(offset)
        + "&orderByFields=SSL+ASC"
        + "&f=geojson"
    )
    return base_url + "?" + qs

def fetch_pages(url, label=""):
    """Paginate through all records from an ArcGIS REST endpoint."""
    all_features = []
    offset = 0
    page = 0
    while True:
        page += 1
        full_url = build_url(url, offset)
        if page == 1:
            print(f"  {label} URL: {full_url[:120]}…")
        print(f"  {label} page {page} (offset {offset}, fetched {len(all_features)} so far)…", end=" ", flush=True)
        with urllib.request.urlopen(full_url, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if "error" in data:
            raise RuntimeError(f"API error: {data['error']}")
        features = data.get("features", [])
        all_features.extend(features)
        # Log geometry of first feature on page 1 for debugging
        if page == 1 and features:
            print(f"got {len(features)}, geom[0]={features[0].get('geometry')}")
        else:
            print(f"got {len(features)}")
        if len(features) < PAGE_SIZE or not data.get("exceededTransferLimit", True):
            break
        offset += PAGE_SIZE
        time.sleep(0.3)
    return all_features

def compute_score(p):
    """Investment score: 4 factors. Returns 0–100."""
    factors = {}

    # 1. Price-per-unit (40 pts) — lower is better for value
    if p["num_units"] > 0 and p["sale_price"] > 0:
        ppu = p["sale_price"] / p["num_units"]
        if   ppu < 100_000:  s = 100
        elif ppu < 200_000:  s = 85
        elif ppu < 300_000:  s = 70
        elif ppu < 400_000:  s = 55
        elif ppu < 600_000:  s = 40
        elif ppu < 800_000:  s = 25
        elif ppu < 1_200_000:s = 15
        else:                s = 5
        factors["ppu"] = {"score": s, "value": int(ppu)}
    else:
        factors["ppu"] = {"score": 50, "value": 0}

    # 2. Condition/Grade (25 pts)
    grade_scores = {
        "Exceptional": 20, "Above Average": 40, "Good": 60,
        "Average": 75, "Below Average": 90, "Poor": 100,
        "Unsatisfactory": 100, "Neglected": 100,
    }
    g = (p.get("grade") or "").strip()
    factors["grade"] = {"score": grade_scores.get(g, 65), "value": g or "Unknown"}

    # 3. Lot-to-building ratio (20 pts) — higher = more development potential
    if p["gba_sqft"] > 0 and p["land_sqft"] > 0:
        ratio = p["land_sqft"] / p["gba_sqft"]
        if   ratio >= 4.0: s = 100
        elif ratio >= 2.5: s = 85
        elif ratio >= 1.5: s = 65
        elif ratio >= 1.0: s = 45
        elif ratio >= 0.5: s = 25
        else:              s = 10
        factors["ratio"] = {"score": s, "value": round(ratio, 2)}
    else:
        factors["ratio"] = {"score": 50, "value": 0}

    # 4. Metro proximity (15 pts) — closer is better
    _, miles = nearest_metro(p["lat"], p["lng"])
    if   miles <= 0.25: s = 100
    elif miles <= 0.5:  s = 85
    elif miles <= 0.75: s = 70
    elif miles <= 1.0:  s = 55
    elif miles <= 1.5:  s = 35
    elif miles <= 2.5:  s = 20
    else:               s = 5
    factors["metro"] = {"score": s, "value": round(miles, 2)}

    composite = round(
        factors["ppu"]["score"]   * 0.40 +
        factors["grade"]["score"] * 0.25 +
        factors["ratio"]["score"] * 0.20 +
        factors["metro"]["score"] * 0.15
    )
    return {"composite": composite, "factors": factors}

def main():
    print("=" * 60)
    print("DC Multifamily Property Fetcher")
    print("=" * 60)

    # ── Step 1: Fetch CAMA property data ────────────────────────────────────
    print("\n[1/3] Fetching CAMA residential data…")
    raw_features = []
    is_point = False

    # Try polygon layer first (DCGIS_DATA — geometry reliably returned)
    try:
        raw_features = fetch_pages(POLY_URL, label="MapServer/25 polygon")
        first = raw_features[0] if raw_features else None
        has_geom = first and first.get("geometry") is not None
        if has_geom:
            print(f"  ✓ Polygon layer — got {len(raw_features)} features with geometry")
        else:
            print(f"  ✗ Polygon layer returned {len(raw_features)} features but geometry=None — trying point layer…")
            raw_features = []
    except Exception as e:
        print(f"  ✗ Polygon layer failed: {e} — trying point layer…")

    # Fall back to point layer (DCGIS_APPS) if polygon layer failed or had no geometry
    if not raw_features:
        try:
            raw_features = fetch_pages(POINT_URL, label="MapServer/4 point")
            first = raw_features[0] if raw_features else None
            if first and first.get("geometry") and first["geometry"].get("x") is not None:
                is_point = True
                print(f"  ✓ Point layer — got {len(raw_features)} features with geometry")
            else:
                print(f"  ✗ Point layer also returned no geometry. Both endpoints failed.")
        except Exception as e:
            print(f"  ✗ Point layer failed: {e}")

    print(f"  Raw features: {len(raw_features)}")

    # ── Step 2: Transform CAMA features ─────────────────────────────────────
    print("\n[2/3] Processing features…")
    props = []
    skipped = 0
    for i, f in enumerate(raw_features):
        # GeoJSON uses "properties"; ArcGIS JSON uses "attributes" — support both
        a = f.get("properties") or f.get("attributes") or {}
        # Skip secondary buildings on same lot
        if a.get("BLDG_NUM") not in (None, 0, 1):
            skipped += 1
            continue
        # Get coordinates — handle GeoJSON and legacy ArcGIS JSON formats
        geom = f.get("geometry")
        if not geom:
            skipped += 1
            continue
        lat = lng = None
        gtype = geom.get("type", "")
        coords_field = geom.get("coordinates")
        if gtype == "Point" and coords_field and len(coords_field) >= 2:
            lng, lat = coords_field[0], coords_field[1]
        elif gtype == "Polygon" and coords_field and coords_field[0]:
            c = poly_center([coords_field[0]])
            if c: lng, lat = c
        elif gtype == "MultiPolygon" and coords_field and coords_field[0] and coords_field[0][0]:
            c = poly_center([coords_field[0][0]])
            if c: lng, lat = c
        elif geom.get("x") is not None and geom.get("y") is not None:
            lng, lat = geom["x"], geom["y"]
        elif geom.get("rings"):
            c = poly_center(geom["rings"])
            if c: lng, lat = c
        if lat is None or lng is None:
            skipped += 1
            continue

        # Skip if coordinates look wrong for DC (rough bounding box)
        if not (38.7 < lat < 39.1 and -77.2 < lng < -76.9):
            skipped += 1
            continue

        ssl = (a.get("SSL") or "").strip()
        usecode = a.get("USECODE") or 0
        is_vacant = usecode in (89, 91, 92)
        use_type = "Vacant Lot" if is_vacant else USECODE_MAP.get(usecode, "Multifamily")

        sale_ts = a.get("SALEDATE")
        sale_date = ""
        if sale_ts:
            try:
                dt = datetime.fromtimestamp(sale_ts / 1000, tz=timezone.utc)
                sale_date = dt.strftime("%b %Y")
            except Exception:
                pass

        p = {
            "id": f"P{i}",
            "ssl": ssl,
            "address": f"SSL: {ssl}" if ssl else "DC Property",
            "ward": 0,
            "neighborhood": "DC",
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "num_units":  int(a.get("NUM_UNITS") or 0),
            "stories":    int(a.get("STORIES")   or 0),
            "year_built": int(a.get("AYB")        or 0) or None,
            "gba_sqft":   int(a.get("GBA")        or 0),
            "land_sqft":  int(a.get("LANDAREA")   or 0),
            "sale_price": int(a.get("PRICE")      or 0),
            "sale_date":  sale_date,
            "use_type":   use_type,
            "grade":      a.get("GRADE_D") or "",
            "zoning":     "",
            "owner":      "",
            "owner_address": "",
            "assessment": 0,
            "tax": 0,
        }
        p["score"] = compute_score(p)
        props.append(p)

    print(f"  Valid: {len(props)}, Skipped: {skipped}")

    # ── Step 3: Enrich with Tax Extract (owner, address, ward, zoning) ──────
    print(f"\n[3/3] Enriching {len(props)} properties with owner/tax data…")
    ssl_index = {p["ssl"]: p for p in props if p["ssl"]}
    ssls = list(ssl_index.keys())
    enriched = 0

    for batch_start in range(0, len(ssls), 100):
        batch = ssls[batch_start:batch_start + 100]
        quoted = ",".join(f"'{s}'" for s in batch)
        params = {
            "where": f"SSL IN ({quoted})",
            "outFields": "SSL,OWNERNAME,ADDRESS,ZIPCODE,WARD,ZONING,ASSESSMENT,TAX",
            "returnGeometry": "false",
            "outSR": "4326",
            "resultRecordCount": 200,
            "f": "json",
        }
        try:
            full_url = f"{TAX_URL}?{urllib.parse.urlencode(params)}"
            with urllib.request.urlopen(full_url, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            for feat in data.get("features", []):
                a = feat.get("attributes", {})
                ssl = (a.get("SSL") or "").strip()
                if ssl in ssl_index:
                    p = ssl_index[ssl]
                    if a.get("OWNERNAME"):  p["owner"]          = a["OWNERNAME"]
                    if a.get("ADDRESS"):    p["owner_address"]  = a["ADDRESS"]
                    if a.get("WARD"):       p["ward"]           = int(a["WARD"])
                    if a.get("ZONING"):     p["zoning"]         = a["ZONING"]
                    if a.get("ASSESSMENT"): p["assessment"]     = int(a["ASSESSMENT"])
                    if a.get("TAX"):        p["tax"]            = int(a["TAX"])
                    enriched += 1
        except Exception as e:
            print(f"  ⚠ Tax batch {batch_start//100 + 1} failed: {e}")
        progress = min(batch_start + 100, len(ssls))
        print(f"  Tax data: {progress}/{len(ssls)} SSLs processed…", end="\r")
        time.sleep(0.2)

    # Re-score with enriched data
    for p in props:
        p["score"] = compute_score(p)

    print(f"\n  ✓ Enriched {enriched} properties with owner/tax data")

    # ── Save output ──────────────────────────────────────────────────────────
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "properties.json")

    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(props),
        "source": "DC Open Data — CAMA Residential + ITS Tax Extract",
        "properties": props,
    }

    with open(out_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\n{'=' * 60}")
    print(f"✓ Saved {len(props)} properties → {out_path}")
    print(f"  File size: {size_kb:.0f} KB")
    print(f"  Updated:   {output['updated']}")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Open GitHub Desktop")
    print("  2. Commit 'data/properties.json'")
    print("  3. Push to GitHub → tgrepe.com updates automatically")

if __name__ == "__main__":
    main()
