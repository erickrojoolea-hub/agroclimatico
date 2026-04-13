#!/usr/bin/env python3
"""
Convert PVsyst SIT/MET files to CSV format for the agroclimatico app.

1. Parses all .SIT files to extract site metadata (lat, lon, alt, monthly values)
2. Decodes .MET files (base-62 encoded hourly data) to CSV
3. Calculates pairwise distances and optimal search radii (15km default, midpoint split)
4. Generates sites_db.json with all ~354 site entries
"""

import os
import re
import json
import math
import sys
import unicodedata
from datetime import datetime, timedelta
from collections import defaultdict


def normalize_name(s):
    """Normalize Unicode to NFC for consistent comparison."""
    return unicodedata.normalize('NFC', s)

# ── Paths ──────────────────────────────────────────────────────────────────────
GDRIVE_DATA = (
    "/Users/erickrojoolea/Library/CloudStorage/"
    "GoogleDrive-erick@toroea.com/Unidades compartidas/"
    "Toro Energy/Nuevos negocios/Agro/Data"
)
SITES_DIR = os.path.join(GDRIVE_DATA, "Sites")
METEO_DIR = os.path.join(GDRIVE_DATA, "Meteo")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_DIR, "data", "pvsyst")
DB_OUTPUT = os.path.join(PROJECT_DIR, "data", "sites_db.json")

# ── Base-62 Decoder ───────────────────────────────────────────────────────────
CHARSET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
CHAR_MAP = {c: i for i, c in enumerate(CHARSET)}


def base62_decode(s):
    """Decode a base-62 encoded string to integer."""
    result = 0
    for c in s:
        result = result * 62 + CHAR_MAP[c]
    return result


def parse_tokens(data_str):
    """Parse +val+val-val... into list of signed integers."""
    tokens = []
    current = ''
    sign = 1
    for ch in data_str:
        if ch in ('+', '-'):
            if current:
                tokens.append(sign * base62_decode(current))
            current = ''
            sign = 1 if ch == '+' else -1
        else:
            current += ch
    if current:
        tokens.append(sign * base62_decode(current))
    return tokens


# ── SIT File Parser ──────────────────────────────────────────────────────────
def parse_sit_file(filepath):
    """Parse a PVsyst .SIT file and return site metadata dict."""
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read().replace('\r', '')

    def extract(key, default=None, as_float=True):
        m = re.search(rf'{key}=(.+)', content)
        if m:
            val = m.group(1).strip()
            if as_float:
                try:
                    return float(val)
                except ValueError:
                    return default
            return val
        return default

    def extract_monthly(key):
        """Extract 12 monthly values from comma-separated line."""
        m = re.search(rf'{key}=([\d.,\-\s]+)', content)
        if m:
            vals = [v.strip() for v in m.group(1).split(',') if v.strip()]
            try:
                floats = [float(v) for v in vals]
                return floats[:12]  # skip annual sum
            except ValueError:
                pass
        return None

    site = {
        'name': extract('Site', '', as_float=False),
        'country': extract('Country', 'Chile', as_float=False),
        'lat': extract('Latitude'),
        'lon': extract('Longitude'),
        'alt': extract('Altitude', 0),
        'timezone': extract('TimeZone', -4),
        'source': extract('Source', '', as_float=False),
        'global_h': extract_monthly('GlobalH'),
        'diffuse_h': extract_monthly('DiffuseH'),
        't_amb': extract_monthly('TAmb'),
        'wind_vel': extract_monthly('WindVel'),
        'rel_hum': extract_monthly('RelHum'),
    }
    return site


# ── MET File Parser & CSV Converter ─────────────────────────────────────────
def parse_met_to_csv(met_filepath, output_csv_path):
    """
    Parse a PVsyst .MET file and write standard CSV output.
    Format: date,GlobHor,DiffHor,BeamHor,T_Amb,WindVel
    """
    with open(met_filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read().replace('\r', '')

    lines = content.split('\n')

    # Parse site metadata from embedded SiteMet section
    site_name = ''
    source = ''
    m = re.search(r'SiteM=(.+)', content)
    if m:
        site_name = m.group(1).strip()
    m = re.search(r'SourceM=(.+)', content)
    if m:
        source = m.group(1).strip()

    # Parse all 5 variables
    variables = {}
    current_var = None
    current_wr = None
    current_mode = None
    current_days = []

    for line in lines:
        line = line.strip()
        m = re.match(r'VarSimul_\d+=(\w+)', line)
        if m:
            if current_var:
                variables[current_var] = {
                    'wr': current_wr,
                    'mode': current_mode,
                    'days': current_days
                }
            current_var = m.group(1)
            current_wr = None
            current_mode = None
            current_days = []
            continue
        if current_var:
            if 'WRFactor=' in line:
                current_wr = float(line.split('=')[1])
            elif 'VarMode=' in line:
                current_mode = line.split('=')[1]
            elif line.startswith('Day_'):
                current_days.append(line)
        if 'End of the List' in line and current_var:
            variables[current_var] = {
                'wr': current_wr,
                'mode': current_mode,
                'days': current_days
            }

    required_vars = ['GlobHor', 'DiffHor', 'T_Amb', 'WindVel']
    for v in required_vars:
        if v not in variables:
            raise ValueError(f"Variable {v} not found in {met_filepath}")

    # Decode all variables into {(month, day): [24 hourly values]}
    decoded = {}
    for var_name in required_vars:
        var = variables[var_name]
        wr = var['wr']
        mode = var['mode']
        var_data = {}

        for day_line in var['days']:
            parts = day_line.split('=', 1)
            day_key = parts[0]  # Day_DDMM
            dd = int(day_key[4:6])
            mm = int(day_key[6:8])

            raw_tokens = parse_tokens(parts[1])

            # Convert to physical values
            hourly = []
            for val in raw_tokens:
                if mode == 'Accum':
                    # Irradiance: val / (WRFactor * 3.6) gives kWh/m²
                    # Convert to W/m² (average over hour): kWh/m² * 1000
                    physical = val / wr / 3.6 * 1000  # W/m²
                else:
                    # Temperature, wind: val / WRFactor
                    physical = val / wr
                hourly.append(physical)

            var_data[(mm, dd)] = hourly

        decoded[var_name] = var_data

    # Build CSV
    # Date format: DD/MM/YY HH:MM (matching existing PVsyst CSV format)
    # Year: 1990 (matching existing files)
    csv_lines = []
    # Header matching PVsyst CSV export format
    csv_lines.append(f"# PVsyst 8.0.21 - {site_name}")
    csv_lines.append(f"# Source: {source}")
    csv_lines.append("# Converted from MET by agroclimatico converter")
    csv_lines.append("date,GlobHor,DiffHor,BeamHor,T_Amb,WindVel")
    csv_lines.append("    ,W/m²,W/m²,W/m²,°C,m/s")

    # Generate 8760 hours (Jan 1 00:00 to Dec 31 23:00)
    base_date = datetime(1990, 1, 1, 0, 0)
    hour_count = 0

    for day_of_year in range(365):
        dt = base_date + timedelta(days=day_of_year)
        mm = dt.month
        dd = dt.day

        for hh in range(24):
            date_str = f"{dd:02d}/{mm:02d}/90 {hh:02d}:00"

            ghi = decoded['GlobHor'].get((mm, dd), [0]*24)
            dhi = decoded['DiffHor'].get((mm, dd), [0]*24)
            tamb = decoded['T_Amb'].get((mm, dd), [15]*24)
            wind = decoded['WindVel'].get((mm, dd), [2]*24)

            g = ghi[hh] if hh < len(ghi) else 0
            d = dhi[hh] if hh < len(dhi) else 0
            t = tamb[hh] if hh < len(tamb) else 15
            w = wind[hh] if hh < len(wind) else 2

            # BeamHor = GlobHor - DiffHor (can't be negative)
            b = max(0, g - d)

            csv_lines.append(f"{date_str},{g:.1f},{d:.1f},{b:.1f},{t:.2f},{w:.4f}")
            hour_count += 1

    with open(output_csv_path, 'w', encoding='latin-1') as f:
        f.write('\n'.join(csv_lines))

    return hour_count


# ── Haversine Distance ──────────────────────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    """Distance in km between two geographic points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))


# ── Calculate Optimal Radii ─────────────────────────────────────────────────
def calculate_radii(sites, max_radius=15.0):
    """
    Calculate optimal search radius for each site.
    Default: max_radius km. If two sites are closer than 2*max_radius,
    split the overlap at the midpoint.
    """
    names = list(sites.keys())
    radii = {name: max_radius for name in names}

    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            n1, n2 = names[i], names[j]
            s1, s2 = sites[n1], sites[n2]

            if s1['lat'] is None or s2['lat'] is None:
                continue

            dist = haversine_km(s1['lat'], s1['lon'], s2['lat'], s2['lon'])

            if dist < 2 * max_radius:
                half = dist / 2.0
                radii[n1] = min(radii[n1], half)
                radii[n2] = min(radii[n2], half)

    return radii


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("PVsyst SIT/MET → CSV Converter")
    print("=" * 70)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 1: Parse all SIT files
    print(f"\n{'='*70}")
    print("STEP 1: Parsing SIT files")
    print(f"{'='*70}")

    sit_files = sorted([f for f in os.listdir(SITES_DIR) if f.endswith('.SIT')])
    print(f"  Found {len(sit_files)} SIT files")

    sites = {}
    errors = []
    for i, sit_file in enumerate(sit_files):
        try:
            site = parse_sit_file(os.path.join(SITES_DIR, sit_file))
            name = normalize_name(site['name']) if site['name'] else ''
            if not name:
                name = normalize_name(sit_file.replace('_MN82.SIT', '').replace('.SIT', ''))
            sites[name] = site
            if (i + 1) % 50 == 0:
                print(f"    Parsed {i+1}/{len(sit_files)}...")
        except Exception as e:
            errors.append((sit_file, str(e)))

    print(f"  Successfully parsed: {len(sites)} sites")
    if errors:
        print(f"  Errors: {len(errors)}")
        for f, e in errors[:5]:
            print(f"    - {f}: {e}")

    # Step 2: Convert MET files to CSV
    print(f"\n{'='*70}")
    print("STEP 2: Converting MET files to CSV")
    print(f"{'='*70}")

    met_files = sorted([f for f in os.listdir(METEO_DIR) if f.endswith('.MET')])
    print(f"  Found {len(met_files)} MET files")

    converted = 0
    met_errors = []
    site_to_csv = {}

    for i, met_file in enumerate(met_files):
        # Extract site name from filename: SiteName_MN82_SYN.MET
        site_name = normalize_name(met_file.replace('_MN82_SYN.MET', '').replace('_SYN.MET', '').replace('.MET', ''))

        # Clean filename for output
        safe_name = site_name.replace(' ', '_').replace('/', '_')
        csv_filename = f"{safe_name}.CSV"
        csv_path = os.path.join(OUTPUT_DIR, csv_filename)

        try:
            hours = parse_met_to_csv(
                os.path.join(METEO_DIR, met_file),
                csv_path
            )
            site_to_csv[site_name] = csv_filename
            converted += 1
            if (i + 1) % 50 == 0 or (i + 1) == len(met_files):
                print(f"    Converted {i+1}/{len(met_files)} ({hours} hours)")
        except Exception as e:
            met_errors.append((met_file, str(e)))

    print(f"  Successfully converted: {converted} files")
    if met_errors:
        print(f"  Errors: {len(met_errors)}")
        for f, e in met_errors[:5]:
            print(f"    - {f}: {e}")

    # Step 3: Calculate radii with overlap logic
    print(f"\n{'='*70}")
    print("STEP 3: Calculating optimal radii")
    print(f"{'='*70}")

    # Filter to Chile only (lat -17 to -56, lon -76 to -66)
    valid_sites = {
        k: v for k, v in sites.items()
        if v['lat'] is not None and v['lon'] is not None
        and -56 <= v['lat'] <= -17
        and -76 <= v['lon'] <= -66
    }
    non_chile = len(sites) - len(valid_sites) - len([v for v in sites.values() if v['lat'] is None])
    if non_chile > 0:
        print(f"  Filtered out {non_chile} non-Chile sites")
    radii = calculate_radii(valid_sites, max_radius=20.0)

    # Stats
    r_values = list(radii.values())
    r_below_15 = [r for r in r_values if r < 15.0]
    print(f"  Total sites with coordinates: {len(valid_sites)}")
    print(f"  Sites with reduced radius (overlap): {len(r_below_15)}")
    if r_below_15:
        print(f"  Min radius: {min(r_below_15):.2f} km")
        print(f"  Avg reduced radius: {sum(r_below_15)/len(r_below_15):.2f} km")

    # Step 4: Build sites database
    print(f"\n{'='*70}")
    print("STEP 4: Building sites database")
    print(f"{'='*70}")

    sites_db = {}
    matched = 0

    # Build NFC-normalized lookup for CSV matching
    nfc_csv_lookup = {}
    for met_name, csv_f in site_to_csv.items():
        key = normalize_name(met_name).lower().strip()
        nfc_csv_lookup[key] = csv_f
        # Also add underscore→space variant
        nfc_csv_lookup[key.replace('_', ' ')] = csv_f

    for name, site in valid_sites.items():
        # Find matching CSV file
        norm_key = normalize_name(name).lower().strip()
        csv_file = nfc_csv_lookup.get(norm_key)
        if not csv_file:
            csv_file = nfc_csv_lookup.get(norm_key.replace(' ', '_'))
        if not csv_file:
            # Try removing spaces/punctuation
            def _clean(s):
                return s.replace(' ', '').replace("'", '').replace('_', '').lower()
            for met_key, csv_f in nfc_csv_lookup.items():
                if _clean(met_key) == _clean(norm_key):
                    csv_file = csv_f
                    break

        # Extract HR values (convert from fraction to percentage)
        hr_pct = None
        if site.get('rel_hum'):
            hr_pct = [round(v * 100, 1) for v in site['rel_hum']]

        entry = {
            'lat': site['lat'],
            'lon': site['lon'],
            'alt': site['alt'] or 0,
            'radio_km': round(radii.get(name, 15.0), 2),
            'csv_file': csv_file,
            'fuente_meteo': site.get('source', 'Meteonorm 8.2'),
            'global_h_monthly': site.get('global_h'),
            't_amb_monthly': site.get('t_amb'),
            'wind_monthly': site.get('wind_vel'),
            'hr_monthly': hr_pct,
        }

        sites_db[name] = entry
        if csv_file:
            matched += 1

    print(f"  Sites in database: {len(sites_db)}")
    print(f"  Sites with CSV data: {matched}")

    # Save database
    with open(DB_OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(sites_db, f, ensure_ascii=False, indent=2)
    print(f"  Saved to: {DB_OUTPUT}")

    # Step 5: Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"  SIT files parsed: {len(sites)}")
    print(f"  MET files converted: {converted}")
    print(f"  CSV files in {OUTPUT_DIR}: {len(os.listdir(OUTPUT_DIR))}")
    print(f"  Sites database entries: {len(sites_db)}")
    print(f"  Sites with CSV match: {matched}")

    # Show geographic coverage
    lats = [s['lat'] for s in valid_sites.values()]
    lons = [s['lon'] for s in valid_sites.values()]
    print(f"\n  Geographic coverage:")
    print(f"    Latitude:  {min(lats):.4f} to {max(lats):.4f}")
    print(f"    Longitude: {min(lons):.4f} to {max(lons):.4f}")

    print(f"\n{'='*70}")
    print("All done!")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
