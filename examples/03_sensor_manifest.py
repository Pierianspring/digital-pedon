"""
Example 03 — Sensor Manifest (BYOD)
=====================================
Declare your sensors in a manifest. Feed raw logger data — the framework
converts units and renames keys automatically.

Covers: in-situ multi-depth sensors, unit conversion, CSV batch ingestion.

Run:  python examples/03_sensor_manifest.py
"""

import csv
import io
import time

from digital_pedon import build_pedon
from digital_pedon.sensor import SensorManifest, SensorLayer, ManifestBuilder

# ── 1. Build the pedon ────────────────────────────────────────────────────────
pedon = build_pedon({
    "site_name": "Multi-Depth Logger Site",
    "horizons": [
        {"designation":"Ap","depth_top_cm":0, "depth_bottom_cm":28,"soil_type":"loamy_topsoil"},
        {"designation":"Bw","depth_top_cm":28,"depth_bottom_cm":70,"soil_type":"clay_subsoil"},
        {"designation":"C", "depth_top_cm":70,"depth_bottom_cm":130,"soil_type":"sandy_loam"},
    ],
})

# ── 2a. Build manifest in Python (no YAML file needed) ───────────────────────
manifest = (
    ManifestBuilder("Multi-Depth Logger Site", "SITE_001")

    # Ap horizon — TEROS 11, reading water content in %, temperature in °F
    .add_insitu(source_key="VWC_15cm_pct", canonical_key="volumetric_water_content",
                unit="%", horizon_id="Ap", depth_cm=15, sampling_rate_s=3600,
                sensor_type="METER TEROS 11",
                description="Ap horizon TDR probe, installed 2025-03-01")
    .add_insitu(source_key="Temp_15cm_F",  canonical_key="soil_temperature_c",
                unit="degF", horizon_id="Ap", depth_cm=15, sampling_rate_s=3600,
                sensor_type="METER TEROS 11")
    .add_insitu(source_key="EC_15cm_uScm", canonical_key="electrical_conductivity_ds_m",
                unit="μS/cm", horizon_id="Ap", depth_cm=15, sampling_rate_s=3600,
                sensor_type="METER TEROS 11")

    # Bw horizon — Sentek, reading water content in m³/m³, temperature in °C
    .add_insitu(source_key="VWC_45cm",   canonical_key="volumetric_water_content",
                unit="m3/m3", horizon_id="Bw", depth_cm=45, sampling_rate_s=3600,
                sensor_type="Sentek Drill & Drop")
    .add_insitu(source_key="Temp_45cm",  canonical_key="soil_temperature_c",
                unit="degC",  horizon_id="Bw", depth_cm=45, sampling_rate_s=3600,
                sensor_type="Sentek Drill & Drop")

    # C horizon — manual tensiometer read (kPa → cm H₂O auto-converted)
    .add_manual(source_key="Tension_90cm_kPa", canonical_key="matric_potential_cm",
                unit="kPa", horizon_id="C", depth_cm=90,
                description="Jet-fill tensiometer, read weekly",
                optional=True)

    .build()
)

print(manifest.summary())

# ── 2b. Optionally save the manifest to YAML for reuse ───────────────────────
try:
    yaml_str = manifest.to_yaml()
    print("\nManifest YAML (first 500 chars):")
    print(yaml_str[:500])
except ImportError:
    print("\n(Install pyyaml to save manifest as YAML: pip install pyyaml)")

# ── 2c. Or save as JSON ───────────────────────────────────────────────────────
json_str = manifest.to_json()
print(f"\nManifest JSON: {len(json_str)} bytes")

# ── 3. Create the sensor layer ────────────────────────────────────────────────
layer = SensorLayer(manifest, validate_range=True)

# ── 4. Ingest a single raw reading (one multi-depth row) ─────────────────────
raw_row = {
    "VWC_15cm_pct":    28.5,    # % → 0.285 cm³/cm³
    "Temp_15cm_F":     59.4,    # °F → 15.2 °C
    "EC_15cm_uScm":    380.0,   # μS/cm → 0.38 dS/m
    "VWC_45cm":        0.382,   # m³/m³ → unchanged
    "Temp_45cm":       12.1,    # °C → unchanged
    # Tension_90cm_kPa is absent — optional, so silently skipped
}

readings = layer.split_by_horizon(raw_row)
print(f"\nSplit into {len(readings)} horizon reading(s):")
for r in readings:
    print(f"  {r}")
    pedon.update_sync(r)

# ── 5. CSV batch ingestion ────────────────────────────────────────────────────
# Simulate a logger CSV file
FAKE_CSV = """\
timestamp,VWC_15cm_pct,Temp_15cm_F,EC_15cm_uScm,VWC_45cm,Temp_45cm
1741600000,27.1,58.2,370.0,0.375,11.8
1741603600,27.8,58.9,372.0,0.378,12.0
1741607200,28.5,59.4,380.0,0.382,12.1
1741610800,29.2,60.1,385.0,0.387,12.3
"""

print("\n── CSV batch ingestion ──")
reader = csv.DictReader(io.StringIO(FAKE_CSV))
n = 0
for row in reader:
    ts  = float(row.pop("timestamp"))
    raw = {k: float(v) for k, v in row.items()}
    for reading in layer.split_by_horizon(raw, timestamp=ts):
        pedon.update_sync(reading)
        n += 1

print(f"Processed {n} horizon-readings from CSV.")
print(f"Ap history: {len(pedon.query_history('Ap'))} records")
print(f"Bw history: {len(pedon.query_history('Bw'))} records")

# ── 6. Final state ────────────────────────────────────────────────────────────
snap = pedon.snapshot()
for hz in snap["horizons"]:
    st = hz["state"]
    print(f"\n{hz['designation']} ({hz['depth_top_cm']}–{hz['depth_bottom_cm']} cm)")
    print(f"  θ   = {st.get('volumetric_water_content', st.get('theta', '?')):.3f} cm³/cm³")
    print(f"  ψ   = {st.get('matric_potential_cm', '?')} cm H₂O")
    print(f"  q↓  = {st.get('flux_down_cm_day', '?'):+.4f} cm/day")
