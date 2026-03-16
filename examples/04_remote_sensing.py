"""
Example 04 — Remote Sensing Integration
=========================================
Demonstrates how to integrate user-extracted remote sensing values
(NDVI, LST, SAR soil moisture index) alongside in-situ sensors.

The user extracts pixel values themselves (e.g. from Google Earth Engine,
SNAP, QGIS, or any RS processing tool) and declares them in the manifest.

Run:  python examples/04_remote_sensing.py
"""

from digital_pedon import build_pedon, register_method
from digital_pedon.sensor import SensorManifest, SensorLayer

# ── 1. Build pedon ────────────────────────────────────────────────────────────
pedon = build_pedon({
    "site_name": "Remote Sensing Demo",
    "latitude":   51.505,
    "longitude":   4.469,
    "horizons": [
        {"designation":"Ap","depth_top_cm":0, "depth_bottom_cm":28,"soil_type":"loamy_topsoil"},
        {"designation":"Bw","depth_top_cm":28,"depth_bottom_cm":70,"soil_type":"clay_subsoil"},
    ],
})

# ── 2. Declare mixed-origin manifest ──────────────────────────────────────────
# In-situ sensors AND remote sensing bands — all declared the same way.
manifest = SensorManifest.from_dict({
    "site_name": "Remote Sensing Demo",
    "pedon_id":  "RS_DEMO",
    "fields": [
        # ── In-situ (hourly) ─────────────────────────────────────────────
        {
            "source_key":      "VWC_Ap_pct",
            "canonical_key":   "volumetric_water_content",
            "unit":            "%",
            "horizon_id":      "Ap",
            "depth_cm":        15,
            "sampling_rate_s": 3600,
            "sensor_type":     "METER TEROS 11",
            "data_origin":     "in_situ",
        },
        {
            "source_key":      "Temp_Ap_C",
            "canonical_key":   "soil_temperature_c",
            "unit":            "degC",
            "horizon_id":      "Ap",
            "depth_cm":        15,
            "sampling_rate_s": 3600,
            "sensor_type":     "METER TEROS 11",
            "data_origin":     "in_situ",
        },

        # ── Sentinel-2 MSI (10-day revisit) ──────────────────────────────
        # User extracts pixel values at field centroid with GEE / SNAP / QGIS.
        # Values are already cloud-masked L2A surface reflectance.
        {
            "source_key":      "NDVI_S2",
            "canonical_key":   "ndvi",
            "unit":            "dimensionless",   # already computed ratio
            "horizon_id":      "Ap",
            "depth_cm":        0,
            "sampling_rate_s": 864000,            # ~10 days
            "sensor_type":     "Sentinel-2 MSI (B4/B8 ratio)",
            "data_origin":     "remote_sensing",
            "description":     "Extracted at field centroid, cloud-masked, 10 m pixel",
            "optional":        True,
        },
        {
            "source_key":      "B8_DN",           # raw Sentinel-2 DN value
            "canonical_key":   "surface_reflectance_nir",
            "unit":            "DN",              # auto-converted: /10000 → reflectance
            "horizon_id":      "Ap",
            "depth_cm":        0,
            "sampling_rate_s": 864000,
            "sensor_type":     "Sentinel-2 MSI Band 8 (NIR)",
            "data_origin":     "remote_sensing",
            "optional":        True,
        },

        # ── Sentinel-1 SAR (5-day revisit) ────────────────────────────────
        {
            "source_key":      "SMI_S1",
            "canonical_key":   "soil_moisture_index",
            "unit":            "dimensionless",
            "horizon_id":      "Ap",
            "depth_cm":        0,
            "sampling_rate_s": 432000,            # ~5 days
            "sensor_type":     "Sentinel-1 SAR derived SMI",
            "data_origin":     "remote_sensing",
            "description":     "Change detection SMI from VV/VH ratio",
            "optional":        True,
        },

        # ── Landsat 8/9 TIRS (16-day revisit) ────────────────────────────
        {
            "source_key":      "LST_C",
            "canonical_key":   "land_surface_temperature_c",
            "unit":            "degC",
            "horizon_id":      "Ap",
            "depth_cm":        0,
            "sampling_rate_s": 1382400,           # ~16 days
            "sensor_type":     "Landsat 8 Band 10 TIRS",
            "data_origin":     "remote_sensing",
            "description":     "Surface temperature, emissivity-corrected",
            "optional":        True,
        },

        # ── FAO MODIS ET (8-day composite) ───────────────────────────────
        {
            "source_key":      "ET_mm_day",
            "canonical_key":   "evapotranspiration_mm_day",
            "unit":            "mm/day",
            "horizon_id":      "Ap",
            "depth_cm":        0,
            "sampling_rate_s": 691200,            # 8 days
            "sensor_type":     "MOD16A2 MODIS 500m ET",
            "data_origin":     "remote_sensing",
            "optional":        True,
        },
    ],
})

print(manifest.summary())

# ── 3. Register a custom solver that uses RS data ─────────────────────────────
def water_deficit_index(params: dict, state: dict) -> dict:
    """
    Simple water deficit index combining in-situ θ and remote sensing NDVI.
    Both sources are in canonical state after ingestion.
    """
    theta    = state.get("volumetric_water_content", state.get("theta", 0))
    theta_s  = params.get("theta_s", 0.45)
    ndvi     = state.get("ndvi")           # may be None if RS not available yet
    lst      = state.get("land_surface_temperature_c")
    smi      = state.get("soil_moisture_index")

    wdi = 1.0 - (theta / max(theta_s, 1e-6))   # 0 = saturated, 1 = bone dry

    # Boost estimate if we have multiple RS sources
    note = "in_situ only"
    if ndvi is not None:
        # Low NDVI + high WDI → confirmed stress
        ndvi_stress = ndvi < 0.35
        wdi = wdi * 0.7 + (1 - max(0, ndvi)) * 0.3
        note = f"in_situ + NDVI={ndvi:.3f}"
    if smi is not None:
        # Blend with SAR SMI
        wdi = wdi * 0.6 + (1 - smi) * 0.4
        note += f" + SMI={smi:.3f}"

    return {
        "water_deficit_index": round(wdi, 4),
        "wdi_data_sources":    note,
        "drought_alert":       wdi > 0.65,
    }

register_method({
    "name":        "water_deficit_index",
    "fn":           water_deficit_index,
    "description": "Multi-source water deficit index (in-situ + RS fusion)",
    "inputs":      ["volumetric_water_content", "theta_s", "ndvi",
                    "soil_moisture_index", "land_surface_temperature_c"],
    "outputs":     ["water_deficit_index", "wdi_data_sources", "drought_alert"],
})

# ── 4. Ingest: in-situ only ───────────────────────────────────────────────────
layer = SensorLayer(manifest)

print("\n── Reading 1: in-situ only ──")
r1 = layer.ingest({
    "VWC_Ap_pct": 28.5,
    "Temp_Ap_C":  14.2,
})
result1 = pedon.update_sync(r1)
print(f"  WDI = {result1['derived']['water_deficit_index']:.3f} "
      f"({result1['derived']['wdi_data_sources']})")
print(f"  Drought alert: {result1['derived']['drought_alert']}")

# ── 5. Ingest: in-situ + Sentinel-2 + SAR ────────────────────────────────────
print("\n── Reading 2: in-situ + Sentinel-2 NDVI + SAR SMI ──")
r2 = layer.ingest({
    "VWC_Ap_pct": 18.2,    # dry period
    "Temp_Ap_C":  22.5,
    "NDVI_S2":    0.28,    # stressed vegetation
    "SMI_S1":     0.31,    # SAR confirms dry
    "LST_C":      38.4,    # hot surface
    "B8_DN":      2850,    # Sentinel-2 Band 8 DN → auto /10000 → 0.285 reflectance
})
result2 = pedon.update_sync(r2)
print(f"  WDI = {result2['derived']['water_deficit_index']:.3f} "
      f"({result2['derived']['wdi_data_sources']})")
print(f"  NIR reflectance = {r2.get('surface_reflectance_nir', '?')}")
print(f"  Drought alert: {result2['derived']['drought_alert']}")

# ── 6. Show what's stored in state ───────────────────────────────────────────
print("\n── All keys in Ap state after RS ingestion ──")
state = pedon.snapshot()["horizons"][0]["state"]
for k, v in sorted(state.items()):
    if k != "derived":
        print(f"  {k}: {v}")
print("  derived keys:", list(state.get("derived", {}).keys()))
