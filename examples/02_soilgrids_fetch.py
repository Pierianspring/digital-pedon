"""
Example 02 — Fetch Soil Properties from SoilGrids
==================================================
Build a pedon automatically from GPS coordinates.
Requires internet connection (one call to rest.isric.org).

Run:  python examples/02_soilgrids_fetch.py
"""

from digital_pedon import build_pedon, print_snapshot
from digital_pedon.sources import fetch_soil_profile

# ── Fetch from SoilGrids using only coordinates ───────────────────────────────
print("Fetching soil profile for Ghent, Belgium (51.05°N, 3.72°E)...")
try:
    config = fetch_soil_profile(latitude=51.05, longitude=3.72)
    print(f"  Source: {config.get('source','?')}")
    print(f"  Horizons: {[h['designation'] for h in config['horizons']]}")

    # ── Override one value with a local measurement ───────────────────────────
    for hz in config["horizons"]:
        if hz["designation"] == "Ap":
            hz["organic_carbon_pct"] = 3.8   # your lab measurement wins

    # ── Build and run ─────────────────────────────────────────────────────────
    pedon = build_pedon({**config, "site_name": "Ghent Research Site"})

    pedon.update_sync({
        "horizon_id":               "Ap",
        "volumetric_water_content":  0.34,
        "soil_temperature_c":        11.5,
    })

    print_snapshot(pedon)

except RuntimeError as e:
    print(f"Network unavailable: {e}")
    print("(Run with internet access to see SoilGrids data)")

    # Offline fallback — same workflow, manual properties
    print("\nFalling back to manual profile...")
    pedon = build_pedon({
        "site_name": "Ghent (offline fallback)",
        "latitude": 51.05, "longitude": 3.72,
        "horizons": [
            {"designation":"Ap","depth_top_cm":0,"depth_bottom_cm":25,
             "soil_type":"loamy_topsoil","organic_carbon_pct":3.8},
            {"designation":"Bw","depth_top_cm":25,"depth_bottom_cm":65,
             "soil_type":"clay_subsoil"},
            {"designation":"C","depth_top_cm":65,"depth_bottom_cm":120,
             "soil_type":"sandy_loam"},
        ],
    })
    pedon.update_sync({"horizon_id":"Ap",
                       "volumetric_water_content":0.34,"soil_temperature_c":11.5})
    print_snapshot(pedon)
