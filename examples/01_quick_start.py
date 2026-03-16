"""
Example 01 — Quick Start
========================
Build a two-horizon pedon, feed one sensor reading, inspect results.

Run:  python examples/01_quick_start.py
"""

from digital_pedon import build_pedon, register_method, print_snapshot

# ── 1. Build the digital twin ─────────────────────────────────────────────────
pedon = build_pedon({
    "site_name": "Quick-Start Site",
    "latitude":   51.505,
    "longitude":   4.469,
    "thresholds": {
        "theta_saturation": 0.44,
        "theta_wilting_pt": 0.09,
    },
    "horizons": [
        {
            "designation":    "Ap",
            "depth_top_cm":   0,
            "depth_bottom_cm":28,
            "soil_type":      "loamy_topsoil",
            # Override one property from the built-in soil type:
            "organic_carbon_pct": 3.2,
        },
        {
            "designation":    "Bw",
            "depth_top_cm":   28,
            "depth_bottom_cm":70,
            "soil_type":      "clay_subsoil",
        },
    ],
})

print(f"Pedon built: {pedon}")

# ── 2. Register a simple custom solver ───────────────────────────────────────
def plant_available_water(params: dict, state: dict) -> dict:
    """
    Plant-available water as fraction of field capacity.
    Returns 0 (dry) to 1 (at field capacity).
    """
    theta    = state.get("volumetric_water_content", state.get("theta", 0))
    theta_fc = params.get("theta_s", 0.40) * 0.7   # rough field capacity
    theta_wp = params.get("theta_r", 0.08) * 1.2
    paw = max(0.0, min(1.0, (theta - theta_wp) / max(theta_fc - theta_wp, 1e-6)))
    return {
        "plant_available_water_fraction": round(paw, 3),
        "water_stress": paw < 0.20,
    }

register_method({
    "name":        "plant_available_water",
    "fn":           plant_available_water,
    "description": "PAW fraction between wilting point and field capacity",
    "inputs":      ["volumetric_water_content", "theta_s", "theta_r"],
    "outputs":     ["plant_available_water_fraction", "water_stress"],
})

# ── 3. Feed sensor readings ───────────────────────────────────────────────────
result_ap = pedon.update_sync({
    "horizon_id":               "Ap",
    "volumetric_water_content":  0.31,    # cm³/cm³
    "soil_temperature_c":        14.2,    # °C
    "electrical_conductivity_ds_m": 0.4, # dS/m
})

import json
print(json.dumps(result_ap, indent=2, default=str))

result_bw = pedon.update_sync({
    "horizon_id":               "Bw",
    "volumetric_water_content":  0.38,
    "soil_temperature_c":        12.8,
})

# ── 4. Inspect results ────────────────────────────────────────────────────────
print("── Ap horizon results ──")
print(f"  θ   = {result_ap['theta']:.3f} cm³/cm³")
print(f"  ψ   = {result_ap['derived'].get('matric_potential_cm', 0):.1f} cm H₂O")
print(f"  K(θ)= {result_ap['derived'].get('hydraulic_conductivity_cm_day', 0):.3f} cm/day")
flux = result_ap['derived'].get('flux_down_cm_day', 0)
print(f"  q↓  = {flux:+.4f} cm/day  ({'draining' if flux < 0 else 'upward/flat'})")
print(f"  PAW = {result_ap['derived'].get('plant_available_water_fraction', 0):.0%}")
print(f"  CO₂ = {result_ap['derived'].get('soil_respiration_mg_C_cm3_day', 0):.4f} mg C/cm³/day")
# ── 5. Full snapshot ──────────────────────────────────────────────────────────
print_snapshot(pedon)

# ── 6. Query history ─────────────────────────────────────────────────────────
history = pedon.query_history("Ap")
print(f"\nAp history: {len(history)} record(s)")
print(f"  Last reading: θ={history[-1]['theta']:.3f}")

# ── 7. JSON-LD export ────────────────────────────────────────────────────────
print("\nJSON-LD export (first 300 chars):")
print(pedon.to_json()[:300], "...")
