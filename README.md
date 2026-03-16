# Digital Pedon

**A dictionary-first digital twin framework for soil profile monitoring, physics-based inference, and global interoperability.**

[![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![No dependencies](https://img.shields.io/badge/dependencies-none-brightgreen.svg)]()
[![GLOSIS compliant](https://img.shields.io/badge/ontology-GLOSIS%2FFAO-brown.svg)](http://w3id.org/glosis)

---

## What is Digital Pedon?

Digital Pedon (DP) is a lightweight, stdlib-only Python framework that creates a **living digital twin of a soil profile**. It ingests sensor and remote sensing data, runs physics-based solvers, fires threshold alarms, and stores a time-series history — all operating on plain Python `dict`s with no external dependencies.

```
Physical soil profile          Digital twin
───────────────────            ────────────────────────────────────
O   horizon (0–5 cm)    →     θ = 0.42  ψ = −18 cm   q = +0.23 ↑
Ap  horizon (5–28 cm)   →     θ = 0.31  ψ = −94 cm   q = −0.55 ↓
Bw  horizon (28–65 cm)  →     θ = 0.28  ψ = −144 cm  q = −0.41 ↓
Btg horizon (65–110 cm) →  ⚠ θ = 0.44  ψ = −12 cm   q = +0.23 ↑  [SATURATION]
C   horizon (110–150 cm)→     θ = 0.22  ψ = −380 cm  q = −0.18 ↓
```

---

## Key Features

- **Dictionary-first**: everything in, everything out — plain `dict`s, no classes to learn
- **Zero dependencies**: pure Python 3.9+ standard library only
- **BYOD sensor layer**: declare your sensors in a YAML manifest; the framework converts units and renames keys automatically
- **Remote sensing support**: NDVI, LST, SAR soil moisture, surface reflectance — declared the same way as in-situ sensors
- **Physics-based solvers**: Van Genuchten, Darcy-Buckingham, De Vries heat equation, Q10 CO₂ flux — all built in; add your own with one function
- **GLOSIS-compliant ontology**: every property keyed to its FAO/OGC URI; exports JSON-LD for any global soil database
- **LLM-ready**: a JSON-LD context manifest and OpenAI-compatible tool schema let any LLM agent call the framework as native tools
- **SoilGrids integration**: fetch initial soil properties from the global 250 m database using only GPS coordinates

---

## Installation

```bash
git clone https://github.com/digitalpedon/framework
cd framework
# No pip install needed — zero external dependencies
# Optional: pip install pyyaml   (for YAML manifest loading)
```

To use as a package:
```bash
pip install -e .
```

---

## Quick Start

```python
from digital_pedon import build_pedon, register_method

# 1. Describe your soil profile
pedon = build_pedon({
    "site_name": "North Field",
    "latitude":   51.505,
    "longitude":   4.469,
    "horizons": [
        {"designation": "Ap", "depth_top_cm": 0,  "depth_bottom_cm": 28,
         "soil_type": "loamy_topsoil"},
        {"designation": "Bw", "depth_top_cm": 28, "depth_bottom_cm": 70,
         "soil_type": "clay_subsoil"},
    ],
})

# 2. Feed a sensor reading — get physics back instantly
result = pedon.update_sync({
    "horizon_id":               "Ap",
    "volumetric_water_content":  0.31,   # cm³/cm³
    "soil_temperature_c":        14.2,   # °C
})

print(result["matric_potential_cm"])          # → −94.7
print(result["hydraulic_conductivity_cm_day"])# → 3.84
print(result["flux_down_cm_day"])             # → −0.55  (draining)
```

---

## Sensor Manifest (BYOD)

Declare your sensors once in a YAML file — any sensor type, any unit, any data origin:

```yaml
# sensors.yaml
site_name: My Field Site
fields:
  - source_key:      VWC_Ap_pct        # column name in your logger CSV
    canonical_key:   volumetric_water_content
    unit:            "%"               # framework converts to cm³/cm³
    horizon_id:      Ap
    depth_cm:        15
    sampling_rate_s: 3600
    sensor_type:     METER TEROS 11
    data_origin:     in_situ

  - source_key:      Temp_Ap_F
    canonical_key:   soil_temperature_c
    unit:            degF              # framework converts to °C
    horizon_id:      Ap
    depth_cm:        15
    sensor_type:     METER TEROS 11
    data_origin:     in_situ

  - source_key:      NDVI_S2
    canonical_key:   ndvi
    unit:            dimensionless
    horizon_id:      Ap
    sampling_rate_s: 864000           # 10-day Sentinel-2 revisit
    sensor_type:     Sentinel-2 MSI
    data_origin:     remote_sensing
    optional:        true
```

```python
from digital_pedon.sensor import SensorManifest, SensorLayer

manifest = SensorManifest.from_yaml("sensors.yaml")
layer    = SensorLayer(manifest)

# Ingest raw logger row — automatic unit conversion + key renaming
readings = layer.split_by_horizon({
    "VWC_Ap_pct": 28.5,
    "Temp_Ap_F":  59.4,
    "NDVI_S2":    0.62,
})

for r in readings:
    pedon.update_sync(r)
```

---

## LLM Integration

Make the Digital Pedon callable as tools by any LLM agent:

```python
import anthropic, json
from digital_pedon.llm import PedonDispatcher, load_tools, load_context

dispatcher = PedonDispatcher()
tools      = load_tools()    # dp_tools.json → OpenAI-compatible schema
context    = load_context()  # dp_llm_context.json → system prompt

client   = anthropic.Anthropic()
messages = [{"role": "user", "content":
             "Build a two-horizon loamy pedon for Ghent, Belgium, "
             "then tell me its matric potential after a wet reading."}]

while True:
    resp = client.messages.create(
        model="claude-sonnet-4-6", max_tokens=2048,
        system=json.dumps(context), tools=tools, messages=messages,
    )
    if resp.stop_reason == "end_turn":
        print(resp.content[0].text)
        break
    for block in resp.content:
        if block.type == "tool_use":
            result = dispatcher.run(block.name, block.input)
            # ... return result to LLM ...
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│   SoilGrids (REST)    Sensor manifest (YAML/JSON)    Remote sensing     │
│        ↓                       ↓                           ↓            │
│   sources/soilgrids.py    sensor/sensor_layer.py    (user-extracted)    │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      STRUCTURAL LAYER (immutable)                       │
│   Horizon geometry · VG parameters · Texture · Thresholds · Metadata   │
│                          pedon_api.py :: build_pedon()                  │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ↓ update_sync()
┌─────────────────────────────────────────────────────────────────────────┐
│                       DYNAMIC LAYER (live state)                        │
│   KV Store (current state per horizon, overwritten each update)         │
│   Time-Series Log (append-only archive of all readings)                 │
│   Event System (5 alarms: saturation · wilting · temp · EC)            │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      FUNCTIONAL LAYER (solvers)                         │
│   Van Genuchten → Darcy-Buckingham → Heat equation → Q10 respiration   │
│              + any custom solver via register_method()                  │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                    ONTOLOGY LAYER (interoperability)                    │
│   GLOSIS canonical keys · OGC URIs · JSON-LD export · Alias resolver   │
│                         ontology/vocab.py                               │
└─────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                         LLM LAYER (optional)                            │
│   dp_llm_context.json (system context) · dp_tools.json (tool schema)  │
│                    llm/dp_agent.py (dispatcher)                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Canonical Properties & Units

| Property | Key | Unit | Standard |
|---|---|---|---|
| Volumetric water content | `volumetric_water_content` | cm³/cm³ | GLOSIS/ISO-11461 |
| Matric potential | `matric_potential_cm` | cm H₂O | GLOSIS |
| Soil temperature | `soil_temperature_c` | °C | GLOSIS/ISO-11274 |
| Electrical conductivity | `electrical_conductivity_ds_m` | dS/m | GLOSIS/ISO-11265 |
| Hydraulic conductivity | `hydraulic_conductivity_cm_day` | cm/day | Van Genuchten 1980 |
| Vertical flux | `flux_down_cm_day` | cm/day | Darcy 1856 |
| Soil respiration | `soil_respiration_mg_C_cm3_day` | mg C/cm³/day | Q10 |
| NDVI | `ndvi` | dimensionless | GLOSIS |
| Land surface temperature | `land_surface_temperature_c` | °C | GLOSIS |
| Soil moisture index | `soil_moisture_index` | dimensionless | — |

---

## Supported Unit Conversions (Sensor Layer)

| From | To | Property |
|---|---|---|
| `%` | `cm³/cm³` | Water content |
| `°F`, `K` | `°C` | Temperature |
| `kPa`, `hPa`, `bar`, `pF`, `MPa` | `cm H₂O` | Matric potential |
| `μS/cm`, `mS/m`, `mS/cm` | `dS/m` | Electrical conductivity |
| `DN` (int16) | reflectance | Sentinel-2 L2A |

---

## Citing Digital Pedon

```bibtex
@software{badreldin_youssef_2026_digitalpedon,
  author    = {Badreldin, Nasem and Youssef, Ali},
  title     = {Digital Pedon: A Digital Twin Framework for Soil Profile
               Monitoring, Physics-Based Inference, and Global Interoperability},
  year      = {2026},
  version   = {2.0},
  license   = {CC-BY-4.0},
  url       = {https://github.com/digitalpedon/framework}
}
```

---

## License

[Creative Commons Attribution 4.0 International (CC BY 4.0)](LICENSE)

© 2026  Ali Youssef & Nasem Badreldin
