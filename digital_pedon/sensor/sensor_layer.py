"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              DIGITAL PEDON — SENSOR LAYER  (v2)                            ║
║                                                                              ║
║  Bring-Your-Own-Data (BYOD) input layer.                                   ║
║                                                                              ║
║  Philosophy                                                                  ║
║  ----------                                                                  ║
║  Hardware complexity lives outside this framework. You deliver your data    ║
║  — already read from the logger, raster, or API — and declare what each    ║
║  field means via a sensor manifest (YAML, JSON, or Python dict).           ║
║                                                                              ║
║  The layer does exactly three things:                                       ║
║    1. Validate incoming readings against the manifest                       ║
║    2. Convert every value to the canonical Digital Pedon SI unit           ║
║    3. Rename every field to the canonical Digital Pedon key                ║
║                                                                              ║
║  The result is a plain dict that pedon.update_sync() understands,          ║
║  regardless of what units or column names your original data used.         ║
║                                                                              ║
║  Supported data origins                                                      ║
║  ----------------------                                                      ║
║  • In-situ sensors    (TDR, FDR, tensiometers, thermistors, EC probes)     ║
║  • Datalogger CSV     (any column layout declared in the manifest)          ║
║  • Remote sensing     (user-extracted pixel values: NDVI, LST, SMI, etc.)  ║
║  • Manual field obs   (entered by hand, same pipeline)                     ║
║                                                                              ║
║  Usage                                                                       ║
║  -----                                                                       ║
║  manifest = SensorManifest.from_yaml("sensors.yaml")   # or .from_dict()  ║
║  layer    = SensorLayer(manifest)                                           ║
║  reading  = layer.ingest(raw_dict)                                         ║
║  result   = pedon.update_sync(reading)                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

Authors : Badreldin, N. & Youssef, A. (2026)
License : CC BY 4.0
"""

from __future__ import annotations

import copy
import json
import logging
import time
from typing import Any

logger = logging.getLogger("digital_pedon.sensor_layer")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CANONICAL PROPERTY TABLE
#
# Every property the Digital Pedon understands.
# Each entry defines:
#   canonical_key  : the key used inside the DP framework
#   si_unit        : the unit the DP always works in internally
#   glosis_uri     : globally unique GLOSIS / OGC identifier
#   description    : plain-language description
#   data_origin    : which data sources can supply this value
# ══════════════════════════════════════════════════════════════════════════════

CANONICAL_PROPERTIES: dict[str, dict[str, Any]] = {

    # ── In-situ soil moisture ─────────────────────────────────────────────
    "volumetric_water_content": {
        "si_unit":     "cm3/cm3",
        "aliases":     ["theta", "vwc", "VWC", "soil_moisture", "swc", "SWC",
                        "volumetric_soil_water"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#VolumetricWaterContent",
        "description": "Volumetric soil water content [cm³ of water per cm³ of soil]",
        "data_origin": ["in_situ", "remote_sensing", "manual"],
        "valid_range": [0.0, 0.95],
    },

    "matric_potential_cm": {
        "si_unit":     "cm_H2O",
        "aliases":     ["psi", "matric_potential", "soil_water_potential",
                        "suction_cm", "tension_cm"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#SoilWaterPotential",
        "description": "Soil matric potential [cm H₂O, always ≤ 0]",
        "data_origin": ["in_situ", "manual"],
        "valid_range": [-1e6, 0.0],
    },

    "soil_temperature_c": {
        "si_unit":     "degC",
        "aliases":     ["temperature_c", "temp_c", "soil_temp", "T_soil",
                        "soil_temperature", "temperature"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#SoilTemperature",
        "description": "Soil temperature [°C]",
        "data_origin": ["in_situ", "remote_sensing", "manual"],
        "valid_range": [-30.0, 80.0],
    },

    "electrical_conductivity_ds_m": {
        "si_unit":     "dS/m",
        "aliases":     ["ec_ds_m", "EC", "ec", "soil_ec", "electrical_conductivity",
                        "ECe", "ECsw"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#ElectricalConductivity",
        "description": "Soil electrical conductivity [dS/m] — salinity proxy",
        "data_origin": ["in_situ", "manual"],
        "valid_range": [0.0, 200.0],
    },

    "bulk_dielectric_permittivity": {
        "si_unit":     "dimensionless",
        "aliases":     ["epsilon", "dielectric", "raw_count_tdr",
                        "apparent_dielectric_constant"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#DielectricPermittivity",
        "description": "Bulk apparent dielectric permittivity [-] (raw TDR/FDR output)",
        "data_origin": ["in_situ"],
        "valid_range": [1.0, 80.0],
    },

    # ── Remote sensing ────────────────────────────────────────────────────
    "ndvi": {
        "si_unit":     "dimensionless",
        "aliases":     ["NDVI", "ndvi_sentinel2", "ndvi_landsat", "ndvi_modis",
                        "normalised_difference_vegetation_index"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#NDVI",
        "description": "Normalised Difference Vegetation Index [-1, 1]",
        "data_origin": ["remote_sensing"],
        "valid_range": [-1.0, 1.0],
    },

    "land_surface_temperature_c": {
        "si_unit":     "degC",
        "aliases":     ["LST", "LST_celsius", "land_surface_temp",
                        "radiometric_surface_temperature"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#LandSurfaceTemperature",
        "description": "Land surface temperature from thermal remote sensing [°C]",
        "data_origin": ["remote_sensing"],
        "valid_range": [-60.0, 90.0],
    },

    "soil_moisture_index": {
        "si_unit":     "dimensionless",
        "aliases":     ["SMI", "smi", "sentinel1_smi", "normalised_soil_moisture",
                        "soil_moisture_anomaly"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#SoilMoistureIndex",
        "description": "Remote sensing soil moisture index [0–1, dimensionless]",
        "data_origin": ["remote_sensing"],
        "valid_range": [0.0, 1.0],
    },

    "surface_reflectance_nir": {
        "si_unit":     "dimensionless",
        "aliases":     ["NIR", "nir", "Band8", "B8", "surface_reflectance_B8",
                        "near_infrared_reflectance"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#SurfaceReflectanceNIR",
        "description": "Near-infrared surface reflectance [0–1]",
        "data_origin": ["remote_sensing"],
        "valid_range": [0.0, 1.0],
    },

    "surface_reflectance_swir1": {
        "si_unit":     "dimensionless",
        "aliases":     ["SWIR1", "swir1", "Band11", "B11",
                        "shortwave_infrared_1"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#SurfaceReflectanceSWIR1",
        "description": "Shortwave infrared band 1 surface reflectance [0–1]",
        "data_origin": ["remote_sensing"],
        "valid_range": [0.0, 1.0],
    },

    "evapotranspiration_mm_day": {
        "si_unit":     "mm/day",
        "aliases":     ["ET", "ETa", "actual_ET", "evapotranspiration",
                        "ET_mm_day"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#Evapotranspiration",
        "description": "Actual evapotranspiration [mm/day]",
        "data_origin": ["remote_sensing", "manual"],
        "valid_range": [0.0, 30.0],
    },

    # ── Standard additional observations ─────────────────────────────────
    "precipitation_mm": {
        "si_unit":     "mm",
        "aliases":     ["rain_mm", "rainfall", "precipitation", "precip_mm"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#Precipitation",
        "description": "Precipitation depth since last reading [mm]",
        "data_origin": ["in_situ", "remote_sensing", "manual"],
        "valid_range": [0.0, 2000.0],
    },

    "redox_potential_mv": {
        "si_unit":     "mV",
        "aliases":     ["Eh", "redox", "ORP", "Eh_mv", "redox_mv"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#RedoxPotential",
        "description": "Soil redox potential [mV]",
        "data_origin": ["in_situ", "manual"],
        "valid_range": [-500.0, 900.0],
    },

    "ph": {
        "si_unit":     "dimensionless",
        "aliases":     ["pH", "soil_ph", "pH_H2O", "pH_CaCl2"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#pH",
        "description": "Soil pH (in H₂O unless otherwise declared)",
        "data_origin": ["in_situ", "manual"],
        "valid_range": [0.0, 14.0],
    },

    "co2_concentration_ppm": {
        "si_unit":     "ppm",
        "aliases":     ["CO2", "co2_ppm", "soil_co2", "carbon_dioxide_ppm"],
        "glosis_uri":  "http://w3id.org/glosis/model/codelists#SoilCO2Concentration",
        "description": "Soil pore-space CO₂ concentration [ppm]",
        "data_origin": ["in_situ"],
        "valid_range": [300.0, 100000.0],
    },
}

# Build an alias lookup: any alias → canonical key
_ALIAS_LOOKUP: dict[str, str] = {}
for _ckey, _cmeta in CANONICAL_PROPERTIES.items():
    _ALIAS_LOOKUP[_ckey] = _ckey                         # canonical maps to itself
    for _alias in _cmeta.get("aliases", []):
        _ALIAS_LOOKUP[_alias] = _ckey


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — UNIT CONVERSION TABLE
#
# Any reading field declared in the manifest can carry a unit string.
# If the unit differs from the canonical SI unit, this table converts it.
# ══════════════════════════════════════════════════════════════════════════════

def _convert(value: float, from_unit: str, canonical_key: str) -> float:
    """
    Convert a numeric value from its declared unit to the canonical SI unit.

    All conversions are pure multiplicative factors or simple offsets.
    Raises ValueError if the unit is unknown for the given canonical key.
    """
    to_unit = CANONICAL_PROPERTIES[canonical_key]["si_unit"]

    if from_unit == to_unit:
        return value

    # ── Temperature ───────────────────────────────────────────────────────
    if canonical_key in ("soil_temperature_c", "land_surface_temperature_c"):
        if from_unit in ("degF", "°F", "F"):
            return (value - 32.0) * 5.0 / 9.0
        if from_unit in ("K", "kelvin"):
            return value - 273.15

    # ── Water content ─────────────────────────────────────────────────────
    if canonical_key == "volumetric_water_content":
        if from_unit in ("%", "percent", "pct"):
            return value / 100.0
        if from_unit in ("m3/m3",):
            return value            # identical numerical value

    # ── Matric potential ──────────────────────────────────────────────────
    if canonical_key == "matric_potential_cm":
        if from_unit in ("kPa",):
            return value * 10.1972   # 1 kPa ≈ 10.197 cm H₂O
        if from_unit in ("hPa", "mbar"):
            return value * 1.01972
        if from_unit in ("bar",):
            return value * 1019.72
        if from_unit in ("pF",):
            # pF = log10(|cm H₂O|); convert back (result is always negative)
            import math
            return -(10.0 ** value)
        if from_unit in ("MPa",):
            return value * 101972.0

    # ── EC ────────────────────────────────────────────────────────────────
    if canonical_key == "electrical_conductivity_ds_m":
        if from_unit in ("mS/cm", "mS cm-1"):
            return value            # 1 mS/cm = 1 dS/m
        if from_unit in ("μS/cm", "uS/cm"):
            return value / 1000.0
        if from_unit in ("mS/m",):
            return value / 100.0

    # ── Reflectance (DN → reflectance) ────────────────────────────────────
    if canonical_key in ("surface_reflectance_nir", "surface_reflectance_swir1"):
        if from_unit in ("DN", "digital_number", "int16"):
            return value / 10000.0  # Sentinel-2 L2A scale factor

    # ── Length / depth ────────────────────────────────────────────────────
    if from_unit in ("mm",) and to_unit == "cm":
        return value / 10.0
    if from_unit in ("m",) and to_unit == "cm":
        return value * 100.0

    # If we reach here, the unit is accepted as-is (best-effort)
    logger.warning(
        "No conversion defined for '%s' → '%s' on key '%s'. "
        "Passing value as-is.",
        from_unit, to_unit, canonical_key,
    )
    return value


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — SENSOR FIELD DECLARATION
# A SensorField describes one column / band from your data source.
# ══════════════════════════════════════════════════════════════════════════════

class SensorField:
    """
    Declaration of a single data field in an incoming reading.

    Parameters
    ----------
    source_key      : str   — the key/column name in your raw data dict
    canonical_key   : str   — which DP property this maps to (or an alias)
    unit            : str   — unit of the incoming value (e.g. "degF", "%", "kPa")
    depth_cm        : float — sensor installation depth below surface [cm]
    horizon_id      : str   — which horizon this reading belongs to
    sampling_rate_s : float — nominal measurement interval [seconds] (metadata only)
    sensor_type     : str   — free text, e.g. "METER TEROS 11", "Sentinel-2 B8"
    data_origin     : str   — "in_situ" | "remote_sensing" | "manual"
    description     : str   — free text note
    valid_range     : tuple — (min, max) hard validity bounds; overrides canonical default
    optional        : bool  — if True, missing field is silently skipped

    Example
    -------
    SensorField(
        source_key      = "VWC_30cm",
        canonical_key   = "volumetric_water_content",
        unit            = "%",
        depth_cm        = 30.0,
        horizon_id      = "Bw",
        sampling_rate_s = 3600,
        sensor_type     = "Sentek Drill & Drop",
        data_origin     = "in_situ",
    )
    """

    def __init__(
        self,
        source_key:      str,
        canonical_key:   str,
        unit:            str,
        horizon_id:      str,
        depth_cm:        float = 0.0,
        sampling_rate_s: float = 3600.0,
        sensor_type:     str   = "unspecified",
        data_origin:     str   = "in_situ",
        description:     str   = "",
        valid_range:     tuple | None = None,
        optional:        bool  = False,
    ):
        # Resolve alias → canonical
        resolved = _ALIAS_LOOKUP.get(canonical_key)
        if resolved is None:
            raise ValueError(
                f"Unknown canonical_key or alias: '{canonical_key}'. "
                f"See CANONICAL_PROPERTIES for the full list."
            )
        self.source_key      = source_key
        self.canonical_key   = resolved
        self.unit            = unit
        self.horizon_id      = horizon_id
        self.depth_cm        = depth_cm
        self.sampling_rate_s = sampling_rate_s
        self.sensor_type     = sensor_type
        self.data_origin     = data_origin
        self.description     = description
        self.optional        = optional
        # Validity bounds: field-level overrides canonical default
        if valid_range:
            self.valid_range = valid_range
        else:
            self.valid_range = CANONICAL_PROPERTIES[resolved].get("valid_range")

    def to_dict(self) -> dict:
        return {
            "source_key":      self.source_key,
            "canonical_key":   self.canonical_key,
            "unit":            self.unit,
            "horizon_id":      self.horizon_id,
            "depth_cm":        self.depth_cm,
            "sampling_rate_s": self.sampling_rate_s,
            "sensor_type":     self.sensor_type,
            "data_origin":     self.data_origin,
            "description":     self.description,
            "optional":        self.optional,
            "valid_range":     self.valid_range,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SensorField":
        vr = d.get("valid_range")
        return cls(
            source_key      = d["source_key"],
            canonical_key   = d["canonical_key"],
            unit            = d["unit"],
            horizon_id      = d["horizon_id"],
            depth_cm        = float(d.get("depth_cm", 0.0)),
            sampling_rate_s = float(d.get("sampling_rate_s", 3600.0)),
            sensor_type     = d.get("sensor_type", "unspecified"),
            data_origin     = d.get("data_origin", "in_situ"),
            description     = d.get("description", ""),
            valid_range     = tuple(vr) if vr else None,
            optional        = bool(d.get("optional", False)),
        )

    def __repr__(self):
        return (f"SensorField({self.source_key!r} → {self.canonical_key!r} "
                f"[{self.unit}], horizon={self.horizon_id!r}, "
                f"type={self.sensor_type!r})")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — SENSOR MANIFEST
# A manifest groups all SensorField declarations for one pedon.
# Load from YAML, JSON, or build from a Python dict.
# ══════════════════════════════════════════════════════════════════════════════

class SensorManifest:
    """
    A collection of SensorField declarations for one Digital Pedon.

    Describes every variable that can arrive in a raw reading, what it means,
    where it comes from, and how to normalise it.

    ── Creating a manifest ──────────────────────────────────────────────────

    Option A: YAML file (recommended — write once, reuse forever)
    ─────────────────────────────────────────────────────────────
    manifest = SensorManifest.from_yaml("sensors.yaml")

    sensors.yaml format:
    ─────────────────────
    site_name: My Field Site
    pedon_id:  SITE_001
    fields:
      - source_key:      VWC_Ap_pct
        canonical_key:   volumetric_water_content
        unit:            "%"
        horizon_id:      Ap
        depth_cm:        15
        sampling_rate_s: 3600
        sensor_type:     METER TEROS 11
        data_origin:     in_situ
        description:     Ap horizon TDR probe, installed March 2025

      - source_key:      Temp_Ap_F
        canonical_key:   soil_temperature_c
        unit:            degF
        horizon_id:      Ap
        depth_cm:        15
        sampling_rate_s: 3600
        sensor_type:     METER TEROS 11
        data_origin:     in_situ

      - source_key:      NDVI_S2
        canonical_key:   ndvi
        unit:            dimensionless
        horizon_id:      Ap
        depth_cm:        0
        sampling_rate_s: 864000      # 10-day Sentinel-2 revisit
        sensor_type:     Sentinel-2 MSI
        data_origin:     remote_sensing
        description:     Extracted from Sentinel-2 L2A band 4/8 ratio

      - source_key:      LST_degC
        canonical_key:   land_surface_temperature_c
        unit:            degC
        horizon_id:      Ap
        depth_cm:        0
        sampling_rate_s: 86400
        sensor_type:     Landsat 8 Band 10
        data_origin:     remote_sensing
        optional:        true        # allowed to be absent in a reading

    Option B: JSON file
    ────────────────────
    manifest = SensorManifest.from_json("sensors.json")

    Option C: Python dict
    ─────────────────────
    manifest = SensorManifest.from_dict({
        "site_name": "My Field Site",
        "pedon_id":  "SITE_001",
        "fields": [
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
            ...
        ],
    })
    """

    def __init__(self, fields: list[SensorField],
                 site_name: str = "", pedon_id: str = ""):
        self.fields    = fields
        self.site_name = site_name
        self.pedon_id  = pedon_id
        # Index: source_key → SensorField (for fast lookup)
        self._index: dict[str, SensorField] = {f.source_key: f for f in fields}

    # ── Constructors ───────────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, d: dict) -> "SensorManifest":
        """Build from a plain Python dict."""
        fields = [SensorField.from_dict(fd) for fd in d.get("fields", [])]
        return cls(fields,
                   site_name = d.get("site_name", ""),
                   pedon_id  = d.get("pedon_id", ""))

    @classmethod
    def from_json(cls, path: str) -> "SensorManifest":
        """Load from a JSON file."""
        with open(path) as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def from_yaml(cls, path: str) -> "SensorManifest":
        """
        Load from a YAML file.
        Requires PyYAML:  pip install pyyaml
        """
        try:
            import yaml
        except ImportError as e:
            raise ImportError(
                "PyYAML is required to load YAML manifests: pip install pyyaml"
            ) from e
        with open(path) as f:
            return cls.from_dict(yaml.safe_load(f))

    # ── Persistence ────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "site_name": self.site_name,
            "pedon_id":  self.pedon_id,
            "fields":    [f.to_dict() for f in self.fields],
        }

    def to_json(self, path: str | None = None, indent: int = 2) -> str:
        s = json.dumps(self.to_dict(), indent=indent)
        if path:
            with open(path, "w") as f:
                f.write(s)
        return s

    def to_yaml(self, path: str | None = None) -> str:
        """Write the manifest as YAML (requires PyYAML)."""
        try:
            import yaml
        except ImportError as e:
            raise ImportError("pip install pyyaml") from e
        s = yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)
        if path:
            with open(path, "w") as f:
                f.write(s)
        return s

    def summary(self) -> str:
        lines = [
            f"SensorManifest — {self.site_name or 'unnamed'} [{self.pedon_id or '?'}]",
            f"  {len(self.fields)} fields declared:",
        ]
        for f in self.fields:
            lines.append(
                f"  • {f.source_key:30s}  [{f.unit:15s}]  → {f.canonical_key}"
                f"  ({f.sensor_type}, {f.data_origin})"
            )
        return "\n".join(lines)

    def __repr__(self):
        return f"SensorManifest(site={self.site_name!r}, fields={len(self.fields)})"


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — SENSOR LAYER
# Validates, converts, and normalises a raw reading dict using a manifest.
# ══════════════════════════════════════════════════════════════════════════════

class SensorLayer:
    """
    The BYOD ingestion gateway.

    Takes a raw reading dict (any keys, any units, any origin) and returns
    a normalised dict that pedon.update_sync() can consume directly.

    Parameters
    ----------
    manifest          : SensorManifest
    strict            : bool  — if True, raise on unknown keys; if False, pass them through
    validate_range    : bool  — if True, raise on out-of-range values; if False, warn only
    add_timestamp     : bool  — if True and 'timestamp' absent, inject current Unix time

    Usage
    -----
    layer   = SensorLayer(manifest)
    reading = layer.ingest({"VWC_Ap_pct": 28.5, "Temp_Ap_F": 59.4, "NDVI_S2": 0.61})
    # → {"horizon_id": "Ap", "volumetric_water_content": 0.285,
    #    "soil_temperature_c": 15.2, "ndvi": 0.61, "timestamp": 1741612800.0}

    result  = pedon.update_sync(reading)
    """

    def __init__(
        self,
        manifest:       SensorManifest,
        strict:         bool = False,
        validate_range: bool = True,
        add_timestamp:  bool = True,
    ):
        self.manifest       = manifest
        self.strict         = strict
        self.validate_range = validate_range
        self.add_timestamp  = add_timestamp

    def ingest(self, raw: dict[str, Any],
               timestamp: float | None = None) -> dict[str, Any]:
        """
        Process one raw reading dict and return a normalised DP reading.

        Parameters
        ----------
        raw       : dict   — raw data with source keys and original units
        timestamp : float  — Unix timestamp; if None and add_timestamp=True, uses now()

        Returns
        -------
        dict with canonical keys, SI units, horizon_id, and timestamp.
        Ready to pass directly to pedon.update_sync().

        Raises
        ------
        ValueError  : if a required field is missing or a value is out of range
                      (only when strict=True / validate_range=True)
        """
        out: dict[str, Any] = {}
        horizon_ids_seen: set[str] = set()

        index = self.manifest._index

        # ── Process declared fields ────────────────────────────────────────
        for source_key, field in index.items():

            if source_key not in raw:
                if field.optional:
                    continue
                # Not optional — check if we can find it by alias
                found = None
                for k in raw:
                    if _ALIAS_LOOKUP.get(k) == field.canonical_key:
                        found = k
                        break
                if found:
                    raw_val = raw[found]
                elif field.optional:
                    continue
                else:
                    raise ValueError(
                        f"Required field '{source_key}' not found in raw reading. "
                        f"Keys received: {list(raw.keys())}"
                    )
            else:
                raw_val = raw[source_key]

            # Convert to float
            try:
                val = float(raw_val)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"Field '{source_key}' value {raw_val!r} is not numeric."
                ) from e

            # Unit conversion → SI
            val = _convert(val, field.unit, field.canonical_key)

            # Range validation
            if self.validate_range and field.valid_range:
                lo, hi = field.valid_range
                if not (lo <= val <= hi):
                    msg = (
                        f"Field '{source_key}' value {val:.4g} "
                        f"out of valid range [{lo}, {hi}] "
                        f"(canonical: {field.canonical_key}, unit: {field.unit})"
                    )
                    if self.validate_range:
                        logger.warning(msg)   # warn but don't crash

            out[field.canonical_key] = val
            horizon_ids_seen.add(field.horizon_id)

        # ── Pass through undeclared fields (non-strict mode) ──────────────
        if not self.strict:
            for k, v in raw.items():
                if k in index:
                    continue
                # Try alias resolution
                canonical = _ALIAS_LOOKUP.get(k)
                if canonical and canonical not in out:
                    try:
                        out[canonical] = float(v)
                    except (TypeError, ValueError):
                        out[k] = v          # keep as-is if not numeric
                elif k not in out:
                    out[k] = v

        # ── Determine horizon_id ───────────────────────────────────────────
        if "horizon_id" not in out:
            if len(horizon_ids_seen) == 1:
                out["horizon_id"] = horizon_ids_seen.pop()
            elif len(horizon_ids_seen) > 1:
                # Multiple horizons in one raw reading — caller must split
                raise ValueError(
                    "This raw reading maps to multiple horizons: "
                    f"{horizon_ids_seen}. "
                    "Call layer.split_by_horizon(raw) to get one dict per horizon."
                )
            else:
                raise ValueError(
                    "horizon_id could not be determined. "
                    "Ensure at least one SensorField in the manifest has horizon_id set."
                )

        # ── Timestamp ─────────────────────────────────────────────────────
        if "timestamp" not in out:
            out["timestamp"] = timestamp or (time.time() if self.add_timestamp else None)

        logger.debug("Ingested reading for horizon '%s': %s", out.get("horizon_id"), out)
        return out

    def split_by_horizon(
        self, raw: dict[str, Any], timestamp: float | None = None
    ) -> list[dict[str, Any]]:
        """
        When a single raw reading contains fields from multiple horizons,
        split it into one normalised dict per horizon.

        This is the common case for multi-depth logger files where one row
        has VWC_10cm, VWC_30cm, VWC_60cm, each belonging to a different horizon.

        Returns
        -------
        List of normalised dicts, one per distinct horizon_id.
        Each can be passed directly to pedon.update_sync().

        Example
        -------
        raw = {
            "VWC_Ap_pct":  28.5,   # → Ap horizon
            "VWC_Bw_pct":  34.1,   # → Bw horizon
            "Temp_Ap_F":   59.4,   # → Ap
            "NDVI_S2":      0.61,  # → Ap (surface)
        }
        readings = layer.split_by_horizon(raw)
        # → [{"horizon_id":"Ap", ...}, {"horizon_id":"Bw", ...}]
        for r in readings:
            pedon.update_sync(r)
        """
        # Group manifest fields by horizon
        by_horizon: dict[str, dict[str, Any]] = {}
        ts = timestamp or (time.time() if self.add_timestamp else None)

        for source_key, field in self.manifest._index.items():
            hid = field.horizon_id

            if source_key not in raw:
                if field.optional:
                    continue
                raise ValueError(f"Required field '{source_key}' missing.")

            val = float(raw[source_key])
            val = _convert(val, field.unit, field.canonical_key)

            if hid not in by_horizon:
                by_horizon[hid] = {"horizon_id": hid, "timestamp": ts}
            by_horizon[hid][field.canonical_key] = val

        return list(by_horizon.values())

    def ingest_csv_row(self, row: dict[str, str],
                       timestamp: float | None = None) -> dict[str, Any]:
        """
        Convenience: ingest one row from csv.DictReader (all values are strings).
        Converts strings to float before processing.

        Example
        -------
        import csv
        with open("logger.csv") as f:
            for row in csv.DictReader(f):
                readings = layer.split_by_horizon(layer.ingest_csv_row(row))
        """
        numeric = {}
        for k, v in row.items():
            try:
                numeric[k] = float(v)
            except (ValueError, TypeError):
                numeric[k] = v
        return self.ingest(numeric, timestamp=timestamp)

    def __repr__(self):
        return (f"SensorLayer(manifest={self.manifest!r}, "
                f"strict={self.strict}, validate_range={self.validate_range})")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — MANIFEST BUILDER HELPER (interactive / programmatic)
# ══════════════════════════════════════════════════════════════════════════════

class ManifestBuilder:
    """
    Fluent builder for constructing a SensorManifest in code.

    manifest = (
        ManifestBuilder("My Site", "SITE_001")
        .add_insitu(
            source_key      = "VWC_Ap_pct",
            canonical_key   = "volumetric_water_content",
            unit            = "%",
            horizon_id      = "Ap",
            depth_cm        = 15,
            sampling_rate_s = 3600,
            sensor_type     = "METER TEROS 11",
        )
        .add_remote_sensing(
            source_key    = "NDVI_S2",
            canonical_key = "ndvi",
            unit          = "dimensionless",
            horizon_id    = "Ap",
            sensor_type   = "Sentinel-2 MSI",
            sampling_rate_s = 864000,
        )
        .build()
    )
    """

    def __init__(self, site_name: str = "", pedon_id: str = ""):
        self.site_name = site_name
        self.pedon_id  = pedon_id
        self._fields: list[SensorField] = []

    def _add(self, data_origin: str, **kwargs) -> "ManifestBuilder":
        self._fields.append(SensorField(data_origin=data_origin, **kwargs))
        return self

    def add_insitu(self, **kwargs) -> "ManifestBuilder":
        """Add an in-situ sensor field."""
        return self._add("in_situ", **kwargs)

    def add_remote_sensing(self, **kwargs) -> "ManifestBuilder":
        """Add a remote sensing derived field (user-extracted pixel value)."""
        return self._add("remote_sensing", **kwargs)

    def add_manual(self, **kwargs) -> "ManifestBuilder":
        """Add a manually entered observation."""
        return self._add("manual", **kwargs)

    def build(self) -> SensorManifest:
        return SensorManifest(self._fields,
                              site_name=self.site_name,
                              pedon_id=self.pedon_id)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — EXAMPLE MANIFESTS  (ready-to-use templates)
# ══════════════════════════════════════════════════════════════════════════════

def example_manifest_insitu() -> SensorManifest:
    """
    Example: two-horizon in-situ monitoring setup.
    METER TEROS 11 at Ap (15 cm) and Bw (45 cm).
    """
    return (
        ManifestBuilder("Example Site — In-situ", "EX_001")
        .add_insitu(
            source_key      = "VWC_Ap_pct",
            canonical_key   = "volumetric_water_content",
            unit            = "%",
            horizon_id      = "Ap",
            depth_cm        = 15.0,
            sampling_rate_s = 3600,
            sensor_type     = "METER TEROS 11",
            description     = "Ap horizon TDR probe",
        )
        .add_insitu(
            source_key      = "Temp_Ap_F",
            canonical_key   = "soil_temperature_c",
            unit            = "degF",
            horizon_id      = "Ap",
            depth_cm        = 15.0,
            sampling_rate_s = 3600,
            sensor_type     = "METER TEROS 11",
        )
        .add_insitu(
            source_key      = "EC_Ap",
            canonical_key   = "electrical_conductivity_ds_m",
            unit            = "dS/m",
            horizon_id      = "Ap",
            depth_cm        = 15.0,
            sampling_rate_s = 3600,
            sensor_type     = "METER TEROS 11",
        )
        .add_insitu(
            source_key      = "VWC_Bw_m3m3",
            canonical_key   = "volumetric_water_content",
            unit            = "m3/m3",
            horizon_id      = "Bw",
            depth_cm        = 45.0,
            sampling_rate_s = 3600,
            sensor_type     = "Sentek Drill & Drop",
        )
        .add_insitu(
            source_key      = "Temp_Bw_C",
            canonical_key   = "soil_temperature_c",
            unit            = "degC",
            horizon_id      = "Bw",
            depth_cm        = 45.0,
            sampling_rate_s = 3600,
            sensor_type     = "Sentek Drill & Drop",
        )
        .build()
    )


def example_manifest_with_remote_sensing() -> SensorManifest:
    """
    Example: in-situ sensors combined with remote sensing inputs.
    Demonstrates multi-origin manifest.
    """
    return (
        ManifestBuilder("Example Site — Mixed Origin", "EX_002")
        .add_insitu(
            source_key      = "VWC_Ap_pct",
            canonical_key   = "volumetric_water_content",
            unit            = "%",
            horizon_id      = "Ap",
            depth_cm        = 15.0,
            sampling_rate_s = 3600,
            sensor_type     = "METER TEROS 11",
        )
        .add_insitu(
            source_key      = "Temp_Ap_C",
            canonical_key   = "soil_temperature_c",
            unit            = "degC",
            horizon_id      = "Ap",
            depth_cm        = 15.0,
            sampling_rate_s = 3600,
            sensor_type     = "METER TEROS 11",
        )
        .add_remote_sensing(
            source_key      = "NDVI_S2",
            canonical_key   = "ndvi",
            unit            = "dimensionless",
            horizon_id      = "Ap",
            depth_cm        = 0.0,
            sampling_rate_s = 864000,    # ~10 days (Sentinel-2 revisit)
            sensor_type     = "Sentinel-2 MSI Band 4/8",
            description     = "Extracted pixel at field centroid, cloud-masked",
        )
        .add_remote_sensing(
            source_key      = "LST_Landsat",
            canonical_key   = "land_surface_temperature_c",
            unit            = "degC",
            horizon_id      = "Ap",
            depth_cm        = 0.0,
            sampling_rate_s = 1382400,   # ~16 days (Landsat revisit)
            sensor_type     = "Landsat 8 Band 10 (TIRS)",
            optional        = True,
        )
        .add_remote_sensing(
            source_key      = "SMI_S1",
            canonical_key   = "soil_moisture_index",
            unit            = "dimensionless",
            horizon_id      = "Ap",
            depth_cm        = 0.0,
            sampling_rate_s = 432000,    # ~5 days (Sentinel-1 revisit)
            sensor_type     = "Sentinel-1 SAR derived SMI",
            optional        = True,
        )
        .build()
    )
