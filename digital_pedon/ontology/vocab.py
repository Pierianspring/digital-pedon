"""
╔══════════════════════════════════════════════════════════════════════════╗
║         LAYER 1 — GLOBAL SOIL ONTOLOGY (The Keys)                       ║
║                                                                          ║
║  Canonical property names drawn from:                                   ║
║    • GLOSIS (FAO Global Soil Information System)                         ║
║    • OGC SoilML / ISO 28258                                              ║
║    • AgroVoc multilingual thesaurus                                      ║
║    • SoilGrids ISRIC property codes                                      ║
║                                                                          ║
║  Every key used anywhere in the DP is defined here with:                ║
║    - its canonical name  (the dict key)                                  ║
║    - its ontology URI    (machine-readable identity)                     ║
║    - its unit            (SI / standard)                                 ║
║    - its source standard                                                 ║
║    - human description                                                   ║
║                                                                          ║
║  Rule: if a concept exists in GLOSIS/OGC, use THEIR key name.           ║
║        Never invent a name for something that already has a standard.    ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


# ── Namespaces ────────────────────────────────────────────────────────────────

NS = {
    "glosis":   "http://w3id.org/glosis/model/",
    "isric":    "https://www.isric.org/explore/soilgrids/",
    "ogc":      "http://www.opengis.net/ont/soilml/",
    "agrovoc":  "http://aims.fao.org/aos/agrovoc/",
    "qudt":     "http://qudt.org/vocab/unit/",
    "skos":     "http://www.w3.org/2004/02/skos/core#",
    "dp":       "http://digitalpedon.org/vocab/",      # local extension namespace
}


# ── Property descriptor ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class OntologyProperty:
    """Describes one soil property with full ontological provenance."""
    key:         str            # canonical Python key used in all DP dicts
    uri:         str            # globally unique identifier
    unit:        str            # SI unit string
    standard:    str            # source standard
    description: str            # human-readable
    aliases:     tuple[str,...] = field(default_factory=tuple)  # vendor / legacy names
    value_type:  str = "float"  # float | int | str | categorical


# ══════════════════════════════════════════════════════════════════════════════
# THE CANONICAL PROPERTY TABLE
# ══════════════════════════════════════════════════════════════════════════════

PROPERTIES: dict[str, OntologyProperty] = {}

def _reg(*args, **kwargs) -> OntologyProperty:
    p = OntologyProperty(*args, **kwargs)
    PROPERTIES[p.key] = p
    return p


# ── Physical / geometric ──────────────────────────────────────────────────────

_reg("upper_depth",
     uri=NS["glosis"]+"codelists#UpperDepth",
     unit="cm", standard="GLOSIS/ISO-28258",
     description="Upper boundary depth of the horizon",
     aliases=("depth_top_cm","top_cm","upperDepth"))

_reg("lower_depth",
     uri=NS["glosis"]+"codelists#LowerDepth",
     unit="cm", standard="GLOSIS/ISO-28258",
     description="Lower boundary depth of the horizon",
     aliases=("depth_bottom_cm","bot_cm","lowerDepth"))

# ── Texture ───────────────────────────────────────────────────────────────────

_reg("clay_content",
     uri=NS["glosis"]+"codelists#ClayContent",
     unit="%", standard="GLOSIS/FAO",
     description="Mass fraction of clay particles (<0.002 mm)",
     aliases=("clay_pct","clay","CLAY","claytotal_r"))

_reg("silt_content",
     uri=NS["glosis"]+"codelists#SiltContent",
     unit="%", standard="GLOSIS/FAO",
     description="Mass fraction of silt particles (0.002–0.05 mm)",
     aliases=("silt_pct","silt","SILT","silttotal_r"))

_reg("sand_content",
     uri=NS["glosis"]+"codelists#SandContent",
     unit="%", standard="GLOSIS/FAO",
     description="Mass fraction of sand particles (0.05–2 mm)",
     aliases=("sand_pct","sand","SAND","sandtotal_r"))

# ── Chemistry ─────────────────────────────────────────────────────────────────

_reg("organic_carbon",
     uri=NS["glosis"]+"codelists#OrganicCarbon",
     unit="g/kg", standard="GLOSIS/ISO-10694",
     description="Soil organic carbon content",
     aliases=("organic_carbon_pct","OC","soc","ocd","SOC_mean","oc"))

_reg("ph_h2o",
     uri=NS["glosis"]+"codelists#SoilReactionpH",
     unit="pH", standard="GLOSIS/ISO-10390",
     description="Soil pH measured in water (1:2.5 soil:water)",
     aliases=("ph","pH","phh2o","ph_water","ph1to1h2o_r"))

_reg("cec",
     uri=NS["glosis"]+"codelists#CEC",
     unit="cmol(+)/kg", standard="GLOSIS/ISO-13536",
     description="Cation exchange capacity",
     aliases=("cec_cmol_kg","CEC","cec7_r"))

_reg("nitrogen_total",
     uri=NS["glosis"]+"codelists#TotalNitrogen",
     unit="g/kg", standard="GLOSIS/ISO-13878",
     description="Total soil nitrogen",
     aliases=("total_N_pct","tn","nitrogen","nv"))

_reg("calcium_carbonate",
     uri=NS["glosis"]+"codelists#CalciumCarbonateEquivalent",
     unit="%", standard="GLOSIS/ISO-10693",
     description="Calcium carbonate (CaCO3) equivalent",
     aliases=("caco3_pct","CaCO3","caco3"))

_reg("electrical_conductivity",
     uri=NS["glosis"]+"codelists#ElectricalConductivity",
     unit="dS/m", standard="GLOSIS/ISO-11265",
     description="Electrical conductivity of saturated paste extract",
     aliases=("ec_ds_m","EC","ece","ECe","ec"))

_reg("exchangeable_sodium_pct",
     uri=NS["glosis"]+"codelists#ExchangeableSodiumPercentage",
     unit="%", standard="GLOSIS",
     description="Exchangeable Sodium Percentage",
     aliases=("esp_pct","ESP","esp"))

# ── Physical ──────────────────────────────────────────────────────────────────

_reg("bulk_density",
     uri=NS["glosis"]+"codelists#BulkDensity",
     unit="g/cm³", standard="GLOSIS/ISO-11272",
     description="Dry bulk density of the fine earth",
     aliases=("bulk_density_g_cm3","BD","dbovendry_r","bdod"))

_reg("coarse_fragments",
     uri=NS["glosis"]+"codelists#CoarseFragments",
     unit="vol%", standard="GLOSIS",
     description="Volumetric fraction of coarse fragments (>2 mm)",
     aliases=("coarse_fragments_pct","cfvo","rock_fragments_pct"))

# ── Hydraulics (Van Genuchten) ────────────────────────────────────────────────

_reg("theta_r",
     uri=NS["dp"]+"HydraulicResidualWaterContent",
     unit="cm³/cm³", standard="Van Genuchten 1980",
     description="Residual volumetric water content",
     aliases=("thetaR","residual_wc"))

_reg("theta_s",
     uri=NS["dp"]+"HydraulicSaturatedWaterContent",
     unit="cm³/cm³", standard="Van Genuchten 1980",
     description="Saturated volumetric water content",
     aliases=("thetaS","saturated_wc","wsat_r"))

_reg("alpha_vg",
     uri=NS["dp"]+"VanGenuchtenAlpha",
     unit="1/cm", standard="Van Genuchten 1980",
     description="Van Genuchten α shape parameter",
     aliases=("alpha","vg_alpha"))

_reg("n_vg",
     uri=NS["dp"]+"VanGenuchtenN",
     unit="-", standard="Van Genuchten 1980",
     description="Van Genuchten n shape parameter",
     aliases=("n","vg_n"))

_reg("Ks",
     uri=NS["dp"]+"SaturatedHydraulicConductivity",
     unit="cm/day", standard="GLOSIS",
     description="Saturated hydraulic conductivity",
     aliases=("Ks_cm_day","ksat","Ksat","ks","ksat_r"))

# ── Dynamic / IoT ─────────────────────────────────────────────────────────────

_reg("volumetric_water_content",
     uri=NS["glosis"]+"codelists#VolumetricWaterContent",
     unit="cm³/cm³", standard="GLOSIS/ISO-11461",
     description="Volumetric water content (θ)",
     aliases=("theta","vwc","VWC","SWC","wc","mv","volumetricWaterContent",
              "soil_moisture","sm"))

_reg("soil_temperature",
     uri=NS["glosis"]+"codelists#SoilTemperature",
     unit="°C", standard="GLOSIS/ISO-11274",
     description="Soil temperature",
     aliases=("temperature_c","temp","T","temp_C","soilTemp","st"))

_reg("matric_potential",
     uri=NS["glosis"]+"codelists#MatricPotential",
     unit="cm H₂O", standard="GLOSIS",
     description="Soil matric (water) potential",
     aliases=("matric_potential_cm","psi","suction","tension","wp"))

# ── Derived ───────────────────────────────────────────────────────────────────

_reg("hydraulic_conductivity",
     uri=NS["dp"]+"UnsaturatedHydraulicConductivity",
     unit="cm/day", standard="Van Genuchten 1980",
     description="Unsaturated hydraulic conductivity K(θ)",
     aliases=("hydraulic_conductivity_cm_day","K_unsat"))

_reg("vertical_flux",
     uri=NS["dp"]+"VerticalWaterFlux",
     unit="cm/day", standard="Darcy 1856",
     description="Vertical Darcian water flux between horizons (+down)",
     aliases=("flux_down_cm_day","darcy_flux","q"))

_reg("soil_respiration",
     uri=NS["agrovoc"]+"c_330916",
     unit="mg C/cm³/day", standard="AgroVoc",
     description="Soil CO₂ efflux (microbial respiration)",
     aliases=("soil_respiration_mg_C_cm3_day","CO2_flux","respiration"))

_reg("thermal_diffusivity",
     uri=NS["dp"]+"ThermalDiffusivity",
     unit="cm²/day", standard="De Vries 1963",
     description="Apparent soil thermal diffusivity",
     aliases=("thermal_diffusivity_cm2_day",))


# ══════════════════════════════════════════════════════════════════════════════
# ALIAS RESOLVER
# ══════════════════════════════════════════════════════════════════════════════

# Flat map: any alias → canonical key
_ALIAS_MAP: dict[str, str] = {}
for _prop in PROPERTIES.values():
    for _alias in _prop.aliases:
        _ALIAS_MAP[_alias.lower()] = _prop.key
    _ALIAS_MAP[_prop.key.lower()] = _prop.key   # self-mapping


def canonical_key(name: str) -> str:
    """
    Resolve any vendor name / legacy alias / alternate spelling
    to its canonical ontology key.

    Returns the canonical key, or the original name if unknown
    (unknown keys are allowed — the system is agnostic).
    """
    return _ALIAS_MAP.get(name.lower(), name)


def normalise_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Translate all keys in a dict to their canonical equivalents.
    Values are untouched.

    Example
    -------
    normalise_dict({"CLAY": 22, "BD": 1.4, "wc": 0.31})
    → {"clay_content": 22, "bulk_density": 1.4, "volumetric_water_content": 0.31}
    """
    return {canonical_key(k): v for k, v in raw.items()}


def describe(key: str) -> dict[str, Any] | None:
    """Return the full ontology descriptor for a canonical key, or None."""
    prop = PROPERTIES.get(key)
    if prop is None:
        return None
    return {
        "key":         prop.key,
        "uri":         prop.uri,
        "unit":        prop.unit,
        "standard":    prop.standard,
        "description": prop.description,
        "aliases":     list(prop.aliases),
    }


def list_properties(standard: str | None = None) -> list[str]:
    """List canonical keys, optionally filtered by standard name."""
    if standard:
        return [k for k, p in PROPERTIES.items() if standard.lower() in p.standard.lower()]
    return list(PROPERTIES.keys())


def to_jsonld(data: dict[str, Any]) -> dict[str, Any]:
    """
    Annotate a plain data dict with a JSON-LD @context block,
    mapping each canonical key to its ontology URI.
    """
    context = {}
    for k in data:
        prop = PROPERTIES.get(k)
        if prop:
            context[k] = {"@id": prop.uri, "@type": prop.unit}
    return {"@context": context, **data}


def unit_of(key: str) -> str | None:
    """Return the SI unit string for a canonical key."""
    prop = PROPERTIES.get(canonical_key(key))
    return prop.unit if prop else None
