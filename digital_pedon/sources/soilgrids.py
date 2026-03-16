"""
╔══════════════════════════════════════════════════════════════════════════╗
║     LAYER 2 — INITIALISATION SOURCES (The Static Data)                  ║
║                                                                          ║
║  Fetches horizon-level soil properties from:                             ║
║    1. SoilGrids (ISRIC)  — global 250m resolution REST API               ║
║    2. SSURGO/STATSGO (USDA)  — US high-resolution soil survey            ║
║    3. OpenSoilData stub  — pluggable regional repository adapter         ║
║                                                                          ║
║  All fetchers return the SAME structure:                                 ║
║    {                                                                     ║
║      "source":   "<name>",                                               ║
║      "horizons": [ { <canonical-key>: value, ... }, ... ]               ║
║    }                                                                     ║
║  Keys are resolved to canonical ontology names via vocab.py.             ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations
import json
import logging
import math
import time
import urllib.request
import urllib.parse
import urllib.error
from typing import Any

from digital_pedon.ontology.vocab import canonical_key, normalise_dict

logger = logging.getLogger("dp.sources")

# ── Shared request helper ─────────────────────────────────────────────────────

def _get_json(url: str, timeout: int = 15) -> dict:
    """Minimal urllib GET that returns parsed JSON. Retries on 503."""
    import time as _time

    req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                "User-Agent": "DigitalPedon/2.0"})
    last_error = None
    for attempt, wait in enumerate([0, 5, 15], start=1):
        if wait:
            logger.info("Retrying in %ds (attempt %d/3)...", wait, attempt)
            _time.sleep(wait)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            last_error = e
            if e.code != 503:
                raise RuntimeError(f"HTTP {e.code} from {url}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"Network error: {e.reason}") from e

    # All 3 attempts returned 503
    raise RuntimeError(
        "\n"
        "  ┌─────────────────────────────────────────────────────────┐\n"
        "  │  SoilGrids (ISRIC) is temporarily unavailable (503).    │\n"
        "  │                                                          │\n"
        "  │  This is a known issue with the free ISRIC REST API.    │\n"
        "  │  Please try again in 30–60 minutes.                     │\n"
        "  │                                                          │\n"
        "  │  In the meantime the framework continues with the       │\n"
        "  │  offline fallback profile automatically.                 │\n"
        "  │                                                          │\n"
        "  │  Status page: https://status.isric.org                  │\n"
        "  └─────────────────────────────────────────────────────────┘\n"
    ) from last_error



# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — SoilGrids (ISRIC)
# REST API: https://rest.isric.org/soilgrids/v2.0/properties/query
# ══════════════════════════════════════════════════════════════════════════════

#: SoilGrids property codes → canonical DP keys
SOILGRIDS_PROPERTY_MAP: dict[str, str] = {
    "clay":   "clay_content",
    "silt":   "silt_content",
    "sand":   "sand_content",
    "soc":    "organic_carbon",
    "phh2o":  "ph_h2o",
    "cec":    "cec",
    "bdod":   "bulk_density",
    "nitrogen": "nitrogen_total",
    "cfvo":   "coarse_fragments",
}

#: SoilGrids unit conversion factors to bring values into DP canonical units
#: SoilGrids returns many properties as integers with a scaling factor
SOILGRIDS_SCALE: dict[str, float] = {
    "clay":     0.1,    # g/kg → %  (divide by 10)
    "silt":     0.1,
    "sand":     0.1,
    "soc":      0.1,    # dg/kg → g/kg  (×0.1) then / 10 for %
    "phh2o":    0.1,    # pH ×10 → pH
    "cec":      0.1,    # mmol(c)/kg → cmol(+)/kg
    "bdod":     0.01,   # cg/cm³ → g/cm³
    "nitrogen": 0.01,   # cg/kg → g/kg
    "cfvo":     0.1,    # cm³/dm³ → vol%
}

#: Standard SoilGrids depth intervals in cm
SOILGRIDS_DEPTHS = [
    ("0-5cm",   0,   5),
    ("5-15cm",  5,  15),
    ("15-30cm", 15, 30),
    ("30-60cm", 30, 60),
    ("60-100cm",60, 100),
    ("100-200cm",100,200),
]


def fetch_soilgrids(
    latitude: float,
    longitude: float,
    properties: list[str] | None = None,
    depth_filter: list[str] | None = None,
) -> dict[str, Any]:
    """
    Fetch soil properties from the SoilGrids v2 REST API for a given location.

    Parameters
    ----------
    latitude, longitude : WGS84 decimal degrees
    properties : SoilGrids property codes to request (default: all mapped)
    depth_filter : depth interval codes to include, e.g. ["0-5cm","5-15cm"]
                   (default: all six standard intervals)

    Returns
    -------
    {
        "source":     "SoilGrids-ISRIC",
        "latitude":   float,
        "longitude":  float,
        "fetched_at": ISO timestamp,
        "horizons": [
            {
                "upper_depth":  float,    ← canonical key
                "lower_depth":  float,
                "clay_content": float,    ← canonical key
                "ph_h2o":       float,
                ...
            }, ...
        ]
    }

    Notes
    -----
    Van Genuchten parameters are NOT provided by SoilGrids.
    Use estimate_vg_params() to derive them from texture after fetching.
    """
    props = properties or list(SOILGRIDS_PROPERTY_MAP.keys())
    depths = depth_filter or [d[0] for d in SOILGRIDS_DEPTHS]

    base_url = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    params = (
        f"?lon={longitude}&lat={latitude}"
        + "".join(f"&property={p}" for p in props)
        + "".join(f"&depth={d}" for d in depths)
        + "&value=mean"
    )
    url = base_url + params
    logger.info("SoilGrids fetch: lat=%.4f lon=%.4f", latitude, longitude)

    raw = _get_json(url, timeout=45)

    # Parse SoilGrids response structure
    # layers → [ {name: "clay", depths: [{label:"0-5cm", values:{mean:220}}, ...]} ]
    depth_buckets: dict[str, dict[str, float]] = {d: {} for d in depths}

    for layer in raw.get("properties", {}).get("layers", []):
        prop_code = layer.get("name", "")
        canonical = SOILGRIDS_PROPERTY_MAP.get(prop_code)
        if not canonical:
            continue
        scale = SOILGRIDS_SCALE.get(prop_code, 1.0)

        for depth_entry in layer.get("depths", []):
            label = depth_entry.get("label", "")
            if label not in depth_buckets:
                continue
            mean_val = depth_entry.get("values", {}).get("mean")
            if mean_val is not None:
                depth_buckets[label][canonical] = round(mean_val * scale, 4)

    horizons = []
    for label, top, bot in SOILGRIDS_DEPTHS:
        if label not in depths:
            continue
        props_dict = depth_buckets.get(label, {})
        if not props_dict:
            continue
        horizon = {
            "upper_depth": float(top),
            "lower_depth": float(bot),
            "source_depth_label": label,
            **props_dict,
        }
        # Estimate VG params from texture
        if all(k in horizon for k in ("clay_content","sand_content","silt_content")):
            vg = estimate_vg_params(horizon)
            horizon.update(vg)
        horizons.append(horizon)

    return {
        "source":     "SoilGrids-ISRIC",
        "latitude":   latitude,
        "longitude":  longitude,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "horizons":   horizons,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — SSURGO / STATSGO (USDA)
# Web Soil Survey REST — Tabular Service
# ══════════════════════════════════════════════════════════════════════════════

#: SSURGO column names → canonical DP keys
SSURGO_COLUMN_MAP: dict[str, str] = {
    "claytotal_r":   "clay_content",
    "silttotal_r":   "silt_content",
    "sandtotal_r":   "sand_content",
    "om_r":          "organic_carbon",       # OM% ÷ 1.724 for OC
    "ph1to1h2o_r":   "ph_h2o",
    "cec7_r":        "cec",
    "dbovendry_r":   "bulk_density",
    "ksat_r":        "Ks",
    "wsat_r":        "theta_s",
    "hzdept_r":      "upper_depth",
    "hzdepb_r":      "lower_depth",
    "hzname":        "designation",
}


def fetch_ssurgo(
    latitude: float,
    longitude: float,
) -> dict[str, Any]:
    """
    Fetch SSURGO horizon data from USDA Web Soil Survey for a point location.

    Uses the Soil Data Access (SDA) tabular query service.
    Only available for US locations.

    Returns the same structure as fetch_soilgrids().
    """
    # SDA tabular query via REST
    query = f"""
        SELECT
            hzname, hzdept_r, hzdepb_r,
            claytotal_r, silttotal_r, sandtotal_r,
            om_r, ph1to1h2o_r, cec7_r,
            dbovendry_r, ksat_r, wsat_r
        FROM
            component AS c
            INNER JOIN chorizon AS ch ON c.cokey = ch.cokey
            INNER JOIN mapunit AS mu ON c.mukey = mu.mukey
            INNER JOIN SDA_Get_Mukey_from_intersection_with_WktWgs84(
                'point({longitude} {latitude})'
            ) AS t ON mu.mukey = t.mukey
        WHERE
            c.majcompflag = 'Yes'
        ORDER BY hzdept_r
    """.strip()

    url = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest"
    data = urllib.parse.urlencode({
        "query":  query,
        "format": "JSON+COLUMNNAME",
    }).encode()
    req = urllib.request.Request(url, data=data,
                                  headers={"Content-Type":
                                           "application/x-www-form-urlencoded"})
    logger.info("SSURGO fetch: lat=%.4f lon=%.4f", latitude, longitude)

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
    except Exception as e:
        raise RuntimeError(f"SSURGO fetch failed: {e}") from e

    table = raw.get("Table", [])
    if len(table) < 2:
        raise ValueError("No SSURGO data returned for this location.")

    columns = [c.lower() for c in table[0]]
    horizons = []

    for row in table[1:]:
        record = dict(zip(columns, row))
        horizon: dict[str, Any] = {}
        for ssurgo_col, dp_key in SSURGO_COLUMN_MAP.items():
            val = record.get(ssurgo_col.lower())
            if val is not None:
                try:
                    horizon[dp_key] = float(val)
                except (ValueError, TypeError):
                    horizon[dp_key] = val

        # OM% → organic carbon (OC = OM / 1.724)
        if "organic_carbon" in horizon:
            horizon["organic_carbon"] = round(horizon["organic_carbon"] / 1.724, 3)

        # ksat: SSURGO gives µm/s → cm/day
        if "Ks" in horizon:
            horizon["Ks"] = round(horizon["Ks"] * 86400 / 10000, 4)

        # Add VG estimates
        if all(k in horizon for k in ("clay_content", "sand_content", "silt_content")):
            vg = estimate_vg_params(horizon)
            horizon.update(vg)

        if horizon:
            horizons.append(horizon)

    return {
        "source":     "SSURGO-USDA",
        "latitude":   latitude,
        "longitude":  longitude,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "horizons":   horizons,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 3 — OpenSoilData / Regional Adapter
# ══════════════════════════════════════════════════════════════════════════════

class OpenSoilDataAdapter:
    """
    Generic adapter for any regional soil data repository that
    returns JSON from a REST endpoint.

    Subclass this and override _build_url() and _parse_response()
    to connect to any new source.

    Built-in adapters
    -----------------
    "esdac"   → EU ESDAC JRC soil data
    "nzsoil"  → New Zealand S-Map
    "custom"  → user-supplied URL template
    """

    KNOWN_ENDPOINTS = {
        "esdac":  "https://esdac.jrc.ec.europa.eu/api/soil?lat={lat}&lon={lon}",
        "nzsoil": "https://smap.landcareresearch.co.nz/api/point?lat={lat}&lon={lon}",
    }

    def __init__(self, source_name: str, url_template: str | None = None) -> None:
        self.source_name  = source_name
        self.url_template = url_template or self.KNOWN_ENDPOINTS.get(source_name)
        if not self.url_template:
            raise ValueError(
                f"Unknown source '{source_name}'. "
                f"Provide url_template or use: {list(self.KNOWN_ENDPOINTS)}"
            )

    def fetch(self, latitude: float, longitude: float) -> dict[str, Any]:
        """
        Fetch and normalise data from the regional endpoint.
        Override _parse_response() to handle non-standard JSON structures.
        """
        url = self.url_template.format(lat=latitude, lon=longitude)
        logger.info("%s fetch: lat=%.4f lon=%.4f", self.source_name, latitude, longitude)
        raw = _get_json(url)
        horizons = self._parse_response(raw)
        return {
            "source":     self.source_name,
            "latitude":   latitude,
            "longitude":  longitude,
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "horizons":   horizons,
        }

    def _parse_response(self, raw: dict) -> list[dict[str, Any]]:
        """
        Parse the raw API response into a list of normalised horizon dicts.
        Override this for non-standard response structures.

        Default: expects {"horizons": [{<key>: <value>}, ...]}
        and passes all keys through the ontology normaliser.
        """
        raw_horizons = raw.get("horizons", raw.get("layers", [raw]))
        return [normalise_dict(h) for h in raw_horizons]


# ══════════════════════════════════════════════════════════════════════════════
# PEDOTRANSFER FUNCTIONS — derive Van Genuchten params from texture
# ══════════════════════════════════════════════════════════════════════════════

def estimate_vg_params(props: dict[str, Any]) -> dict[str, Any]:
    """
    Estimate Van Genuchten hydraulic parameters from texture using
    Rawls & Brakensiek (1985) pedotransfer functions.

    Accepts both canonical and non-canonical key names.
    """
    clay = props.get("clay_content", props.get("clay_pct", 20.0))
    sand = props.get("sand_content", props.get("sand_pct", 40.0))
    oc   = props.get("organic_carbon", props.get("organic_carbon_pct", 1.0))
    bd   = props.get("bulk_density",   props.get("bulk_density_g_cm3", 1.4))

    # Normalise to fractions
    if clay > 1: clay /= 100.0
    if sand > 1: sand /= 100.0

    # Rawls & Brakensiek (1985) regression equations
    # Coefficients from Schaap et al. 2001 (Rosetta simplified)
    ln_Ks  = (
        -0.884
        + 0.0153 * sand * 100
        - 0.0267 * clay * 100
        - 0.584  * bd
        + 0.0145 * oc
    )
    theta_s = (
        0.299
        - 0.251 * sand
        + 0.195 * clay
        + 0.011 * oc
        - 0.031 * bd
    )
    theta_r = max(0.01,
        0.026
        - 0.005 * sand
        + 0.008 * clay
        - 0.003 * oc
    )
    alpha   = math.exp(
        -1.496
        + 0.025 * sand * 100
        - 0.023 * clay * 100
        - 0.195 * bd
    )
    n       = max(1.05,
        1.514
        - 0.007 * sand * 100
        + 0.008 * clay * 100
        - 0.032 * bd
    )
    Ks_cm_day = max(0.01, math.exp(ln_Ks))   # already in cm/day from regression

    return {
        "theta_r":  round(max(0.01, min(theta_r, 0.15)), 4),
        "theta_s":  round(max(theta_r + 0.05, min(theta_s, 0.95)), 4),
        "alpha_vg": round(max(1e-4, alpha), 5),
        "n_vg":     round(n, 4),
        "Ks":       round(Ks_cm_day, 3),
    }


# ══════════════════════════════════════════════════════════════════════════════
# HIGH-LEVEL CONVENIENCE — auto-select best available source
# ══════════════════════════════════════════════════════════════════════════════

def fetch_soil_profile(
    latitude: float,
    longitude: float,
    prefer: str = "soilgrids",
) -> dict[str, Any]:
    """
    Fetch a soil profile for a coordinate, auto-selecting the best source.

    Parameters
    ----------
    latitude, longitude : WGS84 decimal degrees
    prefer : "soilgrids" | "ssurgo" | "auto"
             "auto" uses SSURGO for US locations, SoilGrids elsewhere.

    Returns
    -------
    Normalised profile dict ready to pass into build_pedon().
    """
    is_usa = (-125 < longitude < -66) and (24 < latitude < 50)

    if prefer == "auto":
        prefer = "ssurgo" if is_usa else "soilgrids"

    if prefer == "ssurgo" and not is_usa:
        logger.warning("SSURGO is US-only; falling back to SoilGrids.")
        prefer = "soilgrids"

    logger.info("Fetching soil profile from %s for (%.4f, %.4f)",
                prefer, latitude, longitude)

    if prefer == "ssurgo":
        return fetch_ssurgo(latitude, longitude)
    else:
        return fetch_soilgrids(latitude, longitude)


def profile_to_pedon_config(
    profile: dict[str, Any],
    site_name: str | None = None,
    designation_field: str = "designation",
) -> dict[str, Any]:
    """
    Convert a fetched soil profile dict into a build_pedon() config dict.

    Maps canonical ontology keys to the framework's internal key names
    so build_pedon() can consume it directly.

    Example
    -------
    profile = fetch_soil_profile(51.505, 4.469)
    config  = profile_to_pedon_config(profile, site_name="North Field")
    pedon   = build_pedon(config)
    """
    # Internal framework key aliases for the canonical names
    _INTERNAL = {
        "clay_content":       "clay_pct",
        "silt_content":       "silt_pct",
        "sand_content":       "sand_pct",
        "organic_carbon":     "organic_carbon_pct",
        "ph_h2o":             "ph",
        "bulk_density":       "bulk_density_g_cm3",
        "Ks":                 "Ks_cm_day",
        "upper_depth":        "depth_top_cm",
        "lower_depth":        "depth_bottom_cm",
        "electrical_conductivity": "ec_ds_m",
        "volumetric_water_content":"theta",
        "soil_temperature":   "temperature_c",
    }

    horizons = []
    for i, raw_h in enumerate(profile.get("horizons", [])):
        h: dict[str, Any] = {}
        for k, v in raw_h.items():
            internal_k = _INTERNAL.get(k, k)   # map to internal name, or keep canonical
            h[internal_k] = v

        # Assign a designation if missing
        if designation_field not in h and "designation" not in h:
            h["designation"] = f"H{i+1}"
        elif "designation" not in h:
            h["designation"] = h.pop(designation_field, f"H{i+1}")

        horizons.append(h)

    return {
        "site_name": site_name or f"Site ({profile.get('latitude')}, {profile.get('longitude')})",
        "latitude":  profile.get("latitude"),
        "longitude": profile.get("longitude"),
        "source":    profile.get("source"),
        "horizons":  horizons,
    }
