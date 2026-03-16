"""
╔══════════════════════════════════════════════════════════════════════════╗
║              DIGITAL PEDON — DICTIONARY-FIRST USER API                  ║
║                                                                          ║
║  Everything is a plain Python dict.                                      ║
║  You never need to touch classes, imports, or framework internals.       ║
╚══════════════════════════════════════════════════════════════════════════╝

QUICK-START
-----------
1. Define your soil type(s) as dicts          →  SOIL_TYPE_REGISTRY
2. Define your pedon (profile) as a dict      →  build_pedon(config_dict)
3. Register custom functions as dicts         →  register_method(method_dict)
4. Feed sensor data as dicts                  →  update(reading_dict)
5. Read results as dicts                      →  snapshot() / query_history()

Everything in, everything out — plain dicts, no exceptions.
"""

from __future__ import annotations
import asyncio
import copy
import json
import logging
import math
import time
import uuid
from typing import Any, Callable

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("digital_pedon")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — SOIL TYPE REGISTRY
# A global dictionary of known soil types.
# Users add their own by calling  register_soil_type(soil_dict).
# ══════════════════════════════════════════════════════════════════════════════

#: The master registry — all soil types live here.
#: Key = soil type name (string), Value = property dict.
SOIL_TYPE_REGISTRY: dict[str, dict[str, Any]] = {

    # ── Built-in soil types ───────────────────────────────────────────────

    "loamy_topsoil": {
        "description":          "Typical loam A-horizon",
        # Texture (%)
        "clay_pct":             18.0,
        "silt_pct":             42.0,
        "sand_pct":             40.0,
        # Organic matter
        "organic_carbon_pct":   2.8,
        # Physical
        "bulk_density_g_cm3":   1.25,
        "cec_cmol_kg":          18.0,
        "ph":                   6.4,
        # Van Genuchten hydraulic parameters
        "theta_r":              0.030,
        "theta_s":              0.480,
        "alpha_vg":             0.020,
        "n_vg":                 1.41,
        "Ks_cm_day":            15.0,
    },

    "clay_subsoil": {
        "description":          "Heavy clay B-horizon",
        "clay_pct":             48.0,
        "silt_pct":             32.0,
        "sand_pct":             20.0,
        "organic_carbon_pct":   0.5,
        "bulk_density_g_cm3":   1.50,
        "cec_cmol_kg":          30.0,
        "ph":                   6.8,
        "theta_r":              0.068,
        "theta_s":              0.460,
        "alpha_vg":             0.008,
        "n_vg":                 1.09,
        "Ks_cm_day":            1.0,
    },

    "sandy_loam": {
        "description":          "Sandy loam, well-drained",
        "clay_pct":             10.0,
        "silt_pct":             25.0,
        "sand_pct":             65.0,
        "organic_carbon_pct":   1.2,
        "bulk_density_g_cm3":   1.55,
        "cec_cmol_kg":          8.0,
        "ph":                   6.0,
        "theta_r":              0.025,
        "theta_s":              0.390,
        "alpha_vg":             0.059,
        "n_vg":                 1.48,
        "Ks_cm_day":            30.0,
    },

    "peat": {
        "description":          "Organic peat / histosol",
        "clay_pct":             5.0,
        "silt_pct":             10.0,
        "sand_pct":             85.0,   # mostly fibrous OM, texture unreliable
        "organic_carbon_pct":   45.0,
        "bulk_density_g_cm3":   0.20,
        "cec_cmol_kg":          120.0,
        "ph":                   4.5,
        "theta_r":              0.010,
        "theta_s":              0.900,
        "alpha_vg":             0.013,
        "n_vg":                 1.20,
        "Ks_cm_day":            100.0,
        # Custom properties — freely extendable
        "decomposition_class":  "sapric",
        "fiber_content_pct":    35.0,
    },

    "calcareous_loam": {
        "description":          "Calcareous loam, semi-arid",
        "clay_pct":             22.0,
        "silt_pct":             38.0,
        "sand_pct":             40.0,
        "organic_carbon_pct":   0.8,
        "bulk_density_g_cm3":   1.45,
        "cec_cmol_kg":          20.0,
        "ph":                   7.9,
        "theta_r":              0.040,
        "theta_s":              0.440,
        "alpha_vg":             0.016,
        "n_vg":                 1.32,
        "Ks_cm_day":            6.0,
        # Custom properties
        "caco3_pct":            12.0,
        "gypsum_pct":           0.5,
    },
}


def register_soil_type(soil_dict: dict[str, Any], overwrite: bool = False) -> None:
    """
    Add a new soil type to the global registry.

    Required keys in soil_dict
    --------------------------
    "name"        : str   — unique identifier, e.g. "my_andosol"
    "clay_pct"    : float — clay content [%]
    "silt_pct"    : float — silt content [%]
    "sand_pct"    : float — sand content [%]  (clay+silt+sand ≈ 100)
    "theta_r"     : float — residual water content [cm³/cm³]
    "theta_s"     : float — saturated water content [cm³/cm³]
    "alpha_vg"    : float — Van Genuchten α [1/cm]
    "n_vg"        : float — Van Genuchten n [-]
    "Ks_cm_day"   : float — saturated hydraulic conductivity [cm/day]

    Optional keys (sensible defaults are applied if missing)
    --------------------------------------------------------
    "organic_carbon_pct", "bulk_density_g_cm3", "cec_cmol_kg", "ph",
    "description", … plus any custom keys you like.

    Example
    -------
    register_soil_type({
        "name":              "my_andosol",
        "description":       "Volcanic ash soil, Japan",
        "clay_pct":          15.0,
        "silt_pct":          35.0,
        "sand_pct":          50.0,
        "organic_carbon_pct": 8.0,
        "bulk_density_g_cm3": 0.75,
        "theta_r":           0.02,
        "theta_s":           0.70,
        "alpha_vg":          0.03,
        "n_vg":              1.25,
        "Ks_cm_day":         20.0,
        # Any extra property is stored and passed to all solvers:
        "allophane_pct":     12.0,
        "volcanic_glass_pct": 30.0,
    })
    """
    name = soil_dict.get("name")
    if not name:
        raise ValueError("soil_dict must contain a 'name' key.")
    _validate_soil_dict(soil_dict)
    if name in SOIL_TYPE_REGISTRY and not overwrite:
        raise KeyError(
            f"Soil type '{name}' already exists. Pass overwrite=True to replace it."
        )
    entry = copy.deepcopy(soil_dict)
    entry.pop("name")   # name is the dict key, not a field
    SOIL_TYPE_REGISTRY[name] = entry
    logger.info("Soil type registered: '%s'", name)


def list_soil_types() -> list[str]:
    """Return all registered soil type names."""
    return sorted(SOIL_TYPE_REGISTRY.keys())


def get_soil_type(name: str) -> dict[str, Any]:
    """Return a copy of a registered soil type dict."""
    if name not in SOIL_TYPE_REGISTRY:
        raise KeyError(f"Unknown soil type '{name}'. Available: {list_soil_types()}")
    return copy.deepcopy(SOIL_TYPE_REGISTRY[name])


def _validate_soil_dict(d: dict) -> None:
    _ALIASES = {
        "clay":    ["clay_content","clay_pct","clay"],
        "silt":    ["silt_content","silt_pct","silt"],
        "sand":    ["sand_content","sand_pct","sand"],
        "theta_r": ["theta_r"],
        "theta_s": ["theta_s"],
        "alpha_vg":["alpha_vg","alpha"],
        "n_vg":    ["n_vg","n"],
        "Ks":      ["Ks_cm_day","Ks","ksat"],
    }
    missing = [k for k,aliases in _ALIASES.items() if not any(a in d for a in aliases)]
    if missing:
        raise ValueError(f"Missing required soil keys: {missing}")
    def _get(keys):
        for k in keys:
            if k in d: return d[k]
        return 0.0
    total = _get(["clay_content","clay_pct","clay"]) + _get(["silt_content","silt_pct","silt"]) + _get(["sand_content","sand_pct","sand"])
    if not (99.0 <= total <= 101.0):
        raise ValueError(f"clay+silt+sand must sum ~100%, got {total:.1f}%")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — METHOD (SOLVER) REGISTRY
# Users add their own solver functions as dicts.
# Each dict carries the function itself + metadata.
# ══════════════════════════════════════════════════════════════════════════════

#: The master method registry.
#: Key = method name, Value = {"fn": callable, "description": str, ...}
METHOD_REGISTRY: dict[str, dict[str, Any]] = {}


def register_method(method_dict: dict[str, Any], overwrite: bool = False) -> None:
    """
    Register a new solver / processing function into the framework.

    Required keys in method_dict
    ----------------------------
    "name"        : str       — unique identifier, e.g. "my_nitrogen_model"
    "fn"          : callable  — the solver function (see signature below)

    Optional keys
    -------------
    "description" : str  — human-readable description
    "author"      : str
    "version"     : str
    "inputs"      : list[str]  — param/state keys your function reads
    "outputs"     : list[str]  — derived keys your function returns

    Solver function signature
    -------------------------
    def my_solver(params: dict, state: dict) -> dict:
        '''
        params  — static horizon properties (soil type + config)
        state   — current live state: theta, temperature_c, ec_ds_m, ...
                  plus any previously derived keys from earlier solvers
        returns — dict of new derived quantities to store on the horizon
        '''
        ...

    The returned dict is merged into the horizon's derived state and is
    available to all subsequent solvers in the same update cycle.

    Example — simple nitrogen mineralisation model
    -----------------------------------------------
    def nitrogen_model(params, state):
        temp   = state.get("temperature_c", 15)
        theta  = state.get("theta", 0.3)
        n_org  = params.get("total_N_pct", 0.15)      # custom soil property
        rate   = 0.01 * n_org * (theta / 0.3) * 2 ** ((temp - 20) / 10)
        return {"n_mineralisation_kg_ha_day": round(rate * 1e4, 4)}

    register_method({
        "name":        "nitrogen_mineralisation",
        "fn":          nitrogen_model,
        "description": "Simple temperature- and moisture-driven N mineralisation",
        "inputs":      ["temperature_c", "theta", "total_N_pct"],
        "outputs":     ["n_mineralisation_kg_ha_day"],
    })
    """
    name = method_dict.get("name")
    fn   = method_dict.get("fn")
    if not name:
        raise ValueError("method_dict must contain a 'name' key.")
    if not callable(fn):
        raise ValueError("method_dict must contain a callable 'fn' key.")
    if name in METHOD_REGISTRY and not overwrite:
        raise KeyError(
            f"Method '{name}' already registered. Pass overwrite=True to replace."
        )
    METHOD_REGISTRY[name] = copy.copy(method_dict)
    logger.info("Method registered: '%s'", name)


def unregister_method(name: str) -> None:
    """Remove a method from the registry."""
    METHOD_REGISTRY.pop(name, None)
    logger.info("Method unregistered: '%s'", name)


def list_methods() -> list[dict[str, Any]]:
    """Return metadata for all registered methods (without the fn callable)."""
    result = []
    for name, m in METHOD_REGISTRY.items():
        entry = {k: v for k, v in m.items() if k != "fn"}
        entry["name"] = name
        result.append(entry)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — BUILT-IN SOLVERS  (auto-registered at import)
# ══════════════════════════════════════════════════════════════════════════════

def _van_genuchten(params: dict, state: dict) -> dict:
    def _p(d,*keys,default=0):
        for k in keys:
            if k in d: return d[k]
        return default
    theta   = _p(state,"volumetric_water_content","theta",default=0.30)
    theta_r = _p(params,"theta_r",default=0.034)
    theta_s = _p(params,"theta_s",default=0.46)
    alpha   = _p(params,"alpha_vg","alpha",default=0.016)
    n       = _p(params,"n_vg","n",default=1.37)
    Ks      = _p(params,"Ks_cm_day","Ks","ksat",default=10.0)
    theta   = max(theta_r + 1e-6, min(theta, theta_s - 1e-6))
    m       = 1.0 - 1.0 / n
    Se      = (theta - theta_r) / (theta_s - theta_r)
    try:
        h  = -(1.0 / alpha) * (Se ** (-1.0 / m) - 1.0) ** (1.0 / n)
        Kr = Se ** 0.5 * (1.0 - (1.0 - Se ** (1.0 / m)) ** m) ** 2
    except (ZeroDivisionError, ValueError):
        h, Kr = -1e6, 0.0
    return {
        "matric_potential_cm":           round(h, 4),
        "effective_saturation":          round(Se, 4),
        "rel_hydraulic_conductivity":    round(Kr, 6),
        "hydraulic_conductivity_cm_day": round(Ks * Kr, 6),
    }


def _heat_equation(params: dict, state: dict) -> dict:
    theta    = state.get("theta", 0.30)
    bulk_d   = params.get("bulk_density_g_cm3", 1.4)
    clay_pct = params.get("clay_pct", 20.0)
    oc_pct   = params.get("organic_carbon_pct", 1.0)
    f_min    = (1.0 - oc_pct / 100.0) * bulk_d / 2.65
    f_om     = (oc_pct / 100.0) * bulk_d / 1.3
    f_water  = theta
    f_air    = max(0.0, 1.0 - f_min - f_om - f_water)
    C_soil   = f_min * 2.0 + f_om * 2.5 + f_water * 4.18 + f_air * 0.0013
    lam      = 0.25 + 0.5 * theta + (clay_pct / 100.0) * 0.3
    D_T      = (lam * 86400.0 / 100.0) / C_soil if C_soil > 0 else 0.0
    return {
        "thermal_diffusivity_cm2_day": round(D_T, 4),
        "heat_capacity_J_cm3_K":       round(C_soil, 4),
    }


def _carbon_flux_q10(params: dict, state: dict) -> dict:
    T        = state.get("temperature_c", 15.0)
    theta    = state.get("theta", 0.30)
    oc_pct   = params.get("organic_carbon_pct", 1.0)
    theta_s  = params.get("theta_s", 0.45)
    R_ref    = 0.02 * oc_pct
    R_T      = R_ref * (2.0 ** ((T - 10.0) / 10.0))
    theta_opt = 0.55 * theta_s
    f_theta  = max(0.0, 1.0 - ((theta - theta_opt) / max(theta_opt, 1e-6)) ** 2)
    return {"soil_respiration_mg_C_cm3_day": round(R_T * f_theta, 6)}


# Auto-register built-ins
for _name, _fn, _desc in [
    ("van_genuchten",  _van_genuchten,  "Van Genuchten hydraulic model (matric potential, K)"),
    ("heat_equation",  _heat_equation,  "De Vries soil heat equation (thermal diffusivity)"),
    ("carbon_flux_Q10",_carbon_flux_q10,"Q10 soil respiration model (CO₂ flux)"),
]:
    register_method({"name": _name, "fn": _fn, "description": _desc,
                     "author": "Digital Pedon Framework", "version": "1.0"})


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — PEDON BUILDER
# Takes a single config dict, returns a running DigitalPedon instance.
# ══════════════════════════════════════════════════════════════════════════════

# Default values applied when a key is absent from a horizon config dict
_HORIZON_DEFAULTS: dict[str, Any] = {
    "organic_carbon_pct":  1.0,
    "bulk_density_g_cm3":  1.4,
    "cec_cmol_kg":         15.0,
    "ph":                  6.5,
}

_THRESHOLD_DEFAULTS: dict[str, float] = {
    "theta_saturation":  0.44,
    "theta_wilting_pt":  0.10,
    "temp_high_c":       35.0,
    "temp_low_c":        2.0,
    "ec_high_ds_m":      4.0,
}


class DigitalPedon:
    """
    The running Digital Pedon instance.
    Built by  build_pedon(config_dict) — do not instantiate directly.
    All interaction is through dict-in / dict-out methods.
    """

    def __init__(
        self,
        pedon_id:   str,
        site_name:  str,
        horizons:   list[dict[str, Any]],
        thresholds: dict[str, float],
        latitude:   float | None,
        longitude:  float | None,
    ) -> None:
        self.pedon_id   = pedon_id
        self.site_name  = site_name
        self.latitude   = latitude
        self.longitude  = longitude
        self._thresholds = thresholds

        # Sorted horizons (top → bottom) — each is a plain dict of all params
        self._horizons: list[dict[str, Any]] = sorted(
            horizons, key=lambda h: h["depth_top_cm"]
        )

        # Live state for each horizon — stored in a plain dict
        self._state: dict[str, dict[str, Any]] = {
            h["horizon_id"]: {
                "theta":               0.30,
                "temperature_c":       15.0,
                "ec_ds_m":             0.5,
                "matric_potential_cm": -100.0,
                "flux_down_cm_day":    0.0,
                "derived":             {},
                "last_updated":        None,
            }
            for h in self._horizons
        }

        # Append-only time-series: horizon_id → list of reading dicts
        self._history: dict[str, list[dict]] = {
            h["horizon_id"]: [] for h in self._horizons
        }

        # Event handlers: list of callables(horizon_id, event_name, state_dict)
        self._event_handlers: list[Callable] = [self._default_event_handler]
        self._lock = asyncio.Lock()

    # ── Solver execution ──────────────────────────────────────────────────

    def _run_solvers(
        self,
        params: dict[str, Any],
        state:  dict[str, Any],
    ) -> dict[str, Any]:
        """Run all registered methods and return merged derived dict."""
        derived: dict[str, Any] = {}
        # Merge state + previously derived so later solvers see earlier outputs
        combined_state = {**state, **derived}
        for name, method in METHOD_REGISTRY.items():
            try:
                result = method["fn"](params, combined_state)
                derived.update(result)
                combined_state.update(result)
            except Exception as exc:
                logger.warning("Solver '%s' failed: %s", name, exc)
        return derived

    # ── Flux propagation ─────────────────────────────────────────────────

    def _darcy_flux(
        self,
        upper_params: dict, upper_state: dict,
        lower_params: dict, lower_state: dict,
        dz_cm: float,
    ) -> float:
        u = _van_genuchten(upper_params, upper_state)
        l = _van_genuchten(lower_params, lower_state)
        Km = math.sqrt(max(u["hydraulic_conductivity_cm_day"]
                           * l["hydraulic_conductivity_cm_day"], 0.0))
        grad = (l["matric_potential_cm"] - u["matric_potential_cm"]) / dz_cm
        return round(-Km * (grad + 1.0), 6)

    def _propagate_flux(self, updated_id: str) -> None:
        idx = next((i for i, h in enumerate(self._horizons)
                    if h["horizon_id"] == updated_id), None)
        if idx is None or idx >= len(self._horizons) - 1:
            return
        upper = self._horizons[idx]
        lower = self._horizons[idx + 1]
        us    = self._state[upper["horizon_id"]]
        ls    = self._state[lower["horizon_id"]]
        dz    = ((upper["depth_bottom_cm"] - upper["depth_top_cm"])
                 + (lower["depth_bottom_cm"] - lower["depth_top_cm"])) / 2.0
        flux  = self._darcy_flux(upper, us, lower, ls, dz)
        us["flux_down_cm_day"] = flux
        self._propagate_flux(lower["horizon_id"])

    # ── Threshold events ─────────────────────────────────────────────────

    def _check_thresholds(self, horizon_id: str, state: dict) -> None:
        t = self._thresholds
        events = []
        if state["theta"] >= t["theta_saturation"]:  events.append("saturation")
        if state["theta"] <= t["theta_wilting_pt"]:   events.append("wilting_point")
        if state["temperature_c"] >= t["temp_high_c"]:events.append("high_temp")
        if state["temperature_c"] <= t["temp_low_c"]: events.append("low_temp")
        if state["ec_ds_m"] >= t["ec_high_ds_m"]:     events.append("high_ec")
        for ev in events:
            for handler in self._event_handlers:
                try:
                    handler(horizon_id, ev, copy.deepcopy(state))
                except Exception as e:
                    logger.warning("Event handler error: %s", e)

    @staticmethod
    def _default_event_handler(horizon_id, event, state):
        logger.warning("[EVENT] %s — %s | θ=%.3f T=%.1f°C EC=%.2f",
                       horizon_id, event,
                       state["theta"], state["temperature_c"], state["ec_ds_m"])

    # ══════════════════════════════════════════════════════════════════════
    # PUBLIC DICT-IN / DICT-OUT API
    # ══════════════════════════════════════════════════════════════════════

    async def update(self, reading_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Ingest one sensor reading and return the updated horizon state.

        reading_dict keys
        -----------------
        "horizon_id"    : str   — must match a horizon in this pedon
        "theta"         : float — volumetric water content [cm³/cm³]
        "temperature_c" : float — soil temperature [°C]
        "ec_ds_m"       : float — electrical conductivity [dS/m]
        "timestamp"     : float — Unix epoch (default: now)

        Returns
        -------
        dict with current state + all derived quantities
        """
        hid = reading_dict.get("horizon_id")
        # Accept designation (e.g. "Ap") as well as the internal UUID horizon_id
        if hid not in self._state:
            resolved = next(
                (h["horizon_id"] for h in self._horizons
                 if h.get("designation") == hid),
                None,
            )
            if resolved is None:
                available = [h.get("designation", h["horizon_id"])
                             for h in self._horizons]
                raise KeyError(
                    f"horizon_id '{hid}' not found in this pedon. "
                    f"Available: {available}"
                )
            hid = resolved

        async with self._lock:
            ts = reading_dict.get("timestamp", time.time())

            # ① Update live state — accept both canonical and legacy key names
            def _rv(d, *keys, default):
                for k in keys:
                    if k in d: return float(d[k])
                return default

            state = self._state[hid]
            state["theta"]         = _rv(reading_dict,
                                         "volumetric_water_content", "theta",
                                         default=state["theta"])
            state["temperature_c"] = _rv(reading_dict,
                                         "soil_temperature_c", "temperature_c",
                                         default=state["temperature_c"])
            state["ec_ds_m"]       = _rv(reading_dict,
                                         "electrical_conductivity_ds_m", "ec_ds_m",
                                         default=state["ec_ds_m"])
            state["last_updated"]  = ts

            # Store any extra canonical sensor keys (NDVI, LST, SMI, etc.)
            _EXTRA_SENSOR_KEYS = {
                "ndvi", "land_surface_temperature_c", "soil_moisture_index",
                "surface_reflectance_nir", "surface_reflectance_swir1",
                "evapotranspiration_mm_day", "precipitation_mm",
                "redox_potential_mv", "ph", "co2_concentration_ppm",
                "bulk_dielectric_permittivity", "matric_potential_cm",
            }
            for _k in _EXTRA_SENSOR_KEYS:
                if _k in reading_dict:
                    state[_k] = float(reading_dict[_k])

            # ② Append to history
            self._history[hid].append({
                "timestamp":     ts,
                "theta":         state["theta"],
                "temperature_c": state["temperature_c"],
                "ec_ds_m":       state["ec_ds_m"],
            })

            # ③ Run all solvers → merge derived quantities
            params  = next(h for h in self._horizons if h["horizon_id"] == hid)
            derived = self._run_solvers(params, state)
            state["derived"].update(derived)
            if "matric_potential_cm" in derived:
                state["matric_potential_cm"] = derived["matric_potential_cm"]

            # ④ Check thresholds → fire events
            self._check_thresholds(hid, state)

            # ⑤ Propagate Darcy flux downward through the DAG
            self._propagate_flux(hid)

            return self.get_horizon_state(hid)

    def update_sync(self, reading_dict: dict[str, Any]) -> dict[str, Any]:
        """Synchronous wrapper around update() for non-async contexts."""
        return asyncio.run(self.update(reading_dict))

    def snapshot(self) -> dict[str, Any]:
        """
        Return the full current state of the pedon as a plain dict.

        Structure
        ---------
        {
            "pedon_id":  str,
            "site_name": str,
            "timestamp": float,
            "horizons": [
                {
                    "horizon_id":        str,
                    "designation":       str,
                    "depth_top_cm":      float,
                    "depth_bottom_cm":   float,
                    "soil_type":         str,
                    "state": {
                        "theta":               float,
                        "temperature_c":       float,
                        "ec_ds_m":             float,
                        "matric_potential_cm": float,
                        "flux_down_cm_day":    float,
                        "derived":             dict,
                        ...
                    }
                }, ...
            ]
        }
        """
        return {
            "pedon_id":  self.pedon_id,
            "site_name": self.site_name,
            "latitude":  self.latitude,
            "longitude": self.longitude,
            "timestamp": time.time(),
            "horizons": [
                {
                    **{k: v for k, v in h.items()
                       if k not in ("horizon_id",)},
                    "horizon_id": h["horizon_id"],
                    "state": copy.deepcopy(self._state[h["horizon_id"]]),
                }
                for h in self._horizons
            ],
        }

    def _resolve_hid(self, horizon_id: str) -> str:
        """Resolve FAO designation (e.g. 'Ap') or UUID horizon_id to the UUID key."""
        if horizon_id in self._state:
            return horizon_id
        resolved = next(
            (h["horizon_id"] for h in self._horizons
             if h.get("designation") == horizon_id),
            None,
        )
        if resolved is None:
            available = [h.get("designation", h["horizon_id"]) for h in self._horizons]
            raise KeyError(f"Unknown horizon_id: '{horizon_id}'. Available: {available}")
        return resolved

    def get_horizon_state(self, horizon_id: str) -> dict[str, Any]:
        """Return a copy of the current state dict for one horizon.
        Accepts either the UUID horizon_id or the FAO designation (e.g. 'Ap')."""
        hid = self._resolve_hid(horizon_id)
        return copy.deepcopy(self._state[hid])

    def query_history(
        self,
        horizon_id: str,
        since: float | None = None,
        until: float | None = None,
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Return time-series history for a horizon as a list of dicts.

        Parameters
        ----------
        horizon_id   : FAO designation (e.g. 'Ap') or internal UUID horizon_id
        since, until : Unix timestamps to filter the range
        limit        : max records to return (0 = all)
        """
        hid     = self._resolve_hid(horizon_id)
        records = self._history.get(hid, [])
        if since: records = [r for r in records if r["timestamp"] >= since]
        if until: records = [r for r in records if r["timestamp"] <= until]
        if limit: records = records[-limit:]
        return copy.deepcopy(records)

    def register_event_handler(self, handler: Callable) -> None:
        """
        Register a callback for threshold events.

        Signature: handler(horizon_id: str, event_name: str, state: dict)

        event_name values: "saturation", "wilting_point",
                           "high_temp", "low_temp", "high_ec"

        Example
        -------
        def my_alert(horizon_id, event, state):
            print(f"ALERT on {horizon_id}: {event} | θ={state['theta']:.3f}")

        pedon.register_event_handler(my_alert)
        """
        self._event_handlers.append(handler)

    def to_json(self, indent: int = 2) -> str:
        """Serialise the full snapshot to a JSON string."""
        snap = self.snapshot()
        # Remove non-serialisable items
        def clean(obj):
            if isinstance(obj, dict):
                return {k: clean(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [clean(v) for v in obj]
            if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            return obj
        return json.dumps(clean(snap), indent=indent)

    def __repr__(self) -> str:
        return (f"DigitalPedon(id={self.pedon_id!r}, site={self.site_name!r}, "
                f"horizons={len(self._horizons)}, methods={list(METHOD_REGISTRY)})")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — PEDON BUILDER FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def build_pedon(config_dict: dict[str, Any]) -> DigitalPedon:
    """
    Build and return a DigitalPedon from a plain configuration dict.

    config_dict keys
    ----------------
    "site_name"   : str          (optional, default "Unnamed Site")
    "pedon_id"    : str          (optional, auto-generated if absent)
    "latitude"    : float        (optional)
    "longitude"   : float        (optional)
    "thresholds"  : dict         (optional, see _THRESHOLD_DEFAULTS)
    "horizons"    : list[dict]   (REQUIRED)

    Each horizon dict
    -----------------
    "designation"      : str    — e.g. "Ap", "B1", "C"           (required)
    "depth_top_cm"     : float                                     (required)
    "depth_bottom_cm"  : float                                     (required)
    "soil_type"        : str    — name from SOIL_TYPE_REGISTRY     (optional)
                                  If given, its properties are used as base.
    "horizon_id"       : str    — auto-generated if absent
    + any extra soil property that overrides or extends the soil_type base

    Example
    -------
    pedon = build_pedon({
        "site_name": "North Field",
        "latitude":  51.505,
        "longitude": 4.469,
        "thresholds": {
            "theta_saturation": 0.46,
            "theta_wilting_pt": 0.08,
        },
        "horizons": [
            {
                "designation":    "Ap",
                "depth_top_cm":   0,
                "depth_bottom_cm":25,
                "soil_type":      "loamy_topsoil",
                # Override one property from the base soil type:
                "organic_carbon_pct": 3.5,
                # Add a completely new property (any solver can read it):
                "microplastics_mg_kg": 12.4,
            },
            {
                "designation":    "Bw",
                "depth_top_cm":   25,
                "depth_bottom_cm":70,
                "soil_type":      "clay_subsoil",
            },
        ],
    })
    """
    # ── Pedon-level fields ────────────────────────────────────────────────
    pedon_id   = config_dict.get("pedon_id", str(uuid.uuid4())[:12])
    site_name  = config_dict.get("site_name", "Unnamed Site")
    latitude   = config_dict.get("latitude")
    longitude  = config_dict.get("longitude")

    # Merge user thresholds over defaults
    thresholds = {**_THRESHOLD_DEFAULTS, **config_dict.get("thresholds", {})}

    # ── Horizon resolution ────────────────────────────────────────────────
    raw_horizons = config_dict.get("horizons")
    if not raw_horizons:
        raise ValueError("config_dict must contain a non-empty 'horizons' list.")

    resolved: list[dict[str, Any]] = []
    for raw in raw_horizons:
        _check_horizon_keys(raw)

        # Start from soil_type base (if given), then overlay horizon-specific keys
        base: dict[str, Any] = {}
        soil_type_name = raw.get("soil_type")
        if soil_type_name:
            base = get_soil_type(soil_type_name)   # deep copy from registry

        # Apply defaults for any still-missing keys
        for k, v in _HORIZON_DEFAULTS.items():
            base.setdefault(k, v)

        # Overlay everything from raw (horizon-specific overrides win)
        merged = {**base, **raw}

        # Ensure horizon_id
        merged.setdefault("horizon_id", str(uuid.uuid4())[:8])

        # Validate texture if all three are present
        _clay = next((merged[k] for k in ("clay_content","clay_pct","clay") if k in merged), None)
        _silt = next((merged[k] for k in ("silt_content","silt_pct","silt") if k in merged), None)
        _sand = next((merged[k] for k in ("sand_content","sand_pct","sand") if k in merged), None)
        if all(v is not None for v in (_clay,_silt,_sand)):
            total = _clay + _silt + _sand
            if not (99.0 <= total <= 101.0):
                raise ValueError(
                    f"Horizon '{merged['designation']}': clay+silt+sand = {total:.1f}% (must be ~100%)"
                )

        resolved.append(merged)

    # Validate no depth gaps / overlaps
    sorted_h = sorted(resolved, key=lambda h: h["depth_top_cm"])
    for i in range(1, len(sorted_h)):
        prev_bot = sorted_h[i - 1]["depth_bottom_cm"]
        curr_top = sorted_h[i]["depth_top_cm"]
        if abs(prev_bot - curr_top) > 0.01:
            raise ValueError(
                f"Depth gap/overlap between horizons at {prev_bot}–{curr_top} cm"
            )

    pedon = DigitalPedon(
        pedon_id=pedon_id, site_name=site_name,
        horizons=resolved, thresholds=thresholds,
        latitude=latitude, longitude=longitude,
    )
    logger.info("Built pedon '%s' (%s) with %d horizons and %d active methods.",
                site_name, pedon_id, len(resolved), len(METHOD_REGISTRY))
    return pedon


def _check_horizon_keys(h: dict) -> None:
    required = ["designation", "depth_top_cm", "depth_bottom_cm"]
    missing = [k for k in required if k not in h]
    if missing:
        raise ValueError(f"Horizon dict missing required keys: {missing}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — CONVENIENCE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def make_reading(
    horizon_id: str,
    theta: float,
    temperature_c: float,
    ec_ds_m: float,
    timestamp: float | None = None,
    **extra,
) -> dict[str, Any]:
    """
    Convenience constructor for a sensor reading dict.
    Keyword arguments are passed through as extra fields.
    """
    return {
        "horizon_id":    horizon_id,
        "theta":         theta,
        "temperature_c": temperature_c,
        "ec_ds_m":       ec_ds_m,
        "timestamp":     timestamp or time.time(),
        **extra,
    }


def print_snapshot(pedon: DigitalPedon) -> None:
    """Pretty-print the current pedon state to stdout."""
    snap = pedon.snapshot()
    w = 78
    print(f"\n{'═'*w}")
    print(f"  {snap['site_name']}   [{snap['pedon_id']}]   "
          f"{time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Active methods: {list(METHOD_REGISTRY.keys())}")
    print(f"{'─'*w}")
    for h in snap["horizons"]:
        s = h.get("state", {})
        d = s.get("derived", {})
        print(
            f"  [{h.get('designation','?'):4s}] "
            f"{h['depth_top_cm']:4.0f}–{h['depth_bottom_cm']:.0f}cm | "
            f"θ={s.get('theta',0):.3f}  "
            f"T={s.get('temperature_c',0):5.1f}°C  "
            f"EC={s.get('ec_ds_m',0):.2f}dS/m  "
            f"ψ={s.get('matric_potential_cm',0):8.1f}cm  "
            f"q↓={s.get('flux_down_cm_day',0):+.4f}cm/d"
        )
        # Print any derived quantities that aren't the standard set
        standard = {"matric_potential_cm","effective_saturation",
                    "rel_hydraulic_conductivity","hydraulic_conductivity_cm_day",
                    "thermal_diffusivity_cm2_day","heat_capacity_J_cm3_K"}
        extras = {k: v for k, v in d.items() if k not in standard}
        if extras:
            print(f"  {'':>6}  derived → " +
                  "  ".join(f"{k}={v:.5g}" if isinstance(v, (int, float)) else f"{k}={v!r}" for k, v in extras.items()))
    print(f"{'═'*w}\n")
