"""Tests for pedon_api core — build, update, solvers, history, events."""
import math
import time
import pytest

from digital_pedon import (
    build_pedon, register_method, unregister_method,
    list_methods, make_reading, SOIL_TYPE_REGISTRY,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def simple_pedon():
    return build_pedon({
        "site_name": "Test Site",
        "horizons": [
            {"designation":"Ap","depth_top_cm":0, "depth_bottom_cm":25,
             "soil_type":"loamy_topsoil"},
            {"designation":"Bw","depth_top_cm":25,"depth_bottom_cm":65,
             "soil_type":"clay_subsoil"},
        ],
    })


# ── Build ─────────────────────────────────────────────────────────────────────

def test_build_pedon_returns_instance(simple_pedon):
    assert simple_pedon is not None
    assert simple_pedon.site_name == "Test Site"

def test_build_requires_horizons():
    with pytest.raises((ValueError, Exception)):
        build_pedon({"site_name": "No horizons"})

def test_build_depth_gap_raises():
    with pytest.raises(ValueError, match="gap"):
        build_pedon({"horizons": [
            {"designation":"Ap","depth_top_cm":0,"depth_bottom_cm":25,
             "soil_type":"loamy_topsoil"},
            {"designation":"Bw","depth_top_cm":30,"depth_bottom_cm":65,  # gap!
             "soil_type":"clay_subsoil"},
        ]})

def test_build_texture_sum_raises():
    with pytest.raises(ValueError):
        build_pedon({"horizons": [
            {"designation":"Ap","depth_top_cm":0,"depth_bottom_cm":25,
             "clay_pct":50,"silt_pct":50,"sand_pct":50,   # sums to 150
             "theta_r":0.03,"theta_s":0.45,"alpha_vg":0.02,"n_vg":1.4,"Ks_cm_day":15},
        ]})

def test_soil_type_registry_populated():
    for st in ("loamy_topsoil","clay_subsoil","sandy_loam","peat","calcareous_loam"):
        assert st in SOIL_TYPE_REGISTRY


# ── Update & solvers ──────────────────────────────────────────────────────────

def test_update_returns_dict(simple_pedon):
    result = simple_pedon.update_sync({
        "horizon_id": "Ap",
        "volumetric_water_content": 0.30,
        "soil_temperature_c": 15.0,
    })
    assert isinstance(result, dict)

def test_van_genuchten_keys_present(simple_pedon):
    result = simple_pedon.update_sync({
        "horizon_id": "Ap",
        "volumetric_water_content": 0.30,
        "soil_temperature_c": 15.0,
    })
    assert "matric_potential_cm" in result
    assert "hydraulic_conductivity_cm_day" in result
    assert "effective_saturation" in result

def test_matric_potential_negative(simple_pedon):
    result = simple_pedon.update_sync({
        "horizon_id": "Ap",
        "volumetric_water_content": 0.25,
        "soil_temperature_c": 15.0,
    })
    assert result["matric_potential_cm"] < 0

def test_flux_computed_after_two_horizons(simple_pedon):
    simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.30,"soil_temperature_c":15.0})
    simple_pedon.update_sync({
        "horizon_id":"Bw","volumetric_water_content":0.35,"soil_temperature_c":14.0})
    snap = simple_pedon.snapshot()
    ap_flux = snap["horizons"][0]["state"].get("flux_down_cm_day")
    assert ap_flux is not None
    assert not math.isnan(ap_flux)

def test_carbon_flux_positive(simple_pedon):
    result = simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.30,"soil_temperature_c":20.0})
    assert result["derived"].get("soil_respiration_mg_C_cm3_day", -1) >= 0

def test_heat_equation_keys(simple_pedon):
    result = simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.30,"soil_temperature_c":15.0})
    assert "thermal_diffusivity_cm2_day" in result["derived"]
    assert "heat_capacity_J_cm3_K" in result["derived"]

def test_unknown_horizon_raises(simple_pedon):
    with pytest.raises(KeyError):
        simple_pedon.update_sync({
            "horizon_id":"NONEXISTENT","volumetric_water_content":0.3,"soil_temperature_c":15})


# ── Custom solvers ────────────────────────────────────────────────────────────

def test_register_custom_solver(simple_pedon):
    def my_solver(params, state):
        return {"my_custom_output": 42.0}

    register_method({"name":"test_solver","fn":my_solver})
    result = simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.30,"soil_temperature_c":15.0})
    assert result["derived"].get("my_custom_output") == 42.0
    unregister_method("test_solver")

def test_register_duplicate_raises():
    def fn1(p,s): return {}
    register_method({"name":"dup_test","fn":fn1})
    with pytest.raises(KeyError):
        register_method({"name":"dup_test","fn":fn1})
    unregister_method("dup_test")

def test_register_overwrite(simple_pedon):
    def fn1(p,s): return {"v":1}
    def fn2(p,s): return {"v":2}
    register_method({"name":"overwrite_test","fn":fn1})
    register_method({"name":"overwrite_test","fn":fn2}, overwrite=True)
    result = simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.30,"soil_temperature_c":15.0})
    assert result["derived"]["v"] == 2
    unregister_method("overwrite_test")

def test_solver_chain_receives_earlier_output(simple_pedon):
    """Second solver should see output from Van Genuchten."""
    def uses_psi(params, state):
        psi = state.get("matric_potential_cm", 0)
        return {"psi_seen_by_chain": psi}
    register_method({"name":"chain_test","fn":uses_psi})
    result = simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.28,"soil_temperature_c":15.0})
    assert result["derived"]["psi_seen_by_chain"] < 0
    unregister_method("chain_test")


# ── History ───────────────────────────────────────────────────────────────────

def test_history_grows(simple_pedon):
    for i in range(5):
        simple_pedon.update_sync({
            "horizon_id":"Ap",
            "volumetric_water_content": 0.28 + i * 0.01,
            "soil_temperature_c": 14.0,
        })
    history = simple_pedon.query_history("Ap")
    assert len(history) >= 5

def test_history_limit(simple_pedon):
    for _ in range(10):
        simple_pedon.update_sync({
            "horizon_id":"Ap","volumetric_water_content":0.30,"soil_temperature_c":14.0})
    history = simple_pedon.query_history("Ap", limit=3)
    assert len(history) == 3

def test_history_since_filter(simple_pedon):
    t0 = time.time()
    simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.30,
        "soil_temperature_c":14.0,"timestamp":t0 - 7200})
    simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.31,
        "soil_temperature_c":14.0,"timestamp":t0 - 3600})
    simple_pedon.update_sync({
        "horizon_id":"Ap","volumetric_water_content":0.32,
        "soil_temperature_c":14.0,"timestamp":t0})
    recent = simple_pedon.query_history("Ap", since=t0 - 1800)
    assert len(recent) == 1


# ── Threshold events ─────────────────────────────────────────────────────────

def test_saturation_event_fires(simple_pedon):
    events = []
    simple_pedon.register_event_handler(
        lambda hid, ev, st: events.append(ev))
    simple_pedon.update_sync({
        "horizon_id":"Ap",
        "volumetric_water_content": 0.50,   # > 0.44 threshold
        "soil_temperature_c": 14.0,
    })
    assert "saturation" in events

def test_wilting_event_fires(simple_pedon):
    events = []
    simple_pedon.register_event_handler(
        lambda hid, ev, st: events.append(ev))
    simple_pedon.update_sync({
        "horizon_id":"Ap",
        "volumetric_water_content": 0.05,   # < 0.10 threshold
        "soil_temperature_c": 14.0,
    })
    assert "wilting_point" in events

def test_high_temp_event_fires(simple_pedon):
    events = []
    simple_pedon.register_event_handler(
        lambda hid, ev, st: events.append(ev))
    simple_pedon.update_sync({
        "horizon_id":"Ap",
        "volumetric_water_content": 0.30,
        "soil_temperature_c": 40.0,          # > 35°C
    })
    assert "high_temp" in events


# ── Snapshot & JSON ───────────────────────────────────────────────────────────

def test_snapshot_structure(simple_pedon):
    snap = simple_pedon.snapshot()
    assert "pedon_id" in snap
    assert "horizons" in snap
    assert len(snap["horizons"]) == 2

def test_to_json_valid(simple_pedon):
    import json
    js = simple_pedon.to_json()
    data = json.loads(js)
    assert "pedon_id" in data

def test_make_reading_helper():
    r = make_reading("Ap", theta=0.31, temperature_c=14.2, ec_ds_m=0.4)
    assert r["horizon_id"] == "Ap"
    assert r["theta"] == 0.31
    assert "timestamp" in r
