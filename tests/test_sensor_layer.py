"""Tests for the BYOD sensor layer — manifest, unit conversion, ingestion."""
import json
import pytest

from digital_pedon.sensor import (
    SensorField, SensorManifest, SensorLayer, ManifestBuilder,
    CANONICAL_PROPERTIES,
    example_manifest_insitu, example_manifest_with_remote_sensing,
)


# ── CANONICAL_PROPERTIES ─────────────────────────────────────────────────────

def test_canonical_properties_populated():
    assert len(CANONICAL_PROPERTIES) >= 10

def test_canonical_properties_have_required_keys():
    for key, meta in CANONICAL_PROPERTIES.items():
        assert "si_unit"     in meta, f"{key} missing si_unit"
        assert "aliases"     in meta, f"{key} missing aliases"
        assert "description" in meta, f"{key} missing description"


# ── SensorField ───────────────────────────────────────────────────────────────

def test_sensor_field_canonical_alias_resolution():
    # "theta" is an alias for "volumetric_water_content"
    f = SensorField(source_key="VWC", canonical_key="theta",
                    unit="cm3/cm3", horizon_id="Ap")
    assert f.canonical_key == "volumetric_water_content"

def test_sensor_field_unknown_canonical_raises():
    with pytest.raises(ValueError, match="Unknown"):
        SensorField(source_key="x", canonical_key="nonexistent_key_xyz",
                    unit="cm3/cm3", horizon_id="Ap")

def test_sensor_field_to_dict_roundtrip():
    f = SensorField(source_key="VWC_pct", canonical_key="volumetric_water_content",
                    unit="%", horizon_id="Bw", depth_cm=45.0,
                    sampling_rate_s=3600, sensor_type="TEROS 11",
                    data_origin="in_situ")
    d = f.to_dict()
    f2 = SensorField.from_dict(d)
    assert f2.canonical_key == f.canonical_key
    assert f2.unit          == f.unit
    assert f2.horizon_id    == f.horizon_id
    assert f2.depth_cm      == f.depth_cm


# ── Unit conversion ───────────────────────────────────────────────────────────

def _make_layer(source_key, canonical_key, unit, horizon_id="Ap"):
    manifest = SensorManifest.from_dict({"fields": [{
        "source_key":    source_key,
        "canonical_key": canonical_key,
        "unit":          unit,
        "horizon_id":    horizon_id,
    }]})
    return SensorLayer(manifest)

def test_unit_pct_to_cm3cm3():
    layer = _make_layer("vwc", "volumetric_water_content", "%")
    r = layer.ingest({"vwc": 30.0})
    assert abs(r["volumetric_water_content"] - 0.30) < 1e-9

def test_unit_degF_to_degC():
    layer = _make_layer("temp_F", "soil_temperature_c", "degF")
    r = layer.ingest({"temp_F": 59.0})          # 59 °F = 15 °C
    assert abs(r["soil_temperature_c"] - 15.0) < 1e-6

def test_unit_kelvin_to_degC():
    layer = _make_layer("temp_K", "soil_temperature_c", "K")
    r = layer.ingest({"temp_K": 288.15})        # 288.15 K = 15.0 °C
    assert abs(r["soil_temperature_c"] - 15.0) < 1e-4

def test_unit_kPa_to_cm():
    layer = _make_layer("psi_kPa", "matric_potential_cm", "kPa")
    r = layer.ingest({"psi_kPa": -10.0})        # −10 kPa ≈ −101.97 cm
    assert r["matric_potential_cm"] < -100

def test_unit_uScm_to_dsm():
    layer = _make_layer("ec_uScm", "electrical_conductivity_ds_m", "μS/cm")
    r = layer.ingest({"ec_uScm": 380.0})        # 380 μS/cm = 0.38 dS/m
    assert abs(r["electrical_conductivity_ds_m"] - 0.38) < 1e-9

def test_unit_DN_to_reflectance():
    layer = _make_layer("B8_DN", "surface_reflectance_nir", "DN")
    r = layer.ingest({"B8_DN": 2850.0})         # 2850 / 10000 = 0.285
    assert abs(r["surface_reflectance_nir"] - 0.285) < 1e-9

def test_unit_already_si_unchanged():
    layer = _make_layer("vwc", "volumetric_water_content", "cm3/cm3")
    r = layer.ingest({"vwc": 0.31})
    assert abs(r["volumetric_water_content"] - 0.31) < 1e-9


# ── SensorManifest ────────────────────────────────────────────────────────────

def test_manifest_from_dict():
    m = SensorManifest.from_dict({"fields": [
        {"source_key":"vwc","canonical_key":"volumetric_water_content",
         "unit":"cm3/cm3","horizon_id":"Ap"},
    ]})
    assert len(m.fields) == 1

def test_manifest_json_roundtrip():
    m = example_manifest_insitu()
    js = m.to_json()
    m2 = SensorManifest.from_dict(json.loads(js))
    assert len(m2.fields) == len(m.fields)
    assert m2.fields[0].canonical_key == m.fields[0].canonical_key

def test_manifest_summary_is_string():
    m = example_manifest_insitu()
    s = m.summary()
    assert isinstance(s, str)
    assert len(s) > 0

def test_example_manifest_insitu():
    m = example_manifest_insitu()
    assert len(m.fields) >= 3
    keys = {f.canonical_key for f in m.fields}
    assert "volumetric_water_content" in keys
    assert "soil_temperature_c" in keys

def test_example_manifest_with_rs():
    m = example_manifest_with_remote_sensing()
    keys = {f.canonical_key for f in m.fields}
    assert "ndvi" in keys


# ── ManifestBuilder ───────────────────────────────────────────────────────────

def test_manifest_builder_fluent():
    m = (
        ManifestBuilder("Test Site", "T001")
        .add_insitu(source_key="vwc", canonical_key="volumetric_water_content",
                    unit="%", horizon_id="Ap", depth_cm=15)
        .add_remote_sensing(source_key="ndvi", canonical_key="ndvi",
                            unit="dimensionless", horizon_id="Ap", depth_cm=0)
        .add_manual(source_key="ph_manual", canonical_key="ph",
                    unit="dimensionless", horizon_id="Ap")
        .build()
    )
    assert len(m.fields) == 3
    origins = {f.data_origin for f in m.fields}
    assert origins == {"in_situ", "remote_sensing", "manual"}


# ── SensorLayer.ingest ────────────────────────────────────────────────────────

def test_ingest_adds_horizon_id():
    m = SensorManifest.from_dict({"fields": [{
        "source_key":"vwc","canonical_key":"volumetric_water_content",
        "unit":"cm3/cm3","horizon_id":"Bw"}]})
    layer = SensorLayer(m)
    r = layer.ingest({"vwc": 0.35})
    assert r["horizon_id"] == "Bw"

def test_ingest_adds_timestamp():
    m = SensorManifest.from_dict({"fields": [{
        "source_key":"vwc","canonical_key":"volumetric_water_content",
        "unit":"cm3/cm3","horizon_id":"Ap"}]})
    layer = SensorLayer(m)
    r = layer.ingest({"vwc": 0.30})
    assert "timestamp" in r
    assert r["timestamp"] > 0

def test_ingest_missing_required_raises():
    m = SensorManifest.from_dict({"fields": [{
        "source_key":"vwc","canonical_key":"volumetric_water_content",
        "unit":"cm3/cm3","horizon_id":"Ap","optional":False}]})
    layer = SensorLayer(m)
    with pytest.raises(ValueError):
        layer.ingest({})   # vwc is required but absent

def test_ingest_missing_optional_ok():
    m = SensorManifest.from_dict({"fields": [
        {"source_key":"vwc","canonical_key":"volumetric_water_content",
         "unit":"cm3/cm3","horizon_id":"Ap"},
        {"source_key":"ndvi_opt","canonical_key":"ndvi",
         "unit":"dimensionless","horizon_id":"Ap","optional":True},
    ]})
    layer = SensorLayer(m)
    r = layer.ingest({"vwc": 0.30})   # ndvi_opt absent — should not raise
    assert "volumetric_water_content" in r
    assert "ndvi" not in r


# ── SensorLayer.split_by_horizon ──────────────────────────────────────────────

def test_split_by_horizon_two_horizons():
    m = SensorManifest.from_dict({"fields": [
        {"source_key":"vwc_ap","canonical_key":"volumetric_water_content",
         "unit":"cm3/cm3","horizon_id":"Ap"},
        {"source_key":"vwc_bw","canonical_key":"volumetric_water_content",
         "unit":"cm3/cm3","horizon_id":"Bw"},
    ]})
    layer = SensorLayer(m)
    readings = layer.split_by_horizon({"vwc_ap": 0.30, "vwc_bw": 0.38})
    assert len(readings) == 2
    hids = {r["horizon_id"] for r in readings}
    assert hids == {"Ap", "Bw"}

def test_split_values_correct_after_conversion():
    m = SensorManifest.from_dict({"fields": [
        {"source_key":"vwc_ap_pct","canonical_key":"volumetric_water_content",
         "unit":"%","horizon_id":"Ap"},
        {"source_key":"temp_ap_F","canonical_key":"soil_temperature_c",
         "unit":"degF","horizon_id":"Ap"},
    ]})
    layer = SensorLayer(m)
    readings = layer.split_by_horizon({"vwc_ap_pct": 28.5, "temp_ap_F": 59.0})
    ap = readings[0]
    assert abs(ap["volumetric_water_content"] - 0.285) < 1e-9
    assert abs(ap["soil_temperature_c"] - 15.0) < 1e-6


# ── ingest_csv_row ────────────────────────────────────────────────────────────

def test_ingest_csv_row_converts_strings():
    m = SensorManifest.from_dict({"fields": [{
        "source_key":"vwc","canonical_key":"volumetric_water_content",
        "unit":"cm3/cm3","horizon_id":"Ap"}]})
    layer = SensorLayer(m)
    r = layer.ingest_csv_row({"vwc": "0.31", "timestamp": "1741600000"})
    assert isinstance(r["volumetric_water_content"], float)
    assert abs(r["volumetric_water_content"] - 0.31) < 1e-9
