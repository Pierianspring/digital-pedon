"""Tests for ontology vocab — canonical keys, alias resolution, JSON-LD."""
import pytest

from digital_pedon.ontology import (
    canonical_key, normalise_dict, describe,
    to_jsonld, list_properties, unit_of, PROPERTIES,
)


# ── PROPERTIES registry ───────────────────────────────────────────────────────

def test_properties_populated():
    assert len(PROPERTIES) >= 20

def test_all_properties_have_uri():
    for key, prop in PROPERTIES.items():
        assert prop.uri, f"Property '{key}' has no URI"

def test_all_properties_have_unit():
    for key, prop in PROPERTIES.items():
        assert prop.unit is not None, f"Property '{key}' has no unit"

def test_all_properties_have_description():
    for key, prop in PROPERTIES.items():
        assert prop.description, f"Property '{key}' has no description"


# ── canonical_key ─────────────────────────────────────────────────────────────

def test_canonical_key_self():
    # canonical keys map to themselves
    for key in PROPERTIES:
        assert canonical_key(key) == key

def test_canonical_key_aliases():
    # Well-known aliases
    assert canonical_key("theta")        == "volumetric_water_content"
    assert canonical_key("VWC")          == "volumetric_water_content"
    assert canonical_key("clay_pct")     == "clay_content"
    assert canonical_key("CLAY")         == "clay_content"
    assert canonical_key("BD")           == "bulk_density"
    assert canonical_key("temperature_c")== "soil_temperature"
    assert canonical_key("Ks_cm_day")    == "Ks"
    assert canonical_key("ec_ds_m")      == "electrical_conductivity"
    assert canonical_key("ph")           == "ph_h2o"

def test_canonical_key_case_insensitive():
    assert canonical_key("THETA")         == "volumetric_water_content"
    assert canonical_key("Clay_Pct")      == "clay_content"

def test_canonical_key_unknown_returns_original():
    # Unknown keys pass through unchanged
    result = canonical_key("totally_unknown_key_xyz")
    assert result == "totally_unknown_key_xyz"


# ── normalise_dict ────────────────────────────────────────────────────────────

def test_normalise_dict_translates_aliases():
    raw = {"CLAY": 22.0, "BD": 1.4, "theta": 0.31, "pH": 6.5}
    norm = normalise_dict(raw)
    assert "clay_content"           in norm
    assert "bulk_density"           in norm
    assert "volumetric_water_content" in norm
    assert "ph_h2o"                 in norm

def test_normalise_dict_preserves_values():
    raw = {"theta": 0.31}
    norm = normalise_dict(raw)
    assert norm["volumetric_water_content"] == 0.31

def test_normalise_dict_unknown_keys_pass_through():
    raw = {"my_custom_key": 42.0}
    norm = normalise_dict(raw)
    assert norm["my_custom_key"] == 42.0


# ── describe ─────────────────────────────────────────────────────────────────

def test_describe_known_key():
    info = describe("volumetric_water_content")
    assert info is not None
    assert info["key"]      == "volumetric_water_content"
    assert "uri"            in info
    assert "unit"           in info
    assert "description"    in info
    assert "aliases"        in info

def test_describe_unknown_key_returns_none():
    assert describe("nonexistent_xyz") is None

def test_describe_unit_correct():
    assert describe("volumetric_water_content")["unit"] == "cm³/cm³"
    assert describe("bulk_density")["unit"]             == "g/cm³"
    assert describe("clay_content")["unit"]             == "%"


# ── unit_of ───────────────────────────────────────────────────────────────────

def test_unit_of_canonical():
    assert unit_of("clay_content")              == "%"
    assert unit_of("bulk_density")              == "g/cm³"
    assert unit_of("volumetric_water_content")  == "cm³/cm³"

def test_unit_of_alias():
    assert unit_of("theta") == "cm³/cm³"
    assert unit_of("BD")    == "g/cm³"

def test_unit_of_unknown_returns_none():
    assert unit_of("nonexistent_xyz") is None


# ── list_properties ────────────────────────────────────────────────────────────

def test_list_properties_returns_list():
    props = list_properties()
    assert isinstance(props, list)
    assert len(props) == len(PROPERTIES)

def test_list_properties_filter_by_standard():
    glosis = list_properties(standard="GLOSIS")
    assert len(glosis) > 0
    for key in glosis:
        assert "GLOSIS" in PROPERTIES[key].standard

def test_list_properties_all_are_known_keys():
    for key in list_properties():
        assert key in PROPERTIES


# ── to_jsonld ─────────────────────────────────────────────────────────────────

def test_to_jsonld_adds_context():
    data = {"clay_content": 22.0, "bulk_density": 1.4, "theta": 0.31}
    # normalise first
    from digital_pedon.ontology import normalise_dict
    norm = normalise_dict(data)
    ld   = to_jsonld(norm)
    assert "@context" in ld

def test_to_jsonld_context_has_uris():
    data = normalise_dict({"clay_content": 22.0})
    ld   = to_jsonld(data)
    ctx  = ld["@context"]
    assert "clay_content" in ctx
    uri_entry = ctx["clay_content"]
    assert "@id" in uri_entry
    assert "glosis" in uri_entry["@id"].lower() or "w3id" in uri_entry["@id"].lower()

def test_to_jsonld_preserves_values():
    data = normalise_dict({"theta": 0.31})
    ld   = to_jsonld(data)
    assert ld["volumetric_water_content"] == 0.31


# ── GLOSIS URIs sanity ────────────────────────────────────────────────────────

EXPECTED_GLOSIS_PROPERTIES = [
    "clay_content", "silt_content", "sand_content",
    "organic_carbon", "ph_h2o", "bulk_density",
    "volumetric_water_content", "electrical_conductivity",
]

def test_glosis_properties_have_w3id_uris():
    for key in EXPECTED_GLOSIS_PROPERTIES:
        prop = PROPERTIES.get(key)
        assert prop is not None, f"Missing property: {key}"
        assert "w3id.org/glosis" in prop.uri or "digitalpedon" in prop.uri, \
               f"Unexpected URI for {key}: {prop.uri}"
