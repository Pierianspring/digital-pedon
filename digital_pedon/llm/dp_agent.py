"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              DIGITAL PEDON — LLM AGENT DISPATCHER  (v2)                   ║
║                                                                              ║
║  Connects an LLM agent to live DigitalPedon instances.                     ║
║                                                                              ║
║  Workflow                                                                    ║
║  ────────                                                                    ║
║  1. LLM receives dp_llm_context.json as system context.                    ║
║  2. LLM receives dp_tools.json as its tool schema.                         ║
║  3. LLM emits a tool_use block, e.g.:                                      ║
║       {"name":"dp_update","input":{"pedon_id":"abc","horizon_id":"Ap",...}} ║
║  4. Your code calls:  dispatcher.run(tool_name, tool_input)                ║
║  5. The dispatcher executes the call against the live pedon instance.       ║
║  6. Returns a JSON-serialisable dict → feed back to LLM as tool_result.   ║
║                                                                              ║
║  Usage                                                                       ║
║  ─────                                                                       ║
║  from dp_agent import PedonDispatcher                                       ║
║  dispatcher = PedonDispatcher()                                             ║
║  result = dispatcher.run("dp_build_pedon", config_dict)                    ║
║  result = dispatcher.run("dp_update", reading_dict)                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

Authors : Badreldin, N. & Youssef, A. (2026)
License : CC BY 4.0
"""

from __future__ import annotations

import json
import math
import time
from typing import Any

from digital_pedon.pedon_api import build_pedon, DigitalPedon, register_method
from digital_pedon.sensor.sensor_layer import (SensorManifest, SensorLayer, SensorField,
                           CANONICAL_PROPERTIES)


# ══════════════════════════════════════════════════════════════════════════════
# DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

class PedonDispatcher:
    """
    Session-aware dispatcher that maps LLM tool-call names to Python calls.

    Maintains a registry of active DigitalPedon instances (one per pedon_id)
    and a registry of SensorLayer instances (one per pedon_id that has
    registered a manifest).

    All methods accept and return JSON-serialisable dicts.

    Usage (standalone)
    ------------------
    dispatcher = PedonDispatcher()

    # LLM asks to build a pedon
    result = dispatcher.run("dp_build_pedon", {
        "site_name": "My Site",
        "horizons":  [{"designation":"Ap","depth_top_cm":0,"depth_bottom_cm":28,
                       "soil_type":"loamy_topsoil"}]
    })
    pedon_id = result["pedon_id"]

    # LLM asks to ingest a reading
    result = dispatcher.run("dp_update", {
        "pedon_id":                  pedon_id,
        "horizon_id":                "Ap",
        "volumetric_water_content":  0.31,
        "soil_temperature_c":        14.2,
    })
    print(result)

    Usage (with Anthropic API)
    --------------------------
    import anthropic, json
    from dp_agent import PedonDispatcher

    dispatcher = PedonDispatcher()
    tools      = json.load(open("dp_tools.json"))["tools"]
    context    = json.load(open("dp_llm_context.json"))

    client  = anthropic.Anthropic()
    messages = [{"role":"user","content":"Build a loamy topsoil pedon for Ghent, Belgium"}]

    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=json.dumps(context),
            tools=tools,
            messages=messages,
        )
        if resp.stop_reason == "end_turn":
            print(resp.content[0].text)
            break
        # Process tool uses
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result = dispatcher.run(block.name, block.input)
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     json.dumps(result),
                })
        messages.append({"role":"assistant","content":resp.content})
        messages.append({"role":"user",     "content":tool_results})
    """

    def __init__(self):
        self._pedons:  dict[str, DigitalPedon] = {}
        self._layers:  dict[str, SensorLayer]  = {}
        self._manifests: dict[str, SensorManifest] = {}

    # ── Main dispatch entry point ─────────────────────────────────────────

    def run(self, tool_name: str, tool_input: dict[str, Any]) -> dict[str, Any]:
        """
        Dispatch a tool call by name. Returns a JSON-serialisable dict.

        Parameters
        ----------
        tool_name  : str   — one of the dp_* tool names from dp_tools.json
        tool_input : dict  — the tool_input payload from the LLM

        Raises
        ------
        ValueError if tool_name is unknown.
        """
        handlers = {
            "dp_build_pedon":             self._build_pedon,
            "dp_update":                  self._update,
            "dp_snapshot":                self._snapshot,
            "dp_query_history":           self._query_history,
            "dp_register_sensor_manifest":self._register_sensor_manifest,
            "dp_ingest_raw":              self._ingest_raw,
            "dp_register_solver":         self._register_solver,
            "dp_list_canonical_properties":self._list_canonical_properties,
            "dp_export_jsonld":           self._export_jsonld,
            "dp_describe":                self._describe,
        }
        handler = handlers.get(tool_name)
        if handler is None:
            return {"error": f"Unknown tool: '{tool_name}'. "
                             f"Available: {list(handlers.keys())}"}
        try:
            return self._clean(handler(tool_input))
        except Exception as exc:
            return {"error": str(exc), "tool": tool_name}

    # ── Tool implementations ───────────────────────────────────────────────

    def _build_pedon(self, inp: dict) -> dict:
        pedon = build_pedon(inp)
        self._pedons[pedon.pedon_id] = pedon
        return {
            "status":    "ok",
            "pedon_id":  pedon.pedon_id,
            "site_name": pedon.site_name,
            "horizons":  [h["designation"] for h in pedon._horizons],
            "message":   f"Digital Pedon '{pedon.site_name}' built with "
                         f"{len(pedon._horizons)} horizon(s).",
        }

    def _update(self, inp: dict) -> dict:
        pedon = self._get_pedon(inp)
        reading = {k: v for k, v in inp.items() if k != "pedon_id"}
        result = pedon.update_sync(reading)
        return {"status": "ok", "state": result}

    def _snapshot(self, inp: dict) -> dict:
        pedon = self._get_pedon(inp)
        snap  = pedon.snapshot()
        if "horizon_id" in inp:
            hid = inp["horizon_id"]
            for hz in snap["horizons"]:
                if hz.get("horizon_id") == hid or hz.get("designation") == hid:
                    return {"status": "ok", "snapshot": hz}
            return {"error": f"horizon_id '{hid}' not found."}
        return {"status": "ok", "snapshot": snap}

    def _query_history(self, inp: dict) -> dict:
        pedon  = self._get_pedon(inp)
        hid    = inp.get("horizon_id")
        if not hid:
            return {"error": "horizon_id is required for dp_query_history."}
        records = pedon.query_history(
            horizon_id = hid,
            since      = inp.get("since"),
            until      = inp.get("until"),
            limit      = inp.get("limit", 0),
        )
        return {"status": "ok", "horizon_id": hid,
                "count": len(records), "records": records}

    def _register_sensor_manifest(self, inp: dict) -> dict:
        pedon_id = inp.get("pedon_id", "default")
        manifest = SensorManifest.from_dict(inp)
        self._manifests[pedon_id] = manifest
        self._layers[pedon_id]    = SensorLayer(manifest)
        return {
            "status":    "ok",
            "pedon_id":  pedon_id,
            "fields":    len(manifest.fields),
            "message":   f"Manifest registered with {len(manifest.fields)} field(s).",
            "summary":   manifest.summary(),
        }

    def _ingest_raw(self, inp: dict) -> dict:
        pedon_id = inp.get("pedon_id", "default")
        layer    = self._layers.get(pedon_id)
        if layer is None:
            return {"error": f"No sensor manifest registered for pedon_id '{pedon_id}'. "
                             f"Call dp_register_sensor_manifest first."}
        raw  = inp.get("raw", {})
        ts   = inp.get("timestamp")
        split = inp.get("split_by_horizon", False)

        if split:
            readings = layer.split_by_horizon(raw, timestamp=ts)
        else:
            readings = [layer.ingest(raw, timestamp=ts)]

        # Optionally run dp_update immediately if a pedon is registered
        results = []
        if pedon_id in self._pedons:
            for reading in readings:
                state = self._pedons[pedon_id].update_sync(reading)
                results.append({"reading": reading, "state": state})
        else:
            results = [{"reading": r} for r in readings]

        return {"status": "ok", "count": len(results), "results": results}

    def _register_solver(self, inp: dict) -> dict:
        name    = inp.get("name")
        code    = inp.get("python_code", "")
        inputs  = inp.get("inputs", [])
        outputs = inp.get("outputs", [])
        desc    = inp.get("description", "")

        # Execute the code in a restricted namespace to extract 'fn'
        ns: dict = {}
        try:
            exec(code, ns)  # noqa: S102
        except SyntaxError as e:
            return {"error": f"Syntax error in solver code: {e}"}

        fn = ns.get("fn")
        if fn is None or not callable(fn):
            return {"error": "Solver code must define a callable named 'fn'."}

        register_method({
            "name":        name,
            "fn":          fn,
            "description": desc,
            "inputs":      inputs,
            "outputs":     outputs,
        })
        return {"status": "ok", "name": name,
                "message": f"Solver '{name}' registered. "
                           f"It will run on every subsequent dp_update call."}

    def _list_canonical_properties(self, inp: dict) -> dict:
        origin_filter = inp.get("filter_origin", "all")
        props = []
        for key, meta in CANONICAL_PROPERTIES.items():
            origins = meta.get("data_origin", [])
            derived = meta.get("dp:derived", False)
            if origin_filter == "all":
                pass
            elif origin_filter == "derived" and not derived:
                continue
            elif origin_filter != "derived" and origin_filter not in origins:
                continue
            props.append({
                "canonical_key": key,
                "si_unit":       meta.get("si_unit", ""),
                "aliases":       meta.get("aliases", []),
                "valid_range":   meta.get("valid_range"),
                "data_origin":   meta.get("data_origin", []),
                "description":   meta.get("description", ""),
            })
        return {"status": "ok", "count": len(props), "properties": props}

    def _export_jsonld(self, inp: dict) -> dict:
        pedon = self._get_pedon(inp)
        include_history = inp.get("include_history", False)
        snap  = pedon.snapshot()

        ctx = {
            "@vocab":  "https://schema.org/",
            "dp":      "https://github.com/digitalpedon/framework/ontology#",
            "glosis":  "http://w3id.org/glosis/model/codelists#",
            "xsd":     "http://www.w3.org/2001/XMLSchema#",
        }
        # Annotate known keys with URIs
        for key, meta in CANONICAL_PROPERTIES.items():
            uri = meta.get("glosis_uri")
            if uri:
                ctx[key] = {"@id": uri, "@type": "xsd:decimal",
                            "dp:unit": meta.get("si_unit", "")}

        doc = {
            "@context":  ctx,
            "@type":     "dp:DigitalPedon",
            "pedon_id":  snap["pedon_id"],
            "site_name": snap["site_name"],
            "latitude":  snap["latitude"],
            "longitude": snap["longitude"],
            "timestamp": snap["timestamp"],
            "@graph":    snap["horizons"],
        }

        if include_history:
            doc["dp:history"] = {
                h["designation"]: pedon.query_history(h["horizon_id"])
                for h in pedon._horizons
            }

        return {"status": "ok", "jsonld": doc}

    def _describe(self, inp: dict) -> dict:
        pedon = self._get_pedon(inp)
        snap  = pedon.snapshot()

        descriptions = []
        for hz in snap["horizons"]:
            state = hz.get("state", {})
            theta = state.get("volumetric_water_content",
                    state.get("theta", None))
            psi   = state.get("matric_potential_cm")
            flux  = state.get("flux_down_cm_day")
            temp  = state.get("soil_temperature_c",
                    state.get("temperature_c"))

            # Water status
            if theta is None:
                water_status = "unknown (no reading yet)"
            elif psi is not None and psi < -15000:
                water_status = "below permanent wilting point — severely dry"
            elif psi is not None and psi < -3300:
                water_status = "dry — below field capacity"
            elif psi is not None and psi > -50:
                water_status = "near-saturated or saturated"
            else:
                water_status = "moist — within plant-available range"

            # Flux direction
            if flux is None:
                flux_desc = "not yet computed"
            elif flux < -0.1:
                flux_desc = f"draining downward ({flux:+.3f} cm/day)"
            elif flux > 0.1:
                flux_desc = f"moving upward — possible waterlogging ({flux:+.3f} cm/day)"
            else:
                flux_desc = f"near-zero flux — stable ({flux:+.4f} cm/day)"

            descriptions.append({
                "horizon":      hz.get("designation", hz.get("horizon_id")),
                "depth":        f"{hz.get('depth_top_cm', '?')}–{hz.get('depth_bottom_cm', '?')} cm",
                "water_status": water_status,
                "theta":        round(theta, 4) if theta is not None else None,
                "psi_cm":       round(psi, 1) if psi is not None else None,
                "flux":         flux_desc,
                "temperature":  f"{temp:.1f} °C" if temp is not None else "unknown",
            })

        # Summary sentence
        summary_parts = []
        for d in descriptions:
            summary_parts.append(
                f"Horizon {d['horizon']} ({d['depth']}): {d['water_status']}. "
                f"Flux: {d['flux']}. Temperature: {d['temperature']}."
            )

        return {
            "status":          "ok",
            "pedon_id":        snap["pedon_id"],
            "site_name":       snap["site_name"],
            "horizons":        descriptions,
            "natural_language_summary": " | ".join(summary_parts),
        }

    # ── Helpers ────────────────────────────────────────────────────────────

    def _get_pedon(self, inp: dict) -> DigitalPedon:
        pid = inp.get("pedon_id", "default")
        if pid not in self._pedons:
            raise KeyError(
                f"No active pedon with id '{pid}'. "
                f"Call dp_build_pedon first. "
                f"Active pedons: {list(self._pedons.keys())}"
            )
        return self._pedons[pid]

    @staticmethod
    def _clean(obj: Any) -> Any:
        """Recursively remove NaN/Inf from dicts for JSON safety."""
        if isinstance(obj, dict):
            return {k: PedonDispatcher._clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [PedonDispatcher._clean(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj


# ══════════════════════════════════════════════════════════════════════════════
# QUICK DEMO — run this file directly to test the dispatcher
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import json

    d = PedonDispatcher()

    # 1. Build a pedon
    r = d.run("dp_build_pedon", {
        "site_name": "Demo Site — Flanders",
        "latitude":  51.05, "longitude": 3.72,
        "horizons": [
            {"designation":"Ap", "depth_top_cm":0,  "depth_bottom_cm":26,
             "soil_type":"loamy_topsoil"},
            {"designation":"Bw", "depth_top_cm":26, "depth_bottom_cm":65,
             "soil_type":"clay_subsoil"},
        ],
    })
    print("BUILD:", json.dumps(r, indent=2))
    pid = r["pedon_id"]

    # 2. Register a sensor manifest
    r = d.run("dp_register_sensor_manifest", {
        "pedon_id": pid,
        "fields": [
            {"source_key":"VWC_Ap_pct",  "canonical_key":"volumetric_water_content",
             "unit":"%",    "horizon_id":"Ap", "depth_cm":15,
             "sampling_rate_s":3600, "sensor_type":"METER TEROS 11",
             "data_origin":"in_situ"},
            {"source_key":"Temp_Ap_F",   "canonical_key":"soil_temperature_c",
             "unit":"degF", "horizon_id":"Ap", "depth_cm":15,
             "sampling_rate_s":3600, "sensor_type":"METER TEROS 11",
             "data_origin":"in_situ"},
            {"source_key":"NDVI_S2",     "canonical_key":"ndvi",
             "unit":"dimensionless", "horizon_id":"Ap", "depth_cm":0,
             "sampling_rate_s":864000, "sensor_type":"Sentinel-2 MSI",
             "data_origin":"remote_sensing", "optional": True},
        ],
    })
    print("MANIFEST:", r["message"])

    # 3. Ingest a raw reading (with unit conversion)
    r = d.run("dp_ingest_raw", {
        "pedon_id": pid,
        "raw":      {"VWC_Ap_pct": 28.5, "Temp_Ap_F": 59.4, "NDVI_S2": 0.62},
    })
    print("INGEST + UPDATE state:", json.dumps(r["results"][0]["state"], indent=2))

    # 4. Describe
    r = d.run("dp_describe", {"pedon_id": pid})
    print("DESCRIBE:", r["natural_language_summary"])
