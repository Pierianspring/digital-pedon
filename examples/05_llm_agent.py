"""
Example 05 — LLM Agent Integration
=====================================
Demonstrates how to connect an LLM (Claude) to Digital Pedon as tool calls.
The LLM receives the DP schema as system context and can call any DP method
as a native tool.

Requires:  pip install anthropic

Run:  python examples/05_llm_agent.py

If you don't have an Anthropic API key, the script also shows the standalone
dispatcher usage without the LLM.
"""

import json
import os

from digital_pedon.llm import PedonDispatcher, load_tools, load_context

# ── Standalone dispatcher (no LLM needed) ────────────────────────────────────
print("=" * 60)
print("Standalone Dispatcher Demo")
print("=" * 60)

dispatcher = PedonDispatcher()

# Build a pedon
r = dispatcher.run("dp_build_pedon", {
    "site_name": "LLM Demo Site",
    "latitude":   51.505,
    "longitude":   4.469,
    "horizons": [
        {"designation":"Ap","depth_top_cm":0, "depth_bottom_cm":28,
         "soil_type":"loamy_topsoil"},
        {"designation":"Btg","depth_top_cm":28,"depth_bottom_cm":75,
         "soil_type":"clay_subsoil",
         "theta_saturation":0.46},   # waterlogging-prone
    ],
})
pid = r["pedon_id"]
print(f"\nBuilt pedon: {r['message']}")

# Register sensor manifest
dispatcher.run("dp_register_sensor_manifest", {
    "pedon_id": pid,
    "fields": [
        {"source_key":"VWC_pct", "canonical_key":"volumetric_water_content",
         "unit":"%", "horizon_id":"Ap", "depth_cm":15,
         "sampling_rate_s":3600, "sensor_type":"TEROS 11", "data_origin":"in_situ"},
        {"source_key":"Temp_C",  "canonical_key":"soil_temperature_c",
         "unit":"degC", "horizon_id":"Ap", "depth_cm":15,
         "sampling_rate_s":3600, "sensor_type":"TEROS 11", "data_origin":"in_situ"},
    ],
})

# Ingest raw reading through manifest
r = dispatcher.run("dp_ingest_raw", {
    "pedon_id": pid,
    "raw": {"VWC_pct": 42.8, "Temp_C": 13.1},  # very wet reading
})
print(f"\nIngested reading → state:")
state = r["results"][0]["state"]
print(f"  θ = {state.get('volumetric_water_content', state.get('theta','?')):.3f}")
print(f"  ψ = {state.get('matric_potential_cm','?')} cm H₂O")

# Manually update Btg
dispatcher.run("dp_update", {
    "pedon_id": pid,
    "horizon_id": "Btg",
    "volumetric_water_content": 0.445,  # near saturation
    "soil_temperature_c": 12.4,
})

# Natural language description
r = dispatcher.run("dp_describe", {"pedon_id": pid})
print(f"\nDP Description:")
print(f"  {r['natural_language_summary']}")

# List canonical properties
r = dispatcher.run("dp_list_canonical_properties", {"filter_origin": "remote_sensing"})
print(f"\nRemote sensing properties ({r['count']}):")
for p in r["properties"]:
    print(f"  {p['canonical_key']:40s} [{p['si_unit']}]")

# Export JSON-LD
r = dispatcher.run("dp_export_jsonld", {"pedon_id": pid})
print(f"\nJSON-LD export: {len(json.dumps(r['jsonld']))} chars, "
      f"{len(r['jsonld']['@context'])} annotated properties")


# ── LLM Agent (requires ANTHROPIC_API_KEY) ────────────────────────────────────
print("\n" + "=" * 60)
print("LLM Agent Demo (requires ANTHROPIC_API_KEY)")
print("=" * 60)

api_key = os.environ.get("ANTHROPIC_API_KEY")
if not api_key:
    print("\nANTHROPIC_API_KEY not set. Set it to run the LLM demo.")
    print("The tools schema and context are ready to use:")
    tools   = load_tools()
    context = load_context()
    print(f"  Tools: {len(tools)} tool definitions")
    print(f"  Context: {len(json.dumps(context))} chars JSON-LD")
    print("\nTo use with Claude:")
    print("""
  import anthropic, json
  from digital_pedon.llm import PedonDispatcher, load_tools, load_context

  dispatcher = PedonDispatcher()
  client     = anthropic.Anthropic()
  tools      = load_tools()
  context    = load_context()

  messages = [{"role":"user","content":"Build a pedon for my clay field and tell me the current water status after a reading of 38% VWC."}]

  while True:
      resp = client.messages.create(
          model="claude-sonnet-4-6", max_tokens=2048,
          system=json.dumps(context), tools=tools, messages=messages,
      )
      if resp.stop_reason == "end_turn":
          print(resp.content[0].text)
          break
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
""")
else:
    try:
        import anthropic
    except ImportError:
        print("Install anthropic: pip install anthropic")
        raise SystemExit(1)

    dispatcher2 = PedonDispatcher()
    tools       = load_tools()
    context     = load_context()
    client      = anthropic.Anthropic(api_key=api_key)

    messages = [{
        "role":    "user",
        "content": ("Build a two-horizon pedon (Ap 0–28 cm loamy topsoil, "
                    "Btg 28–75 cm clay subsoil) for Ghent, Belgium. "
                    "Then process a sensor reading: Ap horizon θ=0.42 cm³/cm³, T=13°C. "
                    "Tell me the matric potential, flux direction, and whether there is a "
                    "waterlogging risk."),
    }]

    print("\nSending to Claude...\n")
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=json.dumps(context),
            tools=tools,
            messages=messages,
        )
        if resp.stop_reason == "end_turn":
            for block in resp.content:
                if hasattr(block, "text"):
                    print(block.text)
            break
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                print(f"  [tool] {block.name}({list(block.input.keys())})")
                result = dispatcher2.run(block.name, block.input)
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     json.dumps(result),
                })
        messages.append({"role": "assistant", "content": resp.content})
        messages.append({"role": "user",      "content": tool_results})
