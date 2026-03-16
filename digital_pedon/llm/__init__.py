"""LLM integration — agent dispatcher, tool schema, context manifest."""
import json, pathlib

_HERE = pathlib.Path(__file__).parent

def load_tools() -> list:
    """Return the OpenAI-compatible tools array from dp_tools.json."""
    return json.loads((_HERE / "dp_tools.json").read_text())["tools"]

def load_context() -> dict:
    """Return the full JSON-LD context manifest from dp_llm_context.json."""
    return json.loads((_HERE / "dp_llm_context.json").read_text())

from digital_pedon.llm.dp_agent import PedonDispatcher
__all__ = ["PedonDispatcher","load_tools","load_context"]
