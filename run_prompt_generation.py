import json
import time
import requests

MODEL = "llama3.2"
OLLAMA_URL = "http://localhost:11434/api/generate"

GROUNDING_FILE = "retrieval_grounding_output.json"
TEXTUAL_FILE = "generated_definitions.json"
OUTPUT_FILE = "ontology_outputs.json"


def normalize_table_name(name: str) -> str:
    return name.strip().lower().replace(".", "_")


def build_prompt(schema_context: dict, mapping_context: dict, textual_context: dict) -> str:
    """
    Prompt for Ollama with JSON-format enforcement.
    """
    return f"""
Return STRICT JSON only (no markdown, no extra text) with exactly these keys:
- governance_doc_markdown
- delta_ontology_ttl
- new_terms

IMPORTANT: You must always include all 3 keys even if empty.
- If you have no delta ontology changes, set delta_ontology_ttl to exactly: "# No delta required"
- If you have no new terms, set new_terms to []

Requirements:
- governance_doc_markdown must include:
  1) Table purpose/context
  2) Plain-English definitions for EVERY column in schema_context
  3) Usage notes (grain, joins/keys, caveats)
  4) 2–3 example SQL queries
- delta_ontology_ttl must be MINIMAL and reuse existing terms; add only missing.

SCHEMA CONTEXT:
{json.dumps(schema_context, indent=2)}

MAPPING CONTEXT:
{json.dumps(mapping_context, indent=2)}

TEXTUAL CONTEXT:
{json.dumps(textual_context, indent=2)}
""".strip()


def call_model_json(prompt: str, retries: int = 1, sleep_s: float = 1.0) -> dict:
    """
    Calls Ollama with JSON-format enforcement.
    Ollama returns JSON in payload["response"] as a string; we json.loads it.
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.post(
                OLLAMA_URL,
                json={
                    "model": MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",  # ✅ enforce JSON
                },
                timeout=600,
            )
            r.raise_for_status()
            payload = r.json()

            response_text = (payload.get("response") or "").strip()
            if not response_text:
                raise RuntimeError("Empty response from model")

            return json.loads(response_text)

        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(sleep_s)
            else:
                raise RuntimeError(f"Model JSON call failed after retries: {last_err}") from last_err


def normalize_model_output(output: dict, table_name: str) -> dict:
    """
    Model sometimes returns slightly different key names.
    Normalize to required keys and apply safe defaults.
    """
    key_map = {
        "governance_doc_markdown": ["governance_doc_markdown", "governance_markdown", "governanceDocMarkdown"],
        "delta_ontology_ttl": ["delta_ontology_ttl", "delta_ontology", "deltaOntologyTtl", "delta_ttl", "deltaOntologyTTL"],
        "new_terms": ["new_terms", "newTerms", "added_terms", "addedTerms"],
    }

    normalized = {}
    for required_key, variants in key_map.items():
        for v in variants:
            if v in output:
                normalized[required_key] = output[v]
                break

    # Defaults
    if "new_terms" not in normalized or normalized["new_terms"] is None:
        normalized["new_terms"] = []

    if "delta_ontology_ttl" not in normalized or not str(normalized["delta_ontology_ttl"]).strip():
        normalized["delta_ontology_ttl"] = "# No delta required"

    # Hard requirement: governance doc must exist
    if "governance_doc_markdown" not in normalized or not str(normalized["governance_doc_markdown"]).strip():
        raise RuntimeError(f"Missing governance_doc_markdown in model output for table {table_name}")

    # Ensure types
    if not isinstance(normalized["new_terms"], list):
        normalized["new_terms"] = [normalized["new_terms"]]

    return normalized


def main():
    # Load inputs
    with open(GROUNDING_FILE, "r") as f:
        grounding_data = json.load(f)

    with open(TEXTUAL_FILE, "r") as f:
        textual_data = json.load(f)

    # Build lookup for textual context by table name
    text_lookup = {}
    for row in textual_data:
        t = row.get("table") or row.get("technical_table") or row.get("table_name")
        if t:
            text_lookup[normalize_table_name(t)] = row

    results = []

    for item in grounding_data:
        table_name = item.get("technical_table")
        if not table_name:
            continue

        print(f"Processing: {table_name}")

        schema_context = {
            "table_name": table_name,
            "columns": [
                {
                    "name": c.get("technical_column"),
                    "type": c.get("type", "UNKNOWN"),
                    "is_key": c.get("is_key", False),
                }
                for c in item.get("columns", [])
            ],
            "primary_key": item.get("primary_key", []),
            "foreign_keys": item.get("foreign_keys", []),
        }

        textual_context = text_lookup.get(normalize_table_name(table_name), {})

        prompt = build_prompt(schema_context, item, textual_context)

        raw_output = call_model_json(prompt, retries=1)

        output = normalize_model_output(raw_output, table_name)

        results.append({"table": table_name, "output": output})

    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nDONE -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()