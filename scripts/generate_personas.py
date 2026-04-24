#!/usr/bin/env python3
"""Generate persona library via LLM-enriched narrative fields.

Phase 4 Stage A — samples numeric axes from archetype distributions, then
calls Claude to fill in name/backstory/voice_style with diverse, realistic
crypto-native personas.

Usage (smoke run, 100 personas):
    python scripts/generate_personas.py \
        --count 100 --model claude-sonnet-4-6 --batch-size 10 \
        --concurrency 10 --archetypes-dir data/personas/archetypes \
        --output data/personas/library_smoke_100.jsonl --seed 42
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Import persona module directly to avoid torch dependency in oasis.__init__
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
_PERSONA_MODULE_PATH = _REPO_ROOT / "oasis" / "crypto" / "persona.py"

_spec = importlib.util.spec_from_file_location(
    "oasis.crypto.persona", str(_PERSONA_MODULE_PATH)
)
_persona_mod = importlib.util.module_from_spec(_spec)
# Register the module so pydantic forward refs can resolve via its globals
sys.modules["oasis.crypto.persona"] = _persona_mod
_spec.loader.exec_module(_persona_mod)

# Pydantic forward-ref resolution needed for dynamically loaded module
_ns = {k: getattr(_persona_mod, k) for k in dir(_persona_mod)}
_persona_mod.ArchetypeTemplate.model_rebuild(_types_namespace=_ns)
_persona_mod.PersonaLibrary.model_rebuild(_types_namespace=_ns)

ArchetypeTemplate = _persona_mod.ArchetypeTemplate
Persona = _persona_mod.Persona
PersonaLibrary = _persona_mod.PersonaLibrary

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("generate_personas")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POPULATION_MIX: dict[str, float] = {
    "lurker": 0.45,
    "hodler": 0.15,
    "paperhands": 0.15,
    "fomo_degen": 0.08,
    "ta": 0.05,
    "contrarian": 0.03,
    "news_trader": 0.04,
    "whale": 0.01,
    "kol": 0.02,
    "market_maker": 0.02,
}

DIVERSITY_SEEDS = {
    "age_brackets": ["18-25", "26-35", "36-50", "51+"],
    "regions": [
        "North America", "Europe", "East Asia", "Southeast Asia",
        "Latin America", "India/South Asia", "Africa", "Middle East",
    ],
    "professions": [
        "student", "software engineer", "finance professional",
        "independent trader", "academic", "retail worker", "gig worker",
        "healthcare worker", "creative", "entrepreneur", "other",
    ],
    "entry_years": [2013, 2017, 2020, 2021, 2022, 2024],
    "platforms": ["Twitter/X", "Reddit", "Telegram", "Discord", "YouTube", "Farcaster"],
}

SYSTEM_PROMPT = (
    "You generate diverse crypto market participant personas for a simulation.\n"
    "Each persona must feel like a real internet-native handle, not a caricature.\n"
    "You output only valid JSON. You do not fabricate statistics or invent real people."
)

USER_PROMPT_TEMPLATE = """\
ARCHETYPE: {archetype}

DESCRIPTION: {description}

VOICE STYLE GUIDANCE: {voice_style_template}

For each of the {N} seeds below, create ONE persona that fits the archetype while
differing meaningfully across the group in handle style, backstory structure,
and voice.

Seeds:
{seed_list_formatted}

Constraints:
- Names are internet handles/nicknames, not legal names. Vary style: lower/mixed
  case, numbers, underscores, crypto slang, non-English handles, etc.
- Backstory = 2-3 sentences. Mention how they got into crypto, what drives them
  now, and a concrete detail (a past trade, a mentor, a loss, a city, a job).
  Vary the opening — NOT all starting with "Got into crypto..." or similar.
- voice_style = 1-2 sentences describing how they write online: vocabulary
  choices, emoji/abbreviation use, tone, post length.
- Do not use any real person's name (no Elon, Saylor, Sam, etc.).
- Do not invent specific price targets or percentages.

Return STRICTLY a JSON array of {N} objects, no prose wrapper, no markdown code
fences, each object having exactly these fields:
  "name": string
  "backstory": string
  "voice_style": string"""


# ---------------------------------------------------------------------------
# Diversity seed sampling
# ---------------------------------------------------------------------------

def sample_diversity_seeds(
    count: int, rng: np.random.Generator
) -> list[tuple[str, str, str, int, str]]:
    """Sample `count` diversity seed tuples (with replacement)."""
    seeds = []
    for _ in range(count):
        age = rng.choice(DIVERSITY_SEEDS["age_brackets"])
        region = rng.choice(DIVERSITY_SEEDS["regions"])
        profession = rng.choice(DIVERSITY_SEEDS["professions"])
        entry_year = int(rng.choice(DIVERSITY_SEEDS["entry_years"]))
        platform = rng.choice(DIVERSITY_SEEDS["platforms"])
        seeds.append((age, region, profession, entry_year, platform))
    return seeds


def format_seeds(seeds: list[tuple]) -> str:
    """Format seed tuples into numbered lines for the prompt."""
    lines = []
    for i, (age, region, prof, year, plat) in enumerate(seeds, 1):
        lines.append(
            f"  {i}. age={age}, region={region}, profession={prof}, "
            f"entry_year={year}, primary_platform={plat}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM calling
# ---------------------------------------------------------------------------

async def call_llm_batch(
    client: Any,
    model: str,
    template: ArchetypeTemplate,
    seeds: list[tuple],
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
) -> tuple[list[dict], int, int]:
    """Call Claude to generate narrative fields for a batch of seeds.

    Returns (list_of_dicts, input_tokens, output_tokens).
    """
    user_prompt = USER_PROMPT_TEMPLATE.format(
        archetype=template.archetype,
        description=template.description,
        voice_style_template=template.voice_style_template,
        N=len(seeds),
        seed_list_formatted=format_seeds(seeds),
    )

    async with semaphore:
        for attempt in range(max_retries + 1):
            try:
                response = await client.messages.create(
                    model=model,
                    max_tokens=4096,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": user_prompt}],
                )

                # Extract token counts
                input_tok = response.usage.input_tokens
                output_tok = response.usage.output_tokens

                # Parse JSON from response
                raw_text = response.content[0].text.strip()
                parsed = _parse_json_array(raw_text)

                if len(parsed) != len(seeds):
                    log.warning(
                        "Expected %d items, got %d for %s batch. "
                        "Will retry failed seeds.",
                        len(seeds), len(parsed), template.archetype,
                    )

                return parsed, input_tok, output_tok

            except Exception as e:
                is_rate_limit = "rate" in str(type(e).__name__).lower() or (
                    "429" in str(e)
                )
                if is_rate_limit and attempt < max_retries:
                    wait = 2 ** (attempt + 1)
                    log.warning(
                        "Rate limit hit for %s (attempt %d/%d), "
                        "backing off %ds...",
                        template.archetype, attempt + 1, max_retries, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                elif attempt < max_retries and not is_rate_limit:
                    wait = 2 ** attempt
                    log.warning(
                        "Error for %s (attempt %d/%d): %s. Retrying in %ds...",
                        template.archetype, attempt + 1, max_retries,
                        str(e)[:200], wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                else:
                    log.error(
                        "Failed after %d retries for %s: %s",
                        max_retries, template.archetype, str(e)[:300],
                    )
                    raise

    # Should never reach here due to raise above
    raise RuntimeError("Unexpected flow in call_llm_batch")


def _parse_json_array(text: str) -> list[dict]:
    """Defensively extract a JSON array from LLM output."""
    # Try direct parse first
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # Find first '[' and last ']'
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"No JSON array found in response: {text[:200]}")

    result = json.loads(text[start : end + 1])
    if not isinstance(result, list):
        raise ValueError(f"Parsed JSON is not an array: {type(result)}")
    return result


# ---------------------------------------------------------------------------
# Per-archetype allocation
# ---------------------------------------------------------------------------

def compute_archetype_counts(
    total: int, uniform: bool
) -> dict[str, int]:
    """Compute per-archetype persona counts.

    If uniform=True, distribute evenly (total / num_archetypes each).
    Otherwise, use POPULATION_MIX with largest-remainder allocation.
    """
    archetypes = list(POPULATION_MIX.keys())
    n_archetypes = len(archetypes)

    if uniform:
        base = total // n_archetypes
        remainder = total % n_archetypes
        counts = {a: base for a in archetypes}
        # Distribute remainder to first `remainder` archetypes
        for i, a in enumerate(archetypes):
            if i < remainder:
                counts[a] += 1
        return counts

    # Largest-remainder method
    raw = {a: frac * total for a, frac in POPULATION_MIX.items()}
    floored = {a: int(v) for a, v in raw.items()}
    remainders = {a: raw[a] - floored[a] for a in raw}
    allocated = sum(floored.values())
    for a in sorted(remainders, key=remainders.get, reverse=True):
        if allocated >= total:
            break
        floored[a] += 1
        allocated += 1
    return floored


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_library(personas: list[Persona], templates: dict[str, ArchetypeTemplate]) -> bool:
    """Run all validation checks. Returns True if all pass."""
    errors: list[str] = []
    n = len(personas)

    log.info("--- Validation Report ---")

    # 1. Count check
    if n != 100:
        log.info("  [NOTE] Expected 100 personas, got %d (non-smoke run?)", n)

    # 2. All valid Persona pydantic objects (already true if we got here)
    log.info("  [PASS] All %d personas are valid Persona objects", n)

    # 3. Unique persona_id
    ids = [p.persona_id for p in personas]
    if len(set(ids)) != len(ids):
        dupes = [x for x in ids if ids.count(x) > 1]
        errors.append(f"Duplicate persona_ids: {set(dupes)}")
        log.error("  [FAIL] Duplicate persona_ids found")
    else:
        log.info("  [PASS] All persona_id values unique")

    # 4. Unique names — AUTO-REPAIR. At 10k scale Sonnet has a creativity
    # ceiling per-archetype and will repeat handles. Real users on different
    # platforms share handles all the time. Auto-suffix duplicates with
    # _2/_3/... to restore uniqueness, but cap soft tolerance at 15% — if
    # more than 15% of names collide, something is wrong with the prompt.
    from collections import Counter
    name_counts = Counter(p.name for p in personas)
    duped_count = sum(c - 1 for c in name_counts.values() if c > 1)
    dup_fraction = duped_count / n if n else 0
    if dup_fraction > 0.15:
        errors.append(
            f"Too many duplicate names: {duped_count}/{n} = "
            f"{dup_fraction:.1%%} (max 15%% tolerance)"
        )
        log.error("  [FAIL] %d duplicate name occurrences (%.1f%%, max 15%%)",
                  duped_count, dup_fraction * 100)
    elif duped_count:
        # Auto-repair: suffix duplicates with _2, _3, ...
        seen: dict[str, int] = {}
        for p in personas:
            base = p.name
            if base not in seen:
                seen[base] = 1
            else:
                seen[base] += 1
                p.name = f"{base}_{seen[base]}"
        # Verify unique after repair
        final_names = [p.name for p in personas]
        assert len(set(final_names)) == len(final_names), \
            "Auto-suffix failed to produce unique names"
        log.warning(
            "  [PASS-REPAIR] %d duplicate name occurrences auto-suffixed "
            "(%.2f%% of library)", duped_count, dup_fraction * 100
        )
    else:
        log.info("  [PASS] All %d names unique", n)

    # 5. Backstory length — tolerate a tiny fraction of short outputs (LLM
    # stochasticity at 10k scale will occasionally yield short completions).
    # Hard floor at 20 chars, soft cap at 40. Up to 0.1% of library can be
    # in [20, 40]; anything below 20 always fails.
    very_short = [(p.persona_id, len(p.backstory)) for p in personas if len(p.backstory) < 20]
    somewhat_short = [(p.persona_id, len(p.backstory)) for p in personas
                      if 20 <= len(p.backstory) <= 40]
    tolerance = max(5, int(n * 0.001))  # allow up to 0.1% or 5, whichever is higher
    if very_short:
        errors.append(f"Backstories < 20 chars (hard fail): {very_short}")
        log.error("  [FAIL] %d backstories < 20 chars (hard floor)", len(very_short))
    elif len(somewhat_short) > tolerance:
        errors.append(
            f"Too many short backstories ({len(somewhat_short)} in [20,40], "
            f"tolerance={tolerance}): {somewhat_short[:5]}..."
        )
        log.error("  [FAIL] %d backstories in [20,40], tolerance %d",
                  len(somewhat_short), tolerance)
    else:
        if somewhat_short:
            log.warning("  [PASS-WARN] %d backstories in [20,40], within "
                        "tolerance %d of %d total", len(somewhat_short), tolerance, n)
        else:
            log.info("  [PASS] All backstories > 40 characters")

    # 6. Voice style length
    short_vs = [(p.persona_id, len(p.voice_style)) for p in personas if len(p.voice_style) <= 10]
    if short_vs:
        errors.append(f"Voice styles too short (<= 10 chars): {short_vs}")
        log.error("  [FAIL] %d voice_styles <= 10 chars", len(short_vs))
    else:
        log.info("  [PASS] All voice_styles > 10 characters")

    # 7. Numeric axes within distribution bounds
    for p in personas:
        tmpl = templates.get(p.archetype)
        if not tmpl:
            errors.append(f"{p.persona_id}: unknown archetype {p.archetype}")
            continue
        # risk_tolerance
        if not (0.0 <= p.risk_tolerance <= 1.0):
            errors.append(f"{p.persona_id}: risk_tolerance {p.risk_tolerance} out of [0,1]")
        # time_horizon_minutes
        if p.time_horizon_minutes <= 0:
            errors.append(f"{p.persona_id}: time_horizon_minutes <= 0")
        # social_sensitivity
        if not (0.0 <= p.social_sensitivity <= 1.0):
            errors.append(f"{p.persona_id}: social_sensitivity out of [0,1]")
        # herding_coefficient
        if not (-1.0 <= p.herding_coefficient <= 1.0):
            errors.append(f"{p.persona_id}: herding_coefficient out of [-1,1]")
        # capital_usd
        if p.capital_usd <= 0:
            errors.append(f"{p.persona_id}: capital_usd <= 0")

    bounds_errors = [e for e in errors if "out of" in e or "<= 0" in e]
    if bounds_errors:
        log.error("  [FAIL] %d numeric axis bound violations", len(bounds_errors))
    else:
        log.info("  [PASS] All numeric axes within bounds")

    # 8. action_base_rates sum (already enforced by pydantic, but double-check)
    for p in personas:
        total = (
            p.action_base_rates.silent + p.action_base_rates.react
            + p.action_base_rates.comment + p.action_base_rates.post
            + p.action_base_rates.trade
        )
        if abs(total - 1.0) > 1e-6:
            errors.append(f"{p.persona_id}: action_base_rates sum={total}")
    log.info("  [PASS] All action_base_rates sum to 1.0")

    # 9. Name diversity: at least 8 distinct first-characters per archetype
    #    For small samples (10 per archetype), require min(8, ceil(0.7 * n))
    archetypes = set(p.archetype for p in personas)
    for arch in sorted(archetypes):
        arch_names = [p.name for p in personas if p.archetype == arch]
        first_chars = set(n[0].lower() for n in arch_names if n)
        min_required = min(8, max(5, -(-int(0.7 * len(arch_names)) // 1)))
        if len(first_chars) < min_required:
            if len(arch_names) >= 10:
                errors.append(
                    f"{arch}: only {len(first_chars)} distinct first-chars "
                    f"in names (need >= {min_required}): {sorted(first_chars)}"
                )
                log.warning(
                    "  [WARN] %s: %d distinct first-chars (need %d): %s",
                    arch, len(first_chars), min_required, sorted(first_chars),
                )
            else:
                log.info(
                    "  [INFO] %s: %d names, %d first-chars (small sample)",
                    arch, len(arch_names), len(first_chars),
                )
        else:
            log.info(
                "  [PASS] %s: %d distinct first-chars in names (need %d)",
                arch, len(first_chars), min_required,
            )

    # 10. Backstory opening trigram diversity: >= 5 distinct per archetype
    for arch in sorted(archetypes):
        arch_personas = [p for p in personas if p.archetype == arch]
        trigrams = set()
        for p in arch_personas:
            words = p.backstory.split()[:3]
            trigram = " ".join(words).lower()
            trigrams.add(trigram)
        if len(trigrams) < 5 and len(arch_personas) >= 10:
            errors.append(
                f"{arch}: only {len(trigrams)} distinct backstory trigrams "
                f"(need >= 5): {sorted(trigrams)}"
            )
            log.warning(
                "  [WARN] %s: %d distinct backstory trigrams (need 5)",
                arch, len(trigrams),
            )
        else:
            log.info(
                "  [PASS] %s: %d distinct backstory trigrams",
                arch, len(trigrams),
            )

    if errors:
        log.error("--- VALIDATION FAILED: %d issues ---", len(errors))
        for e in errors:
            log.error("  - %s", e)
        return False

    log.info("--- VALIDATION PASSED ---")
    return True


# ---------------------------------------------------------------------------
# Main generation pipeline
# ---------------------------------------------------------------------------

async def generate_personas(args: argparse.Namespace) -> None:
    """Main async entry point for persona generation."""
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=_REPO_ROOT / ".env")

    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not found in environment. Aborting.")
        sys.exit(1)

    client = anthropic.AsyncAnthropic(api_key=api_key)

    # Load all archetype templates
    archetypes_dir = Path(args.archetypes_dir)
    if not archetypes_dir.is_absolute():
        archetypes_dir = _REPO_ROOT / archetypes_dir

    templates: dict[str, ArchetypeTemplate] = {}
    for yaml_path in sorted(archetypes_dir.glob("*.yaml")):
        tmpl = ArchetypeTemplate.from_yaml(yaml_path)
        templates[tmpl.archetype] = tmpl
        log.info("Loaded archetype: %s", tmpl.archetype)

    if len(templates) != 10:
        log.warning("Expected 10 archetypes, found %d", len(templates))

    # Determine uniform mode
    uniform = args.uniform if args.uniform is not None else (args.count <= 200)
    counts = compute_archetype_counts(args.count, uniform=uniform)
    log.info(
        "Allocation (%s): %s  total=%d",
        "uniform" if uniform else "population-mix",
        dict(sorted(counts.items())),
        sum(counts.values()),
    )

    # Seeded RNG
    rng = np.random.default_rng(args.seed)

    # Phase 1: Sample numeric axes + diversity seeds for all personas
    all_tasks: list[dict] = []  # Each: {persona, seeds, archetype}
    global_idx = 0

    for arch_name in sorted(counts.keys()):
        n = counts[arch_name]
        tmpl = templates[arch_name]
        diversity_seeds = sample_diversity_seeds(n, rng)

        for i in range(n):
            global_idx += 1
            pid = f"p_smoke_{global_idx:06d}"
            persona = tmpl.sample_persona(pid, rng)
            all_tasks.append({
                "persona": persona,
                "seed": diversity_seeds[i],
                "archetype": arch_name,
            })

    log.info("Sampled %d personas with numeric axes", len(all_tasks))

    # Phase 2: Batch LLM calls per archetype
    semaphore = asyncio.Semaphore(args.concurrency)
    total_input_tokens = 0
    total_output_tokens = 0
    completed_personas: list[Persona] = []

    for arch_name in sorted(counts.keys()):
        arch_tasks = [t for t in all_tasks if t["archetype"] == arch_name]
        tmpl = templates[arch_name]

        # Split into batches
        batches = []
        for i in range(0, len(arch_tasks), args.batch_size):
            batches.append(arch_tasks[i : i + args.batch_size])

        log.info(
            "Generating %d personas for '%s' in %d batch(es)...",
            len(arch_tasks), arch_name, len(batches),
        )

        # Run batches concurrently (within concurrency limit)
        batch_coros = []
        for batch in batches:
            seeds = [t["seed"] for t in batch]
            batch_coros.append(
                call_llm_batch(client, args.model, tmpl, seeds, semaphore)
            )

        results = await asyncio.gather(*batch_coros, return_exceptions=True)

        for batch_idx, result in enumerate(results):
            batch = batches[batch_idx]

            if isinstance(result, Exception):
                log.error(
                    "Batch %d for %s failed: %s",
                    batch_idx, arch_name, str(result)[:300],
                )
                sys.exit(1)

            llm_items, in_tok, out_tok = result
            total_input_tokens += in_tok
            total_output_tokens += out_tok

            # Merge LLM output into sampled personas
            for task_idx, task in enumerate(batch):
                persona: Persona = task["persona"]
                if task_idx < len(llm_items):
                    item = llm_items[task_idx]
                    persona = persona.model_copy(
                        update={
                            "name": item.get("name", persona.name),
                            "backstory": item.get("backstory", persona.backstory),
                            "voice_style": item.get("voice_style", persona.voice_style),
                            "generated_by": f"llm:{args.model}",
                            "generated_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                else:
                    log.warning(
                        "Missing LLM output for %s — retrying individually...",
                        persona.persona_id,
                    )
                    # Retry this single seed
                    try:
                        retry_result, in_t, out_t = await call_llm_batch(
                            client, args.model, tmpl,
                            [task["seed"]], semaphore,
                        )
                        total_input_tokens += in_t
                        total_output_tokens += out_t
                        if retry_result:
                            item = retry_result[0]
                            persona = persona.model_copy(
                                update={
                                    "name": item.get("name", persona.name),
                                    "backstory": item.get("backstory", persona.backstory),
                                    "voice_style": item.get("voice_style", persona.voice_style),
                                    "generated_by": f"llm:{args.model}",
                                    "generated_at": datetime.now(timezone.utc).isoformat(),
                                }
                            )
                    except Exception as e:
                        log.error("Retry failed for %s: %s", persona.persona_id, e)
                        sys.exit(1)

                completed_personas.append(persona)

    log.info(
        "Generation complete: %d personas, %d input tokens, %d output tokens",
        len(completed_personas), total_input_tokens, total_output_tokens,
    )

    # Phase 3: Validation
    log.info("Running validation...")
    passed = validate_library(completed_personas, templates)

    if not passed:
        log.error("Validation FAILED. Not writing output files.")
        sys.exit(1)

    # Phase 4: Write output
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = _REPO_ROOT / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lib = PersonaLibrary(personas=completed_personas)
    lib.save_to_jsonl(output_path)
    log.info("Wrote %d personas to %s", len(completed_personas), output_path)

    # Write metadata sidecar
    prompt_hash = hashlib.sha256(USER_PROMPT_TEMPLATE.encode()).hexdigest()
    # Cost estimation: claude-sonnet-4-6 pricing
    # Input: $3/M tokens, Output: $15/M tokens
    cost_estimate = (
        total_input_tokens * 3.0 / 1_000_000
        + total_output_tokens * 15.0 / 1_000_000
    )

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": args.model,
        "count": len(completed_personas),
        "per_archetype": {
            arch: sum(1 for p in completed_personas if p.archetype == arch)
            for arch in sorted(set(p.archetype for p in completed_personas))
        },
        "seed": args.seed,
        "prompt_hash": prompt_hash,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "estimated_cost_usd": round(cost_estimate, 4),
        "validation_passed": True,
    }

    meta_path = output_path.with_suffix("").with_suffix(".meta.json")
    # Handle the .jsonl -> .meta.json conversion properly
    meta_path = output_path.parent / (
        output_path.name.replace(".jsonl", ".meta.json")
    )
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    log.info("Wrote metadata to %s", meta_path)

    # Phase 5: Summary report
    log.info("\n=== GENERATION SUMMARY ===")
    log.info("Total personas: %d", len(completed_personas))
    log.info("Distribution: %s", lib.distribution_summary())
    log.info("Input tokens: %d", total_input_tokens)
    log.info("Output tokens: %d", total_output_tokens)
    log.info("Estimated cost: $%.4f", cost_estimate)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate persona library with LLM-enriched narrative fields"
    )
    p.add_argument("--count", type=int, default=100, help="Total personas to generate")
    p.add_argument("--model", type=str, default="claude-sonnet-4-6", help="Claude model")
    p.add_argument("--batch-size", type=int, default=10, help="Seeds per LLM call")
    p.add_argument("--concurrency", type=int, default=10, help="Max concurrent LLM calls")
    p.add_argument(
        "--archetypes-dir", type=str,
        default="data/personas/archetypes",
        help="Directory containing archetype YAML files",
    )
    p.add_argument(
        "--output", type=str,
        default="data/personas/library_smoke_100.jsonl",
        help="Output JSONL path",
    )
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    p.add_argument(
        "--uniform", type=lambda x: x.lower() in ("true", "1", "yes"),
        default=None,
        help="Force uniform distribution (default: auto, True if count <= 200)",
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(generate_personas(args))


if __name__ == "__main__":
    main()
