import json
import re
from typing import Any, Optional


def _extract_fenced_block(text: str) -> Optional[str]:
    """Return content of first fenced code block, preferring ```json fences.

    Supports ```json ... ``` or ``` ... ```; returns inner text without fences.
    """
    if not isinstance(text, str) or not text:
        return None
    # Prefer ```json blocks
    m = re.search(r"```\s*json\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Fallback: any fenced block
    m = re.search(r"```\s*([\s\S]*?)```", text)
    if m:
        return m.group(1).strip()
    return None


def _extract_first_balanced_segment(text: str, opener: str, closer: str) -> Optional[str]:
    """Extract the first balanced JSON-like segment starting with opener and ending at matching closer.

    Minimal stack-based scanner sufficient for arrays/objects comprised of strings/numbers.
    """
    if not isinstance(text, str) or not text:
        return None
    start = text.find(opener)
    if start == -1:
        return None
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        else:
            if ch == '"':
                in_string = True
                continue
            if ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    return text[start : i + 1].strip()
    return None


def parse_llm_json(text: str) -> Any:
    """Parse JSON from an LLM response robustly.

    Strategy:
    1) Try strict json.loads on the full text.
    2) If fails, extract first ```json fenced block, or any fenced block, then json.loads.
    3) If fails, extract the first balanced array `[...]` segment, else the first object `{...}` segment, then json.loads.
    Raises ValueError if parsing fails.
    """
    # 1) Strict
    try:
        return json.loads(text)
    except Exception:
        pass

    # 2) Fenced block
    block = _extract_fenced_block(text)
    if block:
        try:
            return json.loads(block)
        except Exception:
            pass

    # 3) First array, then object
    arr = _extract_first_balanced_segment(text, "[", "]")
    if arr:
        try:
            return json.loads(arr)
        except Exception:
            pass
    obj = _extract_first_balanced_segment(text, "{", "}")
    if obj:
        try:
            return json.loads(obj)
        except Exception:
            pass

    raise ValueError("unable_to_parse_llm_json")



