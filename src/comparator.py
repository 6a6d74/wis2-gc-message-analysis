import copy
import difflib
import json


def _normalize(msg: dict) -> dict:
    m = copy.deepcopy(msg)
    m.pop("id", None)
    m.get("properties", {}).pop("global-cache", None)
    for link in m.get("links", []):
        if link.get("rel") in ("canonical", "update"):
            link.pop("href", None)
    if "links" in m:
        m["links"] = sorted(m["links"], key=lambda lnk: lnk.get("rel", ""))
    return m


def compare(origin: dict, cache: dict) -> tuple[bool, str]:
    """Return (only_allowed_differences, diff_text).

    Strips the three categories of permitted differences before comparing:
      - top-level 'id' (always a unique UUID per message)
      - 'properties.global-cache' (added only by Global Cache)
      - 'href' of the canonical/update link (points to different host)
    """
    origin_lines = json.dumps(_normalize(origin), sort_keys=True, indent=2).splitlines(keepends=True)
    cache_lines = json.dumps(_normalize(cache), sort_keys=True, indent=2).splitlines(keepends=True)

    diff = list(difflib.unified_diff(origin_lines, cache_lines, fromfile="origin", tofile="cache"))
    if not diff:
        return True, ""
    return False, "".join(diff)
