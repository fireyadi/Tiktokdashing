import argparse
import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional

from TikTokApi import TikTokApi

MS_TOKEN = os.environ.get("ms_token")
AU_PROXY = os.environ.get("AU_PROXY")  # optional

DEFAULT_INPUT = "seed_sounds.txt"
DEFAULT_OUTPUT = "sound_hydration.json"
SOUND_INFO_BATCH_SLEEP = 0.5


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if cleaned.isdigit():
            return int(cleaned)
    return None


def extract_sound_video_count(sound_info: Dict[str, Any]) -> Optional[int]:
    for path in [
        ("stats", "videoCount"),
        ("stats", "videoCountV2"),
        ("stats", "videoCountStr"),
        ("music", "stats", "videoCount"),
        ("music", "videoCount"),
        ("videoCount",),
    ]:
        cur: Any = sound_info
        for key in path:
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(key)
        count = _coerce_int(cur)
        if count is not None:
            return count
    return None


def read_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out


def _from_json_payload(payload: Any) -> List[str]:
    if isinstance(payload, list):
        return [str(x).strip() for x in payload if str(x).strip()]
    if isinstance(payload, dict):
        for key in ["sounds", "sound_ids", "soundIds", "ids"]:
            if key in payload and isinstance(payload[key], list):
                return [str(x).strip() for x in payload[key] if str(x).strip()]
    return []


def read_sound_ids(path: str) -> List[str]:
    if path.lower().endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        ids = _from_json_payload(payload)
    else:
        ids = read_lines(path)

    seen = set()
    deduped = []
    for sid in ids:
        if sid and sid not in seen:
            seen.add(sid)
            deduped.append(sid)
    return deduped


def build_sound_record(sound_id: str, info: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": sound_id,
        "title": info.get("title") or info.get("music", {}).get("title"),
        "authorName": info.get("authorName") or info.get("music", {}).get("authorName"),
        "original": info.get("original") if "original" in info else info.get("music", {}).get("original"),
        "video_count": extract_sound_video_count(info),
    }


async def hydrate_sounds(sound_ids: List[str], batch_sleep: float) -> Dict[str, Any]:
    context_options = {"locale": "en-AU", "timezone_id": "Australia/Sydney"}
    proxies = [{"server": AU_PROXY}] if AU_PROXY else None

    hydrated: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[MS_TOKEN] if MS_TOKEN else None,
            num_sessions=1,
            sleep_after=3,
            context_options=context_options,
            proxies=proxies,
            headless=True,
        )

        for sid in sound_ids:
            try:
                info = await api.sound(id=sid).info()
                if isinstance(info, dict):
                    hydrated.append(build_sound_record(sid, info))
                else:
                    errors.append({"id": sid, "error": "no_info"})
            except Exception as exc:
                errors.append({"id": sid, "error": str(exc)})
            await asyncio.sleep(batch_sleep)

    return {"items": hydrated, "errors": errors}


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hydrate TikTok sound IDs with video counts via TikTokApi."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Path to sound IDs (txt or json).",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output JSON file.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=SOUND_INFO_BATCH_SLEEP,
        help="Seconds to sleep between sound info requests.",
    )
    args = parser.parse_args()

    sound_ids = read_sound_ids(args.input)
    if not sound_ids:
        raise SystemExit(f"No sound IDs found in {args.input}")

    started = time.time()
    result = await hydrate_sounds(sound_ids, args.sleep)
    finished = time.time()

    output = {
        "meta": {
            "input": args.input,
            "output": args.output,
            "sound_count": len(sound_ids),
            "hydrated": len(result["items"]),
            "errors": len(result["errors"]),
            "elapsed_seconds": round(finished - started, 3),
            "ms_token_present": bool(MS_TOKEN),
            "proxy_present": bool(AU_PROXY),
        },
        "items": result["items"],
        "errors": result["errors"],
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Hydrated {len(result['items'])} sounds -> {args.output}")
    if result["errors"]:
        print(f"Errors: {len(result['errors'])}")


if __name__ == "__main__":
    asyncio.run(main())
