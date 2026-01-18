import asyncio
import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from TikTokApi import TikTokApi

MS_TOKEN = os.environ.get("ms_token")
AU_PROXY = os.environ.get("AU_PROXY")  # optional

# -----------------------------
# Output size control
# -----------------------------
INCLUDE_RAW = False  # full raw payload OFF
INCLUDE_RAW_THUMBS = True  # keep ONLY raw.video.cover + raw.author.avatarThumb

# -----------------------------
# Targets
# -----------------------------
TRENDING_TARGET = 300

MAX_ACCOUNTS_TO_CHECK = 200
PER_ACCOUNT_LIMIT = 2

MAX_HASHTAGS_TO_CHECK = 240
PER_HASHTAG_LIMIT = 2

MAX_SOUNDS_TO_CHECK = 240
PER_SOUND_LIMIT = 3

# Auto-seeding rules
ADD_TOP_HASHTAGS = 100
ADD_TOP_CREATORS = 100
ADD_TOP_SUGGEST_WORDS = 30
ADD_TOP_SOUNDS = 100

MIN_HASHTAG_LEN = 3

# Throttling
TRENDING_BATCH = 25
SLEEP_BETWEEN_REQUESTS = 1.0

OUTPUT_PREFIX = "microtrends"

BIG_ACCOUNTS_FILE = "big_accounts.txt"
HASHTAGS_FILE = "seed_hashtags.txt"
SUGGEST_WORDS_FILE = "seed_suggest_words.txt"
SOUNDS_FILE = "seed_sounds.txt"

# Sound info controls
SOUND_INFO_BATCH_SLEEP = 0.5


# -----------------------------
# Helpers
# -----------------------------

def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


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


def write_lines_append_dedup(path: str, new_items: List[str]) -> int:
    """Append new items to file (deduped). Returns number added."""
    existing = {x.strip().lower() for x in read_lines(path)}
    to_add = []
    for x in new_items:
        s = (x or "").strip()
        if not s:
            continue
        key = s.lower()
        if key not in existing:
            existing.add(key)
            to_add.append(s)

    if not to_add:
        return 0

    with open(path, "a", encoding="utf-8") as f:
        for x in to_add:
            f.write(x + "\n")

    return len(to_add)


def build_url(username: Optional[str], vid: Optional[str]) -> Optional[str]:
    if not username or not vid:
        return None
    return f"https://www.tiktok.com/@{username}/video/{vid}"


def extract_hashtags(raw: Dict[str, Any]) -> List[str]:
    tags = []
    for te in raw.get("textExtra") or []:
        hn = te.get("hashtagName")
        if hn:
            tags.append(hn.lower())
    for ch in raw.get("challenges") or []:
        title = ch.get("title")
        if title:
            tags.append(str(title).lower())

    out, seen = [], set()
    for t in tags:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def extract_suggest_words(raw: Dict[str, Any]) -> List[str]:
    out = []
    vsl = raw.get("videoSuggestWordsList") or {}
    for block in vsl.get("video_suggest_words_struct") or []:
        for w in block.get("words") or []:
            word = w.get("word")
            if word:
                out.append(str(word).strip().lower())

    dedup, seen = [], set()
    for w in out:
        if w and w not in seen:
            seen.add(w)
            dedup.append(w)
    return dedup


def suggest_phrase_to_hashtag_candidates(phrase: str) -> List[str]:
    """
    Convert a suggested search phrase into hashtag-like seeds.
    Example: "raah skeleton" -> ["raahskeleton", "raah_skeleton"]
    Returns tag text WITHOUT leading '#'
    """
    p = (phrase or "").strip().lower()
    if not p:
        return []

    cleaned = re.sub(r"[^a-z0-9\s]+", "", p)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return []

    joined = cleaned.replace(" ", "")
    underscored = cleaned.replace(" ", "_")

    out: List[str] = []
    if len(joined) >= MIN_HASHTAG_LEN:
        out.append(joined)
    if len(underscored) >= MIN_HASHTAG_LEN:
        out.append(underscored)

    seen = set()
    deduped = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def safe_ratio(n: Optional[float], d: Optional[float]) -> float:
    try:
        n = float(n or 0)
        d = float(d or 0)
        return (n / d) if d > 0 else 0.0
    except Exception:
        return 0.0


# ---------- Thumbnail slimming helpers ----------

def _safe_get(d: Any, *path: str) -> Optional[Any]:
    cur = d
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _first_url(img_obj: Any) -> Optional[str]:
    if not isinstance(img_obj, dict):
        return None
    urls = img_obj.get("url_list") or img_obj.get("urlList") or []
    if isinstance(urls, list) and urls:
        return urls[0]
    return None


def slim_raw_thumbs(video_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep ONLY raw.video.cover and raw.author.avatarThumb.
    Also produce convenience URLs for app display.
    """
    cover = _safe_get(video_obj, "video", "cover")
    avatar = _safe_get(video_obj, "author", "avatarThumb")
    return {
        "raw": {
            "video": {"cover": cover},
            "author": {"avatarThumb": avatar},
        },
        "cover_url": _first_url(cover),
        "author_avatar_url": _first_url(avatar),
    }


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


# -----------------------------------------------


def extract_video(data: Dict[str, Any], source: str) -> Dict[str, Any]:
    stats = data.get("stats") or {}
    author = data.get("author") or {}
    music = data.get("music") or {}

    vid = data.get("id") or data.get("itemId") or data.get("aweme_id")
    username = author.get("uniqueId")

    plays = stats.get("playCount") or stats.get("viewCount") or 0
    likes = stats.get("diggCount") or stats.get("likeCount") or 0
    comments = stats.get("commentCount") or 0
    shares = stats.get("shareCount") or 0

    hashtags = extract_hashtags(data)
    suggest_words = extract_suggest_words(data)

    sound_id = music.get("id")
    sound_title = music.get("title")
    sound_author = music.get("authorName")

    row = {
        "id": vid,
        "url": build_url(username, vid),
        "source": source,
        "desc": data.get("desc"),
        "createTime": data.get("createTime"),
        "author": {
            "uniqueId": username,
            "nickname": author.get("nickname"),
            "verified": author.get("verified"),
        },
        "stats": {
            "views": plays,
            "likes": likes,
            "comments": comments,
            "shares": shares,
        },
        "hashtags": hashtags,
        "suggest_words": suggest_words,
        "music": {
            "id": sound_id,
            "title": sound_title,
            "authorName": sound_author,
            "original": music.get("original"),
        },
    }

    # Only include the full raw payload if explicitly enabled (OFF by default)
    if INCLUDE_RAW:
        row["raw"] = data
    else:
        # keep only raw thumbnails (small!) even when full raw is off
        if INCLUDE_RAW_THUMBS:
            thumbs = slim_raw_thumbs(data)

            # Keep raw.video.cover and raw.author.avatarThumb
            row["raw"] = thumbs["raw"]

            # Add app-friendly URLs
            row["cover_url"] = thumbs["cover_url"]
            row["author_avatar_url"] = thumbs["author_avatar_url"]

    row["score_base"] = (
        safe_ratio(shares, plays) * 4.0
        + safe_ratio(comments, plays) * 3.0
        + safe_ratio(likes, plays) * 1.0
        + (2.0 if suggest_words else 0.0)
        + (1.0 if sound_id else 0.0)
    )
    return row


def dedupe_merge(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        vid = r.get("id")
        if not vid:
            continue
        if vid not in by_id:
            by_id[vid] = r
        else:
            existing = by_id[vid]
            srcs = existing.get("sources")
            if not srcs:
                s = existing.get("source")
                srcs = []
                if s:
                    srcs.append(s)
                existing.pop("source", None)
                existing["sources"] = srcs

            s2 = r.get("source")
            if s2 and s2 not in srcs:
                srcs.append(s2)

            # Keep thumbnails/urls if missing in the existing record
            if "raw" not in existing and "raw" in r:
                existing["raw"] = r["raw"]
            if existing.get("cover_url") is None and r.get("cover_url"):
                existing["cover_url"] = r["cover_url"]
            if existing.get("author_avatar_url") is None and r.get("author_avatar_url"):
                existing["author_avatar_url"] = r["author_avatar_url"]

    return list(by_id.values())


def add_pool_level_scores(rows: List[Dict[str, Any]], big_accounts: Set[str]) -> None:
    hashtag_freq: Dict[str, int] = {}
    sound_freq: Dict[str, int] = {}

    for r in rows:
        for h in r.get("hashtags") or []:
            hashtag_freq[h] = hashtag_freq.get(h, 0) + 1
        sid = ((r.get("music") or {}).get("id"))
        if sid:
            sound_freq[sid] = sound_freq.get(sid, 0) + 1

    for r in rows:
        score = float(r.get("score_base") or 0.0)

        rare_hits = 0
        for h in r.get("hashtags") or []:
            if hashtag_freq.get(h, 0) <= 2:
                rare_hits += 1
        score += min(3.0, rare_hits * 0.5)

        sid = ((r.get("music") or {}).get("id"))
        if sid and sound_freq.get(sid, 0) <= 2:
            score += 1.0

        au = ((r.get("author") or {}).get("uniqueId") or "").lower()
        if au and au in big_accounts:
            score += 1.0

        r["score"] = round(score, 4)


def top_topics(rows: List[Dict[str, Any]], k: int = 25) -> Dict[str, List[Dict[str, Any]]]:
    hashtag_count: Dict[str, int] = {}
    suggest_count: Dict[str, int] = {}
    sound_count: Dict[str, int] = {}

    for r in rows:
        for h in r.get("hashtags") or []:
            hashtag_count[h] = hashtag_count.get(h, 0) + 1
        for w in r.get("suggest_words") or []:
            suggest_count[w] = suggest_count.get(w, 0) + 1
        sid = ((r.get("music") or {}).get("id"))
        if sid:
            sound_count[sid] = sound_count.get(sid, 0) + 1

    top_hashtags = sorted(hashtag_count.items(), key=lambda x: x[1], reverse=True)[:k]
    top_suggest = sorted(suggest_count.items(), key=lambda x: x[1], reverse=True)[:k]
    top_sounds = sorted(sound_count.items(), key=lambda x: x[1], reverse=True)[:k]

    return {
        "top_hashtags": [{"tag": t, "count": c} for t, c in top_hashtags],
        "top_suggest_words": [{"phrase": p, "count": c} for p, c in top_suggest],
        "top_sounds": [{"sound_id": sid, "count": c} for sid, c in top_sounds],
    }


def should_flag_emerging(sound_info: Dict[str, Any], threshold: int) -> bool:
    count = sound_info.get("video_count")
    if count is None:
        return False
    return count < threshold


# -----------------------------
# Collectors
# -----------------------------
async def collect_trending(api: TikTokApi, target: int) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    rows: List[Dict[str, Any]] = []
    stall = 0

    while len(rows) < target:
        batch = []
        async for v in api.trending.videos(count=TRENDING_BATCH):
            batch.append(v)

        new_rows = []
        for v in batch:
            d = getattr(v, "as_dict", None)
            if not isinstance(d, dict):
                continue
            vid = d.get("id") or d.get("itemId") or d.get("aweme_id")
            if not vid or vid in seen:
                continue
            seen.add(vid)
            new_rows.append(extract_video(d, source="trending"))

        rows.extend(new_rows)

        if not new_rows:
            stall += 1
            if stall >= 6:
                break
        else:
            stall = 0

        await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)

    return rows[:target]


async def collect_accounts(api: TikTokApi, usernames: List[str], per_user: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for u in usernames:
        u = u.lstrip("@").strip()
        if not u:
            continue
        try:
            async for v in api.user(username=u).videos(count=per_user):
                d = getattr(v, "as_dict", None)
                if isinstance(d, dict):
                    rows.append(extract_video(d, source=f"account:{u}"))
        except Exception:
            pass
        await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)
    return rows


async def collect_hashtags(api: TikTokApi, tags: List[str], per_tag: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for t in tags:
        tag = t.strip().lstrip("#")
        if not tag:
            continue
        try:
            async for v in api.hashtag(name=tag).videos(count=per_tag):
                d = getattr(v, "as_dict", None)
                if isinstance(d, dict):
                    rows.append(extract_video(d, source=f"hashtag:{tag}"))
        except Exception:
            pass
        await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)
    return rows


async def collect_sounds(api: TikTokApi, sound_ids: List[str], per_sound: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for sid in sound_ids:
        sid = (sid or "").strip()
        if not sid:
            continue
        try:
            async for v in api.sound(id=sid).videos(count=per_sound):
                d = getattr(v, "as_dict", None)
                if isinstance(d, dict):
                    rows.append(extract_video(d, source=f"sound:{sid}"))
        except Exception:
            pass
        await asyncio.sleep(SLEEP_BETWEEN_REQUESTS)
    return rows


async def collect_sound_info(api: TikTokApi, sound_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    sound_meta: Dict[str, Dict[str, Any]] = {}
    for sid in sound_ids:
        if not sid or sid in sound_meta:
            continue
        try:
            info = await api.sound(id=sid).info()
        except Exception:
            info = None
        if isinstance(info, dict):
            sound_meta[sid] = {
                "id": sid,
                "title": info.get("title") or info.get("music", {}).get("title"),
                "authorName": info.get("authorName") or info.get("music", {}).get("authorName"),
                "original": info.get("original") if "original" in info else info.get("music", {}).get("original"),
                "video_count": extract_sound_video_count(info),
            }
        await asyncio.sleep(SOUND_INFO_BATCH_SLEEP)
    return sound_meta


# -----------------------------
# Seeding from trending
# -----------------------------

def seed_from_trending(trending_rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    hashtag_freq: Dict[str, int] = {}
    creator_freq: Dict[str, int] = {}
    suggest_freq: Dict[str, int] = {}
    sound_freq: Dict[str, int] = {}

    for r in trending_rows:
        for h in r.get("hashtags") or []:
            if len(h) >= MIN_HASHTAG_LEN:
                hashtag_freq[h] = hashtag_freq.get(h, 0) + 1

        u = ((r.get("author") or {}).get("uniqueId") or "").lower()
        if u:
            creator_freq[u] = creator_freq.get(u, 0) + 1

        for w in r.get("suggest_words") or []:
            w = w.strip().lower()
            if w and len(w) >= 4:
                suggest_freq[w] = suggest_freq.get(w, 0) + 1

        sid = ((r.get("music") or {}).get("id") or "")
        if sid:
            sound_freq[sid] = sound_freq.get(sid, 0) + 1

    top_hashtags = [t for t, _ in sorted(hashtag_freq.items(), key=lambda x: x[1], reverse=True)[:ADD_TOP_HASHTAGS]]
    top_creators = [u for u, _ in sorted(creator_freq.items(), key=lambda x: x[1], reverse=True)[:ADD_TOP_CREATORS]]
    top_suggest = [w for w, _ in sorted(suggest_freq.items(), key=lambda x: x[1], reverse=True)[:ADD_TOP_SUGGEST_WORDS]]
    top_sounds = [sid for sid, _ in sorted(sound_freq.items(), key=lambda x: x[1], reverse=True)[:ADD_TOP_SOUNDS]]

    return {
        "hashtags": top_hashtags,
        "creators": top_creators,
        "suggest_words": top_suggest,
        "sounds": top_sounds,
    }


async def main() -> None:
    date = today_str()
    out_path = f"{OUTPUT_PREFIX}_{date}.json"

    context_options = {"locale": "en-AU", "timezone_id": "Australia/Sydney"}
    proxies = [{"server": AU_PROXY}] if AU_PROXY else None

    started = time.time()

    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[MS_TOKEN] if MS_TOKEN else None,
            num_sessions=1,
            sleep_after=3,
            context_options=context_options,
            proxies=proxies,
            headless=True,
        )

        print("1) Collect trending...")
        trending_rows = await collect_trending(api, TRENDING_TARGET)

        seeds = seed_from_trending(trending_rows)

        # Append seeds to files (deduped)
        added_accounts = write_lines_append_dedup(BIG_ACCOUNTS_FILE, seeds["creators"])
        added_tags = write_lines_append_dedup(HASHTAGS_FILE, seeds["hashtags"])
        added_suggest = write_lines_append_dedup(SUGGEST_WORDS_FILE, seeds["suggest_words"])
        added_sounds = write_lines_append_dedup(SOUNDS_FILE, seeds["sounds"])

        # Suggest-phrase -> hashtag seeding
        suggest_hashtag_candidates: List[str] = []
        for phrase in seeds["suggest_words"]:
            suggest_hashtag_candidates.extend(suggest_phrase_to_hashtag_candidates(phrase))
        added_tags_from_suggest = write_lines_append_dedup(HASHTAGS_FILE, suggest_hashtag_candidates)

        # Reload expanded lists
        all_accounts = [u.lstrip("@").strip().lower() for u in read_lines(BIG_ACCOUNTS_FILE)]
        all_tags = [t.strip().lstrip("#").lower() for t in read_lines(HASHTAGS_FILE)]
        all_sounds = [s.strip() for s in read_lines(SOUNDS_FILE)]

        print(
            f"Seeded +{added_accounts} creators, +{added_tags} hashtags "
            f"(+{added_tags_from_suggest} from suggest-phrases), "
            f"+{added_suggest} suggest-phrases, +{added_sounds} sounds"
        )

        print("2) Collect big accounts latest posts...")
        account_rows = await collect_accounts(api, all_accounts[:MAX_ACCOUNTS_TO_CHECK], PER_ACCOUNT_LIMIT)

        print("3) Collect hashtag videos...")
        hashtag_rows = await collect_hashtags(api, all_tags[:MAX_HASHTAGS_TO_CHECK], PER_HASHTAG_LIMIT)

        print("4) Collect sound videos...")
        sound_rows = await collect_sounds(api, all_sounds[:MAX_SOUNDS_TO_CHECK], PER_SOUND_LIMIT)

        print("5) Hydrate sound stats...")
        sound_ids = sorted({(r.get("music") or {}).get("id") for r in trending_rows if (r.get("music") or {}).get("id")})
        sound_meta = await collect_sound_info(api, sound_ids)

    merged = dedupe_merge(trending_rows + account_rows + hashtag_rows + sound_rows)

    for r in merged:
        sid = (r.get("music") or {}).get("id")
        if sid and sid in sound_meta:
            r.setdefault("music", {})["video_count"] = sound_meta[sid].get("video_count")

    add_pool_level_scores(merged, set(all_accounts))
    merged.sort(key=lambda r: float(r.get("score") or 0.0), reverse=True)

    finished = time.time()

    emerging_sounds = [
        sound
        for sound in sound_meta.values()
        if should_flag_emerging(sound, threshold=1000)
    ]

    output = {
        "meta": {
            "date": date,
            "ms_token_present": bool(MS_TOKEN),
            "proxy_present": bool(AU_PROXY),
            "include_raw": INCLUDE_RAW,
            "include_raw_thumbs": INCLUDE_RAW_THUMBS,
            "counts": {
                "trending": len(trending_rows),
                "accounts_raw": len(account_rows),
                "hashtags_raw": len(hashtag_rows),
                "sounds_raw": len(sound_rows),
                "unique_total": len(merged),
            },
            "seed_appended": {
                "added_creators": added_accounts,
                "added_hashtags": added_tags,
                "added_hashtags_from_suggest_phrases": added_tags_from_suggest,
                "added_suggest_words": added_suggest,
                "added_sounds": added_sounds,
                "files": {
                    "big_accounts": BIG_ACCOUNTS_FILE,
                    "seed_hashtags": HASHTAGS_FILE,
                    "seed_suggest_words": SUGGEST_WORDS_FILE,
                    "seed_sounds": SOUNDS_FILE,
                },
            },
            "targets": {
                "trending_target": TRENDING_TARGET,
                "accounts_checked": min(MAX_ACCOUNTS_TO_CHECK, len(all_accounts)),
                "per_account_limit": PER_ACCOUNT_LIMIT,
                "hashtags_checked": min(MAX_HASHTAGS_TO_CHECK, len(all_tags)),
                "per_hashtag_limit": PER_HASHTAG_LIMIT,
                "sounds_checked": min(MAX_SOUNDS_TO_CHECK, len(all_sounds)),
                "per_sound_limit": PER_SOUND_LIMIT,
            },
            "elapsed_seconds": round(finished - started, 3),
        },
        "topics": top_topics(merged, k=25),
        "sound_meta": sound_meta,
        "emerging_sounds": emerging_sounds,
        "items": merged,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(merged)} unique videos to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
