import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from playwright.async_api import async_playwright


def clean_count(txt: Optional[str]) -> Optional[int]:
    if not txt:
        return None
    t = str(txt).strip().upper().replace(",", "")
    m = re.match(r"^([\d.]+)\s*([KM])?$", t)
    if not m:
        return None
    n = float(m.group(1))
    mult = 1_000 if m.group(2) == "K" else 1_000_000 if m.group(2) == "M" else 1
    return int(round(n * mult))


async def get_current_video_data(page) -> Dict[str, Any]:
    # Best-effort selectors; TikTok changes DOM often.
    return await page.evaluate(
        """() => {
            const getText = (sel) => document.querySelector(sel)?.textContent?.trim() ?? null;

            const pageUrl = location.href;
            const canonical = document.querySelector('link[rel="canonical"]')?.getAttribute("href") ?? null;

            const caption =
                document.querySelector('[data-e2e="browse-video-desc"]')?.textContent?.trim() ??
                document.querySelector('[data-e2e="video-desc"]')?.textContent?.trim() ??
                null;

            const author =
                document.querySelector('[data-e2e="browse-username"]')?.textContent?.trim() ??
                document.querySelector('[data-e2e="video-author-uniqueid"]')?.textContent?.trim() ??
                null;

            const likeRaw = getText('[data-e2e="like-count"]');
            const commentRaw = getText('[data-e2e="comment-count"]');
            const shareRaw = getText('[data-e2e="share-count"]');

            const sound =
                document.querySelector('[data-e2e="browse-music"]')?.textContent?.trim() ??
                document.querySelector('[data-e2e="video-music"]')?.textContent?.trim() ??
                null;

            return { page_url: pageUrl, canonical_url: canonical, author, caption, like_raw: likeRaw, comment_raw: commentRaw, share_raw: shareRaw, sound };
        }"""
    )


async def run(max_videos: int = 100, delay_ms: int = 1200, headless: bool = True) -> None:
    # Load saved session
    with open("tiktok_state.json", "r", encoding="utf-8") as f:
        storage_state = json.load(f)

    results = []
    seen = set()
    attempts_without_new = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=storage_state, viewport={"width": 1280, "height": 720})
        page = await context.new_page()

        await page.goto("https://www.tiktok.com/", wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        while len(seen) < max_videos:
            raw = await get_current_video_data(page)
            key = raw.get("canonical_url") or raw.get("page_url")

            if key and key not in seen:
                seen.add(key)
                attempts_without_new = 0

                results.append({
                    "url": key,
                    "author": raw.get("author"),
                    "caption": raw.get("caption"),
                    "likes": clean_count(raw.get("like_raw")),
                    "comments": clean_count(raw.get("comment_raw")),
                    "shares": clean_count(raw.get("share_raw")),
                    "sound": raw.get("sound"),
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
                print(f"Collected {len(seen)}/{max_videos}")

            else:
                attempts_without_new += 1

            await page.keyboard.press("ArrowDown")
            await page.wait_for_timeout(delay_ms)

            if attempts_without_new >= 12:
                print("Stuck; trying PageDown + reload.")
                await page.keyboard.press("PageDown")
                await page.wait_for_timeout(delay_ms)
                await page.reload(wait_until="domcontentloaded")
                await page.wait_for_timeout(2000)
                attempts_without_new = 0

        await context.close()
        await browser.close()

    with open("fyp_100.json", "w", encoding="utf-8") as f:
        json.dump({"count": len(results), "items": results}, f, indent=2)

    print("Wrote fyp_100.json")


if __name__ == "__main__":
    asyncio.run(run())
