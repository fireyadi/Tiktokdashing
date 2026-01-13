import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from playwright.async_api import async_playwright

PROFILE_DIR = "./chrome-profile"


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


async def focus_player(page) -> None:
    await page.mouse.click(640, 360)
    await page.wait_for_timeout(80)


async def dismiss_popups(page) -> None:
    for sel in [
        'button:has-text("Accept")',
        'button:has-text("Agree")',
        'button:has-text("Allow all")',
        'button:has-text("Not now")',
        'button:has-text("Continue")',
        'button:has-text("Log in")',
        '[role="dialog"] button:has-text("Close")',
        'button[aria-label="Close"]',
    ]:
        try:
            loc = page.locator(sel).first
            if await loc.count() and await loc.is_visible():
                # Don't auto-click "Log in" — just close other popups if possible.
                if "Log in" in sel:
                    continue
                await loc.click(timeout=300)
                await page.wait_for_timeout(150)
        except Exception:
            pass


async def debug_dump(page, tag: str) -> None:
    await page.screenshot(path=f"debug_{tag}.png", full_page=True)
    html = await page.content()
    with open(f"debug_{tag}.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[debug] wrote debug_{tag}.png and debug_{tag}.html")


async def get_current_video_data(page) -> Dict[str, Any]:
    # Try multiple ways to find a video URL.
    return await page.evaluate(
        """() => {
            const centerY = window.innerHeight / 2;

            const videoAnchors = Array.from(document.querySelectorAll('a[href*="/video/"]'))
              .map(a => {
                const r = a.getBoundingClientRect();
                const y = r.top + r.height / 2;
                return { href: a.href, r, dist: Math.abs(y - centerY) };
              })
              .filter(x => x.r.width > 0 && x.r.height > 0 && x.r.bottom > 0 && x.r.top < window.innerHeight)
              .sort((a, b) => a.dist - b.dist);

            const videoUrlA = videoAnchors[0]?.href ?? null;

            // Fallback: look for any url-like string in meta tags
            const ogUrl = document.querySelector('meta[property="og:url"]')?.getAttribute("content") ?? null;

            const getText = (sel) => document.querySelector(sel)?.textContent?.trim() ?? null;

            const caption =
              document.querySelector('[data-e2e="browse-video-desc"]')?.textContent?.trim() ??
              document.querySelector('[data-e2e="video-desc"]')?.textContent?.trim() ??
              null;

            const author =
              document.querySelector('[data-e2e="browse-username"]')?.textContent?.trim() ??
              document.querySelector('[data-e2e="video-author-uniqueid"]')?.textContent?.trim() ??
              null;

            const sound =
              document.querySelector('[data-e2e="browse-music"]')?.textContent?.trim() ??
              document.querySelector('[data-e2e="video-music"]')?.textContent?.trim() ??
              null;

            const likeRaw = getText('[data-e2e="like-count"]');
            const commentRaw = getText('[data-e2e="comment-count"]');
            const shareRaw = getText('[data-e2e="share-count"]');

            return {
              video_url: videoUrlA || ogUrl,
              visible_video_links: videoAnchors.length,
              author,
              caption,
              sound,
              like_raw: likeRaw,
              comment_raw: commentRaw,
              share_raw: shareRaw
            };
        }"""
    )


async def run(max_videos: int = 100, delay_ms: int = 450) -> None:
    results = []
    seen = set()
    misses = 0

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 720},
            locale="en-AU",
        )
        page = await context.new_page()

        await page.goto("https://www.tiktok.com/", wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)

        for i in range(max_videos * 6):  # safety cap
            await dismiss_popups(page)

            raw = await get_current_video_data(page)
            key = raw.get("video_url")
            link_count = raw.get("visible_video_links", 0)

            if not key:
                misses += 1
                print(f"[warn] No video_url. visible /video/ links={link_count}. misses={misses}")
                if misses in (3, 8):
                    await debug_dump(page, f"novideo_{misses}")
            else:
                misses = 0
                if key not in seen:
                    seen.add(key)
                    results.append(
                        {
                            "url": key,
                            "author": raw.get("author"),
                            "caption": raw.get("caption"),
                            "likes": clean_count(raw.get("like_raw")),
                            "comments": clean_count(raw.get("comment_raw")),
                            "shares": clean_count(raw.get("share_raw")),
                            "sound": raw.get("sound"),
                            "scraped_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    print(f"Collected {len(seen)}/{max_videos} (links={link_count})")
                    if len(seen) >= max_videos:
                        break

            # ArrowDown navigation (fast)
            await focus_player(page)
            await page.keyboard.press("ArrowDown")
            await page.wait_for_timeout(delay_ms)
            await page.keyboard.press("ArrowDown")
            await page.wait_for_timeout(delay_ms)

        await context.close()

    with open("fyp_100.json", "w", encoding="utf-8") as f:
        json.dump({"count": len(results), "items": results}, f, indent=2)

    print(f"\n🎉 DONE! Scraped {len(results)} videos.")
    print("✅ Output saved to: fyp_100.json\n")


if __name__ == "__main__":
    asyncio.run(run(max_videos=10))
