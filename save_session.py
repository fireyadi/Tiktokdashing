# save_session.py
# Uses your *installed* Google Chrome (not Playwright's bundled Chromium)
# and saves a persistent profile folder so you stay logged in like a normal browser.

import asyncio
from playwright.async_api import async_playwright

PROFILE_DIR = "./chrome-profile"  # folder created in your repo

async def run():
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=False,
            channel="chrome",  # <-- real Chrome app
            viewport={"width": 1280, "height": 720},
        )
        page = await context.new_page()
        await page.goto("https://www.tiktok.com/login", wait_until="domcontentloaded")

        input("Log in manually in the opened Chrome window, then press Enter here...")

        # IMPORTANT: with a persistent profile you don't need storageState files anymore.
        # Closing saves cookies/session into PROFILE_DIR automatically.
        await context.close()
        print(f"✅ Login saved to persistent profile folder: {PROFILE_DIR}")

if __name__ == "__main__":
    asyncio.run(run())
