import asyncio
import json
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={"width": 1280, "height": 720})
        page = await context.new_page()

        await page.goto("https://www.tiktok.com/login", wait_until="domcontentloaded")
        input("Log in manually in the browser, then press Enter here...")

        state = await context.storage_state()
        with open("tiktok_state.json", "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

        await browser.close()
        print("Saved session to tiktok_state.json")

if __name__ == "__main__":
    asyncio.run(run())
