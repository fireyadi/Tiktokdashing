# TikTok FYP Scraper (Playwright + Python)

This project logs into TikTok using a saved Playwright session (cookies/localStorage),
scrolls/advances through the For You Page (FYP), collects 100 unique videos, and outputs JSON.

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
```

## TikTokApi trending + sound stats
Use `tiktok_api_trending.py` to pull trending videos, then hydrate each sound with
its total video count via `api.sound(id=...).info()`. This lets you flag emerging sounds
that have fewer than 1,000 videos overall.

```bash
export ms_token="YOUR_MS_TOKEN"
python tiktok_api_trending.py
```

## Hydrate saved sound IDs
If you already have a list of sound IDs (for example `seed_sounds.txt`), use
`hydrate_sounds.py` to fetch how many videos use each sound.

```bash
export ms_token="YOUR_MS_TOKEN"
python hydrate_sounds.py --input seed_sounds.txt --output sound_hydration.json
```
