# TikTok FYP Scraper (Playwright + Python)

This project logs into TikTok using a saved Playwright session (cookies/localStorage),
scrolls/advances through the For You Page (FYP), collects 100 unique videos, and outputs JSON.

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
