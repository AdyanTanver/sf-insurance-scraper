#!/usr/bin/env python3
"""Quick test: Google Maps batch extraction for one query."""

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from urllib.parse import quote_plus
import time
import re

JS_EXTRACT = r"""() => {
    const results = [];
    const items = document.querySelectorAll('[role="feed"] > div > div');
    for (const item of items) {
        const link = item.querySelector('a[aria-label]');
        if (!link) continue;
        const name = link.getAttribute('aria-label') || '';
        if (!name) continue;
        const text = item.innerText || '';
        const ratingMatch = text.match(/(\d\.\d)\s*\((\d[\d,]*)\)/);
        const phoneMatch = text.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
        let address = '';
        for (const line of text.split('\n')) {
            if (/^\d+\s/.test(line.trim()) || /,\s*CA/.test(line)) {
                address = line.trim();
                break;
            }
        }
        results.push({
            name,
            rating: ratingMatch ? ratingMatch[1] : '',
            reviews: ratingMatch ? ratingMatch[2] : '',
            phone: phoneMatch ? phoneMatch[0] : '',
            address
        });
    }
    return results;
}"""

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(viewport={"width": 1280, "height": 900}, locale="en-US")
    page = context.new_page()
    Stealth().apply_stealth_sync(page)

    query = "commercial insurance"
    url = f"https://www.google.com/maps/search/{quote_plus(query + ' near San Francisco, CA')}/@37.7749,-122.4194,13z"
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    time.sleep(5)
    page.wait_for_selector('[role="feed"]', timeout=10000)

    # Scroll to load all
    feed = page.locator('[role="feed"]')
    prev = 0
    for i in range(15):
        feed.evaluate("el => el.scrollBy(0, 1200)")
        time.sleep(1)
        cur = page.locator('[role="feed"] > div > div > a[aria-label]').count()
        if cur == prev and i > 2:
            break
        prev = cur

    data = page.evaluate(JS_EXTRACT)
    print(f"Extracted {len(data)} businesses:")
    for d in data:
        print(f'  {d["name"]} | {d["address"]} | {d["phone"]} | {d["rating"]}({d["reviews"]})')

    browser.close()
