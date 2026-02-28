#!/usr/bin/env python3
"""
Enrich SF insurance companies with websites and emails.

Strategy:
1. Search Google Maps for each company by name
2. Click into the listing to get website URL
3. Visit each website to scrape email addresses
4. Output enriched SF-only CSV
"""

import csv
import json
import re
import time
import random
from pathlib import Path
from urllib.parse import quote_plus, urlparse

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
import requests
from fake_useragent import UserAgent

INPUT_CSV = Path("output/sf_bay_area_commercial_insurance.csv")
OUTPUT_CSV = Path("output/sf_commercial_insurance_enriched.csv")
UA = UserAgent(fallback="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")


def get_headers():
    return {
        "User-Agent": UA.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "DNT": "1",
    }


def extract_emails(html: str) -> list[str]:
    """Extract email addresses from HTML, filtering junk."""
    raw = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
    junk = ["example.com", "sentry", "webpack", "wixpress", "wix.com",
            "squarespace", "wordpress", "google.com", "schema.org",
            "w3.org", "sentry.io", "cloudflare", "jquery", "bootstrap",
            "placeholder", "yourdomain", "email.com", "domain.com",
            "test.com", "noreply", "no-reply", "yoursite", "change.me",
            "company.com", "sample.com", ".png", ".jpg", ".gif", ".svg"]
    filtered = []
    for em in raw:
        em_lower = em.lower()
        if not any(j in em_lower for j in junk) and len(em) < 80:
            filtered.append(em)
    # Deduplicate preserving order
    seen = set()
    result = []
    for em in filtered:
        if em.lower() not in seen:
            seen.add(em.lower())
            result.append(em)
    return result


def scrape_website_for_email(website: str) -> str:
    """Visit a website and try to find an email address."""
    if not website or not website.startswith("http"):
        return ""

    try:
        resp = requests.get(website, headers=get_headers(), timeout=8, allow_redirects=True)
        if resp.status_code != 200:
            return ""

        emails = extract_emails(resp.text)
        if emails:
            return emails[0]

        # Try /contact or /about pages
        parsed = urlparse(resp.url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for suffix in ["/contact", "/contact-us", "/about", "/about-us", "/contactus"]:
            try:
                r2 = requests.get(base + suffix, headers=get_headers(), timeout=6, allow_redirects=True)
                if r2.status_code == 200:
                    emails = extract_emails(r2.text)
                    if emails:
                        return emails[0]
            except Exception:
                pass

    except Exception:
        pass

    return ""


def main():
    # Load all rows
    all_rows = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            all_rows.append(row)

    # Identify SF rows (Google Maps sourced only — CDI ones have no local data)
    sf_indices = []
    for i, row in enumerate(all_rows):
        city = row.get("city", "")
        if (city.startswith("SF ") or city == "San Francisco") and "Google Maps" in row.get("source", ""):
            sf_indices.append(i)

    print(f"Total rows: {len(all_rows)}")
    print(f"SF companies to enrich: {len(sf_indices)}")
    print()

    # Phase 1: Get websites from Google Maps detail pages
    print("=" * 60)
    print("PHASE 1: Getting websites from Google Maps")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        found_websites = 0
        for count, idx in enumerate(sf_indices):
            row = all_rows[idx]
            name = row["name"]

            if row.get("website"):
                found_websites += 1
                continue

            print(f"  [{count+1}/{len(sf_indices)}] {name}")

            # Search Maps for this specific company
            search = f"{name} San Francisco CA"
            maps_url = f"https://www.google.com/maps/search/{quote_plus(search)}"

            try:
                page.goto(maps_url, wait_until="domcontentloaded", timeout=20000)
                time.sleep(random.uniform(2.0, 3.5))

                # Check if we landed directly on a place page or on search results
                website = ""
                address = ""

                # Try to get website from the detail panel
                try:
                    web_el = page.locator('a[data-item-id="authority"]')
                    if web_el.count():
                        website = web_el.first.get_attribute("href") or ""
                except Exception:
                    pass

                if not website:
                    try:
                        web_el = page.locator('[data-tooltip="Open website"]')
                        if web_el.count():
                            website = web_el.first.get_attribute("href") or ""
                    except Exception:
                        pass

                if not website:
                    # Try clicking the first search result
                    try:
                        first_result = page.locator('[role="feed"] > div > div > a[aria-label]').first
                        if first_result.count():
                            first_result.click()
                            time.sleep(random.uniform(1.5, 2.5))

                            web_el = page.locator('a[data-item-id="authority"]')
                            if web_el.count():
                                website = web_el.first.get_attribute("href") or ""

                            if not website:
                                web_el = page.locator('[data-tooltip="Open website"]')
                                if web_el.count():
                                    website = web_el.first.get_attribute("href") or ""
                    except Exception:
                        pass

                # Also grab address if we don't have it
                if not row.get("address") or row["address"] == row["name"]:
                    try:
                        addr_el = page.locator('[data-item-id="address"]')
                        if addr_el.count():
                            raw_addr = addr_el.first.inner_text(timeout=2000)
                            all_rows[idx]["address"] = raw_addr.strip()
                    except Exception:
                        pass

                if website:
                    all_rows[idx]["website"] = website
                    found_websites += 1
                    print(f"    -> {website}")
                else:
                    print(f"    -> (no website)")

            except Exception as e:
                print(f"    -> error: {e}")

            time.sleep(random.uniform(1.5, 3.0))

            if (count + 1) % 25 == 0:
                print(f"  ... {count+1}/{len(sf_indices)} searched, {found_websites} websites found ...")
                time.sleep(random.uniform(3, 6))

        browser.close()

    print(f"\nWebsites found: {found_websites}/{len(sf_indices)}")

    # Phase 2: Scrape emails from websites
    print()
    print("=" * 60)
    print("PHASE 2: Scraping emails from websites")
    print("=" * 60)

    found_emails = 0
    sites_to_check = [idx for idx in sf_indices if all_rows[idx].get("website")]
    print(f"  {len(sites_to_check)} companies have websites to check for emails")

    for count, idx in enumerate(sites_to_check):
        row = all_rows[idx]
        if row.get("email"):
            found_emails += 1
            continue

        website = row["website"]
        print(f"  [{count+1}/{len(sites_to_check)}] {row['name']}")

        email = scrape_website_for_email(website)
        if email:
            all_rows[idx]["email"] = email
            found_emails += 1
            print(f"    -> {email}")

        time.sleep(random.uniform(0.5, 1.5))

        if (count + 1) % 30 == 0:
            print(f"  ... {count+1}/{len(sites_to_check)} checked, {found_emails} emails found ...")

    print(f"\nEmails found: {found_emails}/{len(sites_to_check)}")

    # Write enriched SF-only CSV
    print()
    print("=" * 60)
    print("SAVING ENRICHED DATA")
    print("=" * 60)

    sf_rows = [all_rows[i] for i in sf_indices]
    # Sort by name
    sf_rows.sort(key=lambda r: r["name"].lower())

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in sf_rows:
            writer.writerow(row)

    print(f"  Enriched SF CSV: {OUTPUT_CSV} ({len(sf_rows)} rows)")

    # Also update the main CSV
    with open(INPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"  Main CSV updated: {INPUT_CSV}")

    # Stats
    print()
    print("=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"  Total SF companies: {len(sf_rows)}")
    print(f"  With phone:   {sum(1 for r in sf_rows if r.get('phone'))}")
    print(f"  With website: {sum(1 for r in sf_rows if r.get('website'))}")
    print(f"  With email:   {sum(1 for r in sf_rows if r.get('email'))}")
    print()
    print("DONE!")


if __name__ == "__main__":
    main()
