#!/usr/bin/env python3
"""
SF Bay Area Commercial Insurance Company Scraper
=================================================
Multi-source scraper that finds every commercial insurance company in
San Francisco and surrounding Bay Area cities.

Sources:
  1. Google Maps (Playwright + stealth)
  2. Yelp (Playwright + stealth + JSON-LD parsing)
  3. Yellow Pages (Playwright + stealth + BeautifulSoup)

All sources use Playwright with stealth mode since requests-based
approaches are blocked by Cloudflare/anti-bot on all major directories.

Output: CSV + JSON with deduplicated results.
"""

import csv
import hashlib
import json
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class InsuranceCompany:
    name: str
    address: str = ""
    city: str = ""
    state: str = "CA"
    zip_code: str = ""
    phone: str = ""
    website: str = ""
    email: str = ""
    rating: str = ""
    review_count: str = ""
    categories: str = ""
    description: str = ""
    source: str = ""
    source_url: str = ""

    @property
    def dedup_key(self) -> str:
        """Normalize name + city for dedup."""
        norm = re.sub(r"[^a-z0-9]", "", self.name.lower())
        city = re.sub(r"[^a-z0-9]", "", self.city.lower())
        return f"{norm}_{city}"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SEARCH_QUERIES = [
    "commercial insurance",
    "business insurance",
    "commercial insurance broker",
    "commercial insurance agent",
    "commercial property insurance",
    "general liability insurance",
    "workers compensation insurance",
    "professional liability insurance",
    "commercial auto insurance",
    "business liability insurance",
    "surety bond insurance",
    "commercial lines insurance",
    "insurance agency commercial",
    "business insurance broker",
    "commercial risk insurance",
]

BAY_AREA_LOCATIONS = [
    "San Francisco, CA",
    "Oakland, CA",
    "San Jose, CA",
    "Daly City, CA",
    "South San Francisco, CA",
    "San Mateo, CA",
    "Redwood City, CA",
    "Palo Alto, CA",
    "Mountain View, CA",
    "Sunnyvale, CA",
    "Santa Clara, CA",
    "Fremont, CA",
    "Hayward, CA",
    "Berkeley, CA",
    "Richmond, CA",
    "Walnut Creek, CA",
    "Concord, CA",
    "San Rafael, CA",
    "Novato, CA",
    "Burlingame, CA",
    "San Bruno, CA",
    "Pleasanton, CA",
    "Dublin, CA",
    "Milpitas, CA",
    "Campbell, CA",
    "Sausalito, CA",
    "Mill Valley, CA",
    "Emeryville, CA",
    "Alameda, CA",
    "San Leandro, CA",
]

# Google Maps bounding boxes (lat/lng) for Bay Area sub-regions
GMAPS_REGIONS = [
    {"name": "SF Downtown",       "lat": 37.7880, "lng": -122.4075, "zoom": 14},
    {"name": "SF Mission/Castro",  "lat": 37.7600, "lng": -122.4200, "zoom": 14},
    {"name": "SF Sunset/Richmond", "lat": 37.7650, "lng": -122.4800, "zoom": 14},
    {"name": "SF SOMA/FiDi",       "lat": 37.7850, "lng": -122.3950, "zoom": 14},
    {"name": "Oakland Downtown",   "lat": 37.8044, "lng": -122.2712, "zoom": 14},
    {"name": "San Jose Downtown",  "lat": 37.3382, "lng": -121.8863, "zoom": 14},
    {"name": "Palo Alto",          "lat": 37.4419, "lng": -122.1430, "zoom": 14},
    {"name": "San Mateo",          "lat": 37.5630, "lng": -122.3255, "zoom": 14},
    {"name": "Walnut Creek",       "lat": 37.9101, "lng": -122.0652, "zoom": 14},
    {"name": "Fremont",            "lat": 37.5485, "lng": -121.9886, "zoom": 14},
    {"name": "Berkeley",           "lat": 37.8716, "lng": -122.2727, "zoom": 14},
    {"name": "Daly City",          "lat": 37.6879, "lng": -122.4702, "zoom": 14},
    {"name": "Sunnyvale",          "lat": 37.3688, "lng": -122.0363, "zoom": 14},
    {"name": "Pleasanton",         "lat": 37.6624, "lng": -121.8747, "zoom": 14},
    {"name": "San Rafael",         "lat": 37.9735, "lng": -122.5311, "zoom": 14},
    {"name": "Redwood City",       "lat": 37.4852, "lng": -122.2364, "zoom": 14},
    {"name": "Concord",            "lat": 37.9780, "lng": -122.0311, "zoom": 14},
    {"name": "Hayward",            "lat": 37.6688, "lng": -122.0808, "zoom": 14},
]

OUTPUT_DIR = Path(__file__).parent / "output"
UA = UserAgent(fallback="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")


def get_headers() -> dict:
    return {
        "User-Agent": UA.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    }


def polite_sleep(low: float = 2.0, high: float = 5.0):
    time.sleep(random.uniform(low, high))


def extract_city_from_address(address: str) -> str:
    """Try to pull the city from a full address string."""
    if not address:
        return ""
    # Pattern: ..., City, CA ZIP
    m = re.search(r",\s*([A-Za-z\s]+),\s*CA", address)
    if m:
        return m.group(1).strip()
    # Pattern: ..., City CA
    m = re.search(r",\s*([A-Za-z\s]+)\s+CA", address)
    if m:
        return m.group(1).strip()
    return ""


def extract_zip(address: str) -> str:
    m = re.search(r"\b(\d{5}(?:-\d{4})?)\b", address)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Source 1: Google Maps via Playwright
# ---------------------------------------------------------------------------

def scrape_google_maps(results: list[InsuranceCompany]):
    """Scrape Google Maps for commercial insurance companies across the Bay Area.

    Uses batch extraction from the list view (much faster than clicking each listing).
    """
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError:
        print("[!] Playwright not installed — skipping Google Maps source.")
        print("    Run: pip install playwright playwright-stealth && playwright install chromium")
        return

    queries = [
        "commercial insurance",
        "business insurance broker",
        "commercial insurance agent",
        "commercial property insurance",
        "general liability insurance",
        "workers compensation insurance",
    ]

    print("\n" + "=" * 60)
    print("SOURCE 1: Google Maps")
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

        total_found = 0
        for region in GMAPS_REGIONS:
            for query in queries:
                search_term = f"{query} near {region['name']}, CA"
                url = f"https://www.google.com/maps/search/{quote_plus(search_term)}/@{region['lat']},{region['lng']},{region['zoom']}z"

                print(f"  [{region['name']}] Searching: {query}")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    polite_sleep(2, 4)

                    # Wait for results feed
                    try:
                        page.wait_for_selector('[role="feed"]', timeout=8000)
                    except Exception:
                        print(f"    No results feed found, skipping...")
                        continue

                    # Scroll to load all results
                    feed = page.locator('[role="feed"]')
                    prev_count = 0
                    for scroll_attempt in range(12):
                        feed.evaluate("el => el.scrollBy(0, 1200)")
                        polite_sleep(0.5, 1.0)
                        current_count = page.locator('[role="feed"] > div > div > a').count()
                        if current_count == prev_count and scroll_attempt > 2:
                            break
                        prev_count = current_count

                    # Batch extract from list view using JS — no clicking needed
                    listings_data = page.evaluate(r"""() => {
                        const results = [];
                        const items = document.querySelectorAll('[role="feed"] > div > div');
                        for (const item of items) {
                            const link = item.querySelector('a[aria-label]');
                            if (!link) continue;
                            const name = link.getAttribute('aria-label') || '';
                            if (!name) continue;

                            const allText = item.innerText || '';
                            const lines = allText.split('\n').map(l => l.trim()).filter(Boolean);

                            let rating = '';
                            let reviewCount = '';
                            const ratingMatch = allText.match(/(\d\.\d)\s*\((\d[\d,]*)\)/);
                            if (ratingMatch) {
                                rating = ratingMatch[1];
                                reviewCount = ratingMatch[2];
                            }

                            let address = '';
                            let category = '';
                            for (const line of lines) {
                                if (/^\d+\s/.test(line) || /,\s*CA/.test(line)) {
                                    address = line;
                                } else if (!category && line !== name && !line.match(/^[\d.]+$/) &&
                                           !line.match(/^\(/) && line.length > 3 && line.length < 50 &&
                                           !line.includes('Open') && !line.includes('Closed') &&
                                           !line.includes('hours') && !line.includes('·')) {
                                    category = line;
                                }
                            }

                            const phoneMatch = allText.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
                            const phone = phoneMatch ? phoneMatch[0] : '';

                            results.push({ name, rating, reviewCount, address, category, phone });
                        }
                        return results;
                    }""")

                    count = 0
                    for data in listings_data:
                        raw_addr = data.get("address", "")
                        company = InsuranceCompany(
                            name=data["name"],
                            address=raw_addr,
                            city=extract_city_from_address(raw_addr) or region["name"].split("/")[0].strip(),
                            zip_code=extract_zip(raw_addr),
                            phone=data.get("phone", ""),
                            rating=data.get("rating", ""),
                            review_count=data.get("reviewCount", ""),
                            categories=data.get("category", ""),
                            source="Google Maps",
                            source_url=url,
                        )
                        results.append(company)
                        count += 1

                    total_found += count
                    print(f"    Found {count} listings")

                except Exception as e:
                    print(f"    Error: {e}")
                    continue

                polite_sleep(2, 4)

        browser.close()

    print(f"  Google Maps total: {total_found} listings")


# ---------------------------------------------------------------------------
# Source 2: Yelp (via Playwright — requests gets 403'd by Cloudflare)
# ---------------------------------------------------------------------------

def _parse_yelp_html(html: str, location: str, url: str) -> list[InsuranceCompany]:
    """Extract businesses from Yelp page HTML using JSON-LD / __NEXT_DATA__ / CSS."""
    companies: list[InsuranceCompany] = []
    soup = BeautifulSoup(html, "lxml")

    # Strategy 1: JSON-LD structured data
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string)
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and data.get("@type") == "ItemList":
                items = data.get("itemListElement", [])
            else:
                continue

            for item in items:
                biz = item.get("item", item) if "item" in item else item
                name = biz.get("name", "")
                if not name:
                    continue

                addr_obj = biz.get("address", {})
                if isinstance(addr_obj, dict):
                    street = addr_obj.get("streetAddress", "")
                    city = addr_obj.get("addressLocality", "")
                    state = addr_obj.get("addressRegion", "CA")
                    zipcode = addr_obj.get("postalCode", "")
                    full_addr = f"{street}, {city}, {state} {zipcode}".strip(", ")
                else:
                    full_addr = str(addr_obj)
                    city = extract_city_from_address(full_addr)
                    state = "CA"
                    zipcode = extract_zip(full_addr)

                agg = biz.get("aggregateRating", {}) or {}
                companies.append(InsuranceCompany(
                    name=name,
                    address=full_addr,
                    city=city if isinstance(addr_obj, dict) else extract_city_from_address(full_addr),
                    state=state if isinstance(addr_obj, dict) else "CA",
                    zip_code=zipcode if isinstance(addr_obj, dict) else extract_zip(full_addr),
                    phone=biz.get("telephone", ""),
                    website=biz.get("url", ""),
                    rating=str(agg.get("ratingValue", "")),
                    review_count=str(agg.get("reviewCount", "")),
                    source="Yelp",
                    source_url=url,
                ))
        except (json.JSONDecodeError, TypeError):
            continue

    if companies:
        return companies

    # Strategy 2: __NEXT_DATA__ JSON blob
    nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if nd_tag and nd_tag.string:
        try:
            nd = json.loads(nd_tag.string)
            props = nd.get("props", {}).get("pageProps", {})
            components = (
                props.get("searchPageProps", {})
                .get("mainContentComponentsListProps", [])
            )
            for comp in components:
                biz = comp.get("searchResultBusiness") or comp.get("bizCardProps") or {}
                name = biz.get("name") or biz.get("businessName", "")
                if not name:
                    continue

                c = InsuranceCompany(
                    name=name,
                    phone=biz.get("phone", ""),
                    rating=str(biz.get("rating", "")),
                    review_count=str(biz.get("reviewCount", "")),
                    categories=", ".join(
                        cat.get("title", "") for cat in biz.get("categories", [])
                    ),
                    city=location.split(",")[0],
                    source="Yelp",
                    source_url=url,
                )
                addr_props = biz.get("addressProps") or biz.get("address") or {}
                if isinstance(addr_props, dict):
                    parts = [addr_props.get("addressLine1", ""), addr_props.get("addressLine2", "")]
                    c.address = ", ".join(p for p in parts if p)
                    c.city = addr_props.get("city", location.split(",")[0])
                    c.zip_code = addr_props.get("postalCode", "")
                companies.append(c)
        except (json.JSONDecodeError, TypeError, KeyError):
            pass

    return companies


def scrape_yelp(results: list[InsuranceCompany]):
    """Scrape Yelp for commercial insurance companies using Playwright."""
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError:
        print("[!] Playwright not installed — skipping Yelp source.")
        return

    print("\n" + "=" * 60)
    print("SOURCE 2: Yelp")
    print("=" * 60)

    yelp_queries = [
        "commercial insurance",
        "business insurance",
        "commercial insurance broker",
        "insurance agency",
        "commercial insurance agent",
    ]

    yelp_locations = [
        "San Francisco, CA",
        "Oakland, CA",
        "San Jose, CA",
        "San Mateo, CA",
        "Palo Alto, CA",
        "Walnut Creek, CA",
        "Fremont, CA",
        "Berkeley, CA",
        "Redwood City, CA",
        "Pleasanton, CA",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        # Warm up — visit homepage first
        try:
            page.goto("https://www.yelp.com/", wait_until="domcontentloaded", timeout=15000)
            polite_sleep(2, 4)
        except Exception:
            pass

        for location in yelp_locations:
            for query in yelp_queries:
                for page_num in range(3):
                    offset = page_num * 10
                    url = (
                        f"https://www.yelp.com/search?"
                        f"find_desc={quote_plus(query)}"
                        f"&find_loc={quote_plus(location)}"
                        f"&start={offset}"
                    )

                    print(f"  [{location}] {query} (page {page_num + 1})")
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=20000)
                        polite_sleep(2, 4)

                        html = page.content()
                        found = _parse_yelp_html(html, location, url)
                        results.extend(found)
                        print(f"    Found {len(found)} listings")

                    except Exception as e:
                        print(f"    Error: {e}")
                        continue

                    polite_sleep(3, 7)

        browser.close()

    print(f"  Yelp total: {len([r for r in results if r.source == 'Yelp'])} listings")


# ---------------------------------------------------------------------------
# Source 3: Yellow Pages (via Playwright — Cloudflare blocks requests)
# ---------------------------------------------------------------------------

def scrape_yellowpages(results: list[InsuranceCompany]):
    """Scrape Yellow Pages for commercial insurance companies using Playwright."""
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError:
        print("[!] Playwright not installed — skipping Yellow Pages source.")
        return

    print("\n" + "=" * 60)
    print("SOURCE 3: Yellow Pages")
    print("=" * 60)

    yp_queries = [
        "commercial insurance",
        "business insurance",
        "insurance brokers",
        "commercial insurance agents",
        "workers compensation insurance",
        "general liability insurance",
    ]

    yp_locations = [
        "San Francisco, CA",
        "Oakland, CA",
        "San Jose, CA",
        "San Mateo, CA",
        "Palo Alto, CA",
        "Walnut Creek, CA",
        "Fremont, CA",
        "Berkeley, CA",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="America/Los_Angeles",
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        for location in yp_locations:
            for query in yp_queries:
                for page_num in range(1, 4):
                    url = (
                        f"https://www.yellowpages.com/search?"
                        f"search_terms={quote_plus(query)}"
                        f"&geo_location_terms={quote_plus(location)}"
                        f"&page={page_num}"
                    )

                    print(f"  [{location}] {query} (page {page_num})")
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=20000)
                        polite_sleep(2, 4)

                        html = page.content()
                        soup = BeautifulSoup(html, "lxml")

                        cards = soup.select(".result .info, .srp-listing .info, .organic .info")
                        for card in cards:
                            name_el = card.select_one(".business-name span, .n a, .business-name a")
                            phone_el = card.select_one(".phones.phone.primary, .phone")
                            addr_el = card.select_one(".adr, .address, .street-address")
                            website_el = card.select_one("a.track-visit-website, a[href*='website']")
                            cats = card.select(".categories a, .links a")

                            name = name_el.get_text(strip=True) if name_el else ""
                            if not name:
                                continue

                            raw_addr = addr_el.get_text(strip=True) if addr_el else ""

                            company = InsuranceCompany(
                                name=name,
                                address=raw_addr,
                                city=extract_city_from_address(raw_addr) or location.split(",")[0],
                                zip_code=extract_zip(raw_addr),
                                phone=phone_el.get_text(strip=True) if phone_el else "",
                                website=website_el["href"] if website_el and website_el.has_attr("href") else "",
                                categories=", ".join(c.get_text(strip=True) for c in cats),
                                source="Yellow Pages",
                                source_url=url,
                            )
                            results.append(company)

                        print(f"    Found {len(cards)} listings")

                    except Exception as e:
                        print(f"    Error: {e}")
                        continue

                    polite_sleep(2, 5)

        browser.close()

    print(f"  Yellow Pages total: {len([r for r in results if r.source == 'Yellow Pages'])} listings")


# ---------------------------------------------------------------------------
# Source 4: CDI Admitted Insurers PDF (official California registry)
# ---------------------------------------------------------------------------

def scrape_cdi_pdf(results: list[InsuranceCompany]):
    """Download and parse the CDI Admitted Insurers PDF for P&C companies."""
    print("\n" + "=" * 60)
    print("SOURCE 4: CDI Admitted Insurers PDF")
    print("=" * 60)

    pdf_url = "https://www.insurance.ca.gov/0250-insurers/0300-insurers/0100-applications/upload/AdmittedInsurers.pdf"

    try:
        resp = requests.get(pdf_url, timeout=30, headers=get_headers())
        if resp.status_code != 200:
            print(f"  Failed to download PDF: HTTP {resp.status_code}")
            return

        pdf_path = OUTPUT_DIR / "cdi_admitted_insurers.pdf"
        OUTPUT_DIR.mkdir(exist_ok=True)
        with open(pdf_path, "wb") as f:
            f.write(resp.content)
        print(f"  Downloaded PDF ({len(resp.content) // 1024} KB)")

        # Try to parse with pdfplumber if available
        try:
            import pdfplumber
        except ImportError:
            print("  pdfplumber not installed — extracting text with basic method")
            # Fallback: just save the PDF for manual review
            print(f"  PDF saved to: {pdf_path}")
            print("  Install pdfplumber for auto-parsing: pip install pdfplumber")
            return

        with pdfplumber.open(pdf_path) as pdf:
            all_text = ""
            for page in pdf.pages:
                all_text += page.extract_text() or ""

        # Format is: "COMPANY NAME NAIC_NUMBER" (number at end of line)
        lines = all_text.split("\n")
        for line in lines:
            line = line.strip()
            if not line or len(line) < 5:
                continue
            # Skip header lines
            if any(skip in line.upper() for skip in [
                "ADMITTED INSURERS", "COMPANY NAME", "PAGE ", "STATE OF",
                "SUBJECT TO", "NAIC NUMBER", "IRI-", "SUPPLEMENTAL"
            ]):
                continue

            # Strip trailing NAIC number (e.g., "ACE INSURANCE COMPANY 22667")
            naic_match = re.match(r"^(.+?)\s+(\d{4,6})\s*$", line)
            if naic_match:
                name = naic_match.group(1).strip()
                naic_code = naic_match.group(2)
            else:
                name = line.strip()
                naic_code = ""

            if not name or len(name) < 4:
                continue

            company = InsuranceCompany(
                name=name,
                state="CA",
                categories=f"NAIC: {naic_code}" if naic_code else "",
                source="CDI Admitted Insurers",
                source_url=pdf_url,
            )
            results.append(company)

        print(f"  CDI PDF total: {len([r for r in results if r.source == 'CDI Admitted Insurers'])} entries")

    except Exception as e:
        print(f"  Error processing CDI PDF: {e}")


# ---------------------------------------------------------------------------
# Website enrichment — visit each company's website for extra info
# ---------------------------------------------------------------------------

def enrich_from_websites(companies: list[InsuranceCompany]):
    """Visit company websites to extract email, description, extra info."""
    print("\n" + "=" * 60)
    print("ENRICHMENT: Visiting company websites")
    print("=" * 60)

    session = requests.Session()
    enriched = 0
    total = len([c for c in companies if c.website and c.website.startswith("http")])
    print(f"  {total} companies have websites to check")

    for i, company in enumerate(companies):
        if not company.website or not company.website.startswith("http"):
            continue

        if enriched >= 200:  # Cap to avoid excessive requests
            print("  Reached enrichment cap (200 sites)")
            break

        try:
            session.headers.update(get_headers())
            resp = session.get(company.website, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # Extract emails
            if not company.email:
                email_matches = re.findall(
                    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                    resp.text
                )
                # Filter out common non-business emails
                for em in email_matches:
                    if not any(x in em.lower() for x in ["example.com", "sentry", "webpack", "wixpress"]):
                        company.email = em
                        break

            # Extract meta description
            if not company.description:
                meta_desc = soup.find("meta", {"name": "description"})
                if meta_desc:
                    company.description = (meta_desc.get("content", "") or "")[:500]

            enriched += 1
            if enriched % 20 == 0:
                print(f"  Enriched {enriched}/{total}")

        except Exception:
            pass

        polite_sleep(1, 3)

    print(f"  Enrichment complete: {enriched} websites visited")


# ---------------------------------------------------------------------------
# Deduplication & output
# ---------------------------------------------------------------------------

def deduplicate(results: list[InsuranceCompany]) -> list[InsuranceCompany]:
    """Deduplicate by normalized name + city. Keep the record with most data."""
    seen: dict[str, InsuranceCompany] = {}

    for company in results:
        key = company.dedup_key
        if key in seen:
            existing = seen[key]
            # Merge: prefer the record with more fields populated
            for fld in ("address", "phone", "website", "email", "rating",
                        "review_count", "categories", "description", "zip_code"):
                existing_val = getattr(existing, fld, "")
                new_val = getattr(company, fld, "")
                if not existing_val and new_val:
                    setattr(existing, fld, new_val)
            # Append source info
            if company.source not in existing.source:
                existing.source += f", {company.source}"
        else:
            seen[key] = company

    return list(seen.values())


def save_results(companies: list[InsuranceCompany]):
    """Save results to CSV and JSON."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Sort by city, then name
    companies.sort(key=lambda c: (c.city.lower(), c.name.lower()))

    # CSV
    csv_path = OUTPUT_DIR / "sf_bay_area_commercial_insurance.csv"
    fieldnames = [
        "name", "address", "city", "state", "zip_code", "phone",
        "website", "email", "rating", "review_count", "categories",
        "description", "source", "source_url"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in companies:
            writer.writerow(asdict(c))
    print(f"  CSV saved: {csv_path} ({len(companies)} rows)")

    # JSON
    json_path = OUTPUT_DIR / "sf_bay_area_commercial_insurance.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([asdict(c) for c in companies], f, indent=2, ensure_ascii=False)
    print(f"  JSON saved: {json_path}")

    # Summary
    summary_path = OUTPUT_DIR / "scrape_summary.txt"
    cities = {}
    for c in companies:
        city = c.city or "Unknown"
        cities[city] = cities.get(city, 0) + 1

    sources = {}
    for c in companies:
        for src in c.source.split(", "):
            sources[src] = sources.get(src, 0) + 1

    with open(summary_path, "w") as f:
        f.write("SF Bay Area Commercial Insurance Scrape Summary\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Total unique companies: {len(companies)}\n\n")

        f.write("By Source:\n")
        for src, count in sorted(sources.items(), key=lambda x: -x[1]):
            f.write(f"  {src}: {count}\n")

        f.write("\nBy City:\n")
        for city, count in sorted(cities.items(), key=lambda x: -x[1]):
            f.write(f"  {city}: {count}\n")

        f.write(f"\nCompanies with phone: {sum(1 for c in companies if c.phone)}\n")
        f.write(f"Companies with website: {sum(1 for c in companies if c.website)}\n")
        f.write(f"Companies with email: {sum(1 for c in companies if c.email)}\n")
        f.write(f"Companies with rating: {sum(1 for c in companies if c.rating)}\n")

    print(f"  Summary saved: {summary_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("SF Bay Area Commercial Insurance Company Scraper")
    print("=" * 60)
    print(f"Searching {len(BAY_AREA_LOCATIONS)} cities across the Bay Area")
    print(f"Using {len(SEARCH_QUERIES)} search queries")
    print()

    all_results: list[InsuranceCompany] = []

    # Parse CLI args for which sources to run
    # Yelp and YP block headless browsers — use gmaps + cdi by default
    sources = sys.argv[1:] if len(sys.argv) > 1 else ["gmaps", "cdi"]

    if "gmaps" in sources:
        scrape_google_maps(all_results)

    if "yelp" in sources:
        scrape_yelp(all_results)

    if "yellowpages" in sources:
        scrape_yellowpages(all_results)

    if "cdi" in sources:
        scrape_cdi_pdf(all_results)

    print(f"\n{'=' * 60}")
    print(f"RAW RESULTS: {len(all_results)} total listings")
    print(f"{'=' * 60}")

    # Deduplicate
    unique = deduplicate(all_results)
    print(f"AFTER DEDUP: {unique_count} unique companies" if (unique_count := len(unique)) else "No results found")

    # Enrich from websites
    if "enrich" in sources or len(sys.argv) <= 1:
        enrich_from_websites(unique)

    # Save
    print(f"\n{'=' * 60}")
    print("SAVING RESULTS")
    print(f"{'=' * 60}")
    save_results(unique)

    print(f"\n{'=' * 60}")
    print("DONE!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
