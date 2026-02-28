#!/bin/bash
# SF Bay Area Commercial Insurance Scraper Runner
# ================================================
#
# Usage:
#   ./run.sh              # Run all sources (gmaps + yelp + yellowpages + cdi + enrich)
#   ./run.sh yelp         # Run only Yelp
#   ./run.sh yellowpages  # Run only Yellow Pages
#   ./run.sh gmaps        # Run only Google Maps
#   ./run.sh cdi          # Run only CDI Admitted Insurers PDF
#   ./run.sh yelp yellowpages  # Run Yelp + Yellow Pages only
#
# Results saved to ./output/

cd "$(dirname "$0")"

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    playwright install chromium
else
    source venv/bin/activate
fi

echo ""
echo "Starting scraper..."
echo ""

python3 scraper.py "$@"

echo ""
echo "Results are in ./output/"
