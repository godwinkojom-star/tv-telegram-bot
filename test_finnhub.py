"""
test_finnhub.py - ONE-OFF, standalone test. NOT part of the bot.

Run this manually in Wispbyte's console:
    python test_finnhub.py

Only calls Finnhub's economic calendar endpoint and only prints the
fields the News Engine plan actually needs: event name, time, country/
currency, impact level, previous/estimate/actual. Nothing else Finnhub
offers (stock quotes, news sentiment, etc.) is touched.

Never modifies the database, signals, the SMC strategy, app.py, or
news_engine.py. Delete this file once you're done testing.
"""

import os
import requests
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ.get("FINNHUB_API_KEY")

if not API_KEY:
    print("FINNHUB_API_KEY is not set in the environment. Nothing to test.")
    raise SystemExit(1)

today = datetime.utcnow().date()
week_from_now = today + timedelta(days=7)

url = "https://finnhub.io/api/v1/calendar/economic"
params = {
    "from": today.isoformat(),
    "to": week_from_now.isoformat(),
    "token": API_KEY,
}

print(f"Requesting Finnhub economic calendar: {today} to {week_from_now}...")
resp = requests.get(url, params=params, timeout=15)

print(f"HTTP status: {resp.status_code}\n")

if resp.status_code in (401, 402, 403):
    print("This endpoint may not be included in your current Finnhub plan.")
    print("Response body:")
    print(resp.text[:1000])
    raise SystemExit(0)

if resp.status_code != 200:
    print("Request failed. Response body:")
    print(resp.text[:1000])
    raise SystemExit(1)

data = resp.json()
events = data.get("economicCalendar", [])

if not isinstance(events, list):
    print("Unexpected response shape (no 'economicCalendar' list found). Raw response:")
    print(data)
    raise SystemExit(1)

print(f"Total events returned (all countries): {len(events)}\n")

# Finnhub gives country codes, not currency codes directly - this maps
# the ones we actually care about per the plan (USD/EUR/GBP/JPY).
WANTED_COUNTRIES = {"US": "USD", "EU": "EUR", "GB": "GBP", "JP": "JPY"}
relevant = [e for e in events if e.get("country") in WANTED_COUNTRIES]

print(f"Events for US/EU/GB/JP: {len(relevant)}\n")
print("-" * 60)

for e in relevant[:25]:  # cap the printout so it's readable
    currency = WANTED_COUNTRIES.get(e.get("country"), e.get("country"))
    print(f"Event:    {e.get('event')}")
    print(f"Country:  {e.get('country')} ({currency})")
    print(f"Time:     {e.get('time')}")
    print(f"Impact:   {e.get('impact')}")
    print(f"Previous: {e.get('prev')}")
    print(f"Estimate: {e.get('estimate')}")
    print(f"Actual:   {e.get('actual')}")
    print("-" * 60)

if len(relevant) > 25:
    print(f"...and {len(relevant) - 25} more (truncated for readability).")

print("\nDone. If this looks right, tell Claude what you saw and we'll")
print("decide the next step. Delete this file when you're finished.")
