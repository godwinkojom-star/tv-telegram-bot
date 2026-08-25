"""
test_fmp.py - ONE-OFF, standalone test. NOT part of the bot.

Run this manually in Wispbyte's console:
    python test_fmp.py

It does nothing except read FMP_API_KEY from the environment (already
set on Wispbyte) and print what Financial Modeling Prep's economic
calendar endpoint returns for USD/EUR/GBP/JPY over the next 7 days.

Delete this file when you're done testing - it's not imported by
app.py or news_engine.py, and nothing else depends on it.
"""

import os
import requests
from datetime import datetime, timedelta

# Wispbyte writes environment variables to a plain .env file on disk
# rather than injecting them directly into the process - without this,
# os.environ.get("FMP_API_KEY") would come back empty even though the
# key really is saved there. Same fix app.py already needed.
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ.get("FMP_API_KEY")

if not API_KEY:
    print("FMP_API_KEY is not set in the environment. Nothing to test.")
    raise SystemExit(1)

today = datetime.utcnow().date()
week_from_now = today + timedelta(days=7)

# Current stable endpoint per FMP's own docs (the older /api/v3/
# path is deprecated).
url = "https://financialmodelingprep.com/stable/economic-calendar"
params = {
    "from": today.isoformat(),
    "to": week_from_now.isoformat(),
    "apikey": API_KEY,
}

print(f"Requesting FMP economic calendar: {today} to {week_from_now}...")
resp = requests.get(url, params=params, timeout=15)

print(f"HTTP status: {resp.status_code}\n")

# A free-plan account not covering this endpoint often shows up as a
# 401/402/403 with an explanatory message rather than a normal error -
# this is useful information either way, so print it plainly instead
# of just crashing.
if resp.status_code in (401, 402, 403):
    print("This endpoint may not be included in your current FMP plan.")
    print("Response body:")
    print(resp.text[:1000])
    raise SystemExit(0)

if resp.status_code != 200:
    print("Request failed. Response body:")
    print(resp.text[:1000])
    raise SystemExit(1)

data = resp.json()

if not isinstance(data, list):
    print("Unexpected response shape (not a list). Raw response:")
    print(data)
    raise SystemExit(1)

print(f"Total events returned (all countries): {len(data)}\n")

WANTED_CURRENCIES = {"USD", "EUR", "GBP", "JPY"}
relevant = [e for e in data if e.get("currency") in WANTED_CURRENCIES]

print(f"Events for USD/EUR/GBP/JPY: {len(relevant)}\n")
print("-" * 60)

for e in relevant[:25]:  # cap the printout so it's readable
    print(f"Event:    {e.get('event')}")
    print(f"Country:  {e.get('country')} ({e.get('currency')})")
    print(f"Time:     {e.get('date')}")
    print(f"Impact:   {e.get('impact')}")
    print(f"Previous: {e.get('previous')}")
    print(f"Estimate: {e.get('estimate')}")
    print(f"Actual:   {e.get('actual')}")
    print("-" * 60)

if len(relevant) > 25:
    print(f"...and {len(relevant) - 25} more (truncated for readability).")

print("\nDone. If this looks right, tell Claude what you saw and we'll")
print("decide the next step. Delete this file when you're finished.")
