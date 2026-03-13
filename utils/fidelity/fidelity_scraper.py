
import argparse
import asyncio
import csv
import os
import random
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Page, Playwright

# --- Configuration ---
LOGIN_URL = "https://digital.fidelity.com/prgw/digital/login/full-page"
HOLDINGS_URL_TEMPLATE = "https://research2.fidelity.com/fidelity/screeners/etf/etfholdings.asp?symbol={symbol}&view=Holdings"
AUTH_FILE = Path("auth.json")
RESULTS_DIR = Path("results")
DELAY_SECONDS = 5
RANDOM_DELAY_MAX = 2

# --- Main Functions ---

async def get_authenticated_page(playwright: Playwright) -> Page:
    """Launches browser, handles login, and returns an authenticated page."""
    username = os.environ.get("FIDELITY_USERNAME")
    password = os.environ.get("FIDELITY_PASSWORD")

    browser = await playwright.chromium.launch(headless=False)
    # We create a fresh context each time, not loading from a file.
    context = await browser.new_context()
    page = await context.new_page()

    print(f"Navigating to login page: {LOGIN_URL}")
    await page.goto(LOGIN_URL)

    if username and password:
        print("Attempting to log in using credentials from environment variables...")
        try:
            # Fidelity's login is a multi-step form on the same page.
            await page.locator("#dom-username-input").fill(username)
            await page.locator("#dom-username-input").press("Enter")
            await page.locator("#dom-pswd-input").fill(password)
            await page.locator("#fs-login-button").click()
            print("Username and password submitted. Please complete 2FA in the browser if prompted.")
        except Exception as e:
            print(f"An error occurred while trying to auto-fill credentials: {e}")
            print("Please complete the login process manually.")
    else:
        print("Credentials not found in environment variables (FIDELITY_USERNAME, FIDELITY_PASSWORD).")
        print("Please complete the login and 2FA process manually in the browser window.")

    # Wait for a "Log Out" link to appear, which is a reliable indicator of a successful login.
    # Timeout is set to 20 minutes to allow for manual entry of credentials and 2FA.
    print("Waiting for login to complete... (check the browser window)")
    await page.get_by_role("link", name="Log Out").wait_for(timeout=3600000)

    print("Login successful. Saving authentication state for this session...")
    # This state is saved for use within this single script run (e.g., for multiple tickers).
    await context.storage_state(path=AUTH_FILE)

    return page

async def fetch_holdings(page: Page, symbol: str) -> list[dict]:
    """Navigates to the holdings page and scrapes the data."""
    print(f"Fetching holdings for {symbol}...")
    url = HOLDINGS_URL_TEMPLATE.format(symbol=symbol)
    await page.goto(url)
    await page.wait_for_load_state('domcontentloaded')

    # Wait for the table to be present
    try:
        await page.wait_for_selector("table.results-table", timeout=30000)
    except Exception:
        print(f"Could not find results table for {symbol}. It may be an invalid ticker or page structure changed.")
        return []

    html_content = await page.content()
    soup = BeautifulSoup(html_content, 'html.parser')

    table = soup.find('table', class_='results-table')
    if not table:
        print(f"No holdings table found for {symbol}.")
        return []

    holdings = []
    rows = table.find('tbody').find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            holdings.append({
                'Symbol': cols[0].text.strip(),
                'Company': cols[1].text.strip(),
                'Weight': cols[2].text.strip(),
            })
    print(f"Found {len(holdings)} holdings for {symbol}.")
    return holdings

def save_holdings_to_csv(symbol: str, holdings: list[dict]):
    """Saves a list of holdings to a CSV file."""
    if not holdings:
        return

    RESULTS_DIR.mkdir(exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    filename = RESULTS_DIR / f"fidelity_{symbol}_{date_str}.csv"

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Symbol', 'Company', 'Weight']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(holdings)

    print(f"Successfully saved holdings to {filename}")

async def main():
    """Main function to orchestrate the scraping process."""
    parser = argparse.ArgumentParser(description="Scrape ETF holdings from Fidelity.com.")
    parser.add_argument('tickers', nargs='+', help="One or more ETF tickers to scrape.")
    args = parser.parse_args()

    async with async_playwright() as p:
        page = None
        try:
            page = await get_authenticated_page(p)

            for i, ticker in enumerate(args.tickers):
                holdings = await fetch_holdings(page, ticker)
                save_holdings_to_csv(ticker, holdings)

                if i < len(args.tickers) - 1:
                    delay = DELAY_SECONDS + random.uniform(0, RANDOM_DELAY_MAX)
                    print(f"Waiting for {delay:.2f} seconds before next fetch...")
                    await asyncio.sleep(delay)

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            if page:
                await page.context.browser.close()

if __name__ == "__main__":
    print("Starting Fidelity ETF scraper.")
    print("Please ensure you have run 'pip install -r requirements.txt' and 'playwright install'")
    asyncio.run(main())
    print("Scraping process finished.")
