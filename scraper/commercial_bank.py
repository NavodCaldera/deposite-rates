import pandas as pd
import re
import sys
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from .base import BaseScraper
from .utils import clean_rate # Only import what's needed

class CommercialBankScraper(BaseScraper):
    def __init__(self):
        super().__init__(
            name="Commercial Bank",
            institution_type="Bank",
            url='https://www.combank.lk/rates-tariff'
        )

    async def scrape(self) -> pd.DataFrame:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(self.url, wait_until='domcontentloaded', timeout=60000)
                fd_dropdown_locator = page.locator('a.expand-link:has-text("Fixed Deposits")')
                await fd_dropdown_locator.wait_for(state="visible", timeout=20000)
                await fd_dropdown_locator.click()
                table_selector = 'div.expand-block:has(a:has-text("Fixed Deposits")) table.with-radius'
                await page.wait_for_selector(table_selector, state='visible', timeout=15000)
                html_content = await page.content()
                await browser.close()

            soup = BeautifulSoup(html_content, 'lxml')
            fd_link = next((link for link in soup.find_all('a', class_='expand-link') if "Fixed Deposits" in link.get_text(separator=" ", strip=True)), None)
            if not (fd_link and (fd_parent_block := fd_link.find_parent('div', class_='expand-block')) and (table_element := fd_parent_block.find('table', class_='with-radius'))):
                return pd.DataFrame()

            data_rows = []
            for row in table_element.select('tbody > tr'):
                cells = row.find_all('td')
                if len(cells) != 4: continue
                try:
                    description_raw = cells[0].get_text(separator=" ", strip=True)
                    term_match = re.search(r'(\d+)\s*Months?', description_raw, re.IGNORECASE)
                    term_months = int(term_match.group(1)) if term_match else None
                    payout_schedule = 'Monthly' if 'monthly' in description_raw.lower() else 'Annually' if 'annually' in description_raw.lower() else 'At Maturity'
                    
                    data_rows.append({
                        'Bank Name': self.name, 
                        'FD Type': 'Standard',
                        'Institution Type': self.institution_type,
                        'Term (Months)': term_months,
                        'Payout Schedule': payout_schedule,
                        'Interest Rate (p.a.)': float(cells[1].get_text(strip=True)),
                        'Annual Effective Rate': float(cells[2].get_text(strip=True)),
                    })
                except (ValueError, IndexError, AttributeError): continue

            if not data_rows:
                return pd.DataFrame()
            return pd.DataFrame(data_rows)
        
        except Exception as e:
            print(f"--- FAILED: {self.name} scraper threw an error. --- \nError: {e}", file=sys.stderr)
            raise e