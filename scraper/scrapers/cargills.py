import pandas as pd
import httpx  # <-- CHANGED: Use httpx for async requests
import re
import sys 
from bs4 import BeautifulSoup
from ..base import BaseScraper
from ..utils import clean_rate, parse_term_to_months

class CargillsScraper(BaseScraper):
    """
    A 'child' scraper for Cargills Bank.
    It inherits all the logic from BaseScraper.
    """
    def __init__(self):
        # Call the parent's __init__ method to set up the name, type, and URL
        super().__init__(
            name="Cargills Bank",
            institution_type="Bank",
            url='https://www.cargillsbank.com/deposit-interest-rates'
        )

    # --- THIS FUNCTION IS NOW ASYNC ---
    async def scrape(self) -> pd.DataFrame:
        """
        This is the unique scraping logic for Cargills Bank.
        """
        try:
            # --- THIS BLOCK IS UPDATED FOR 'httpx' ---
            async with httpx.AsyncClient() as client:
                response = await client.get(self.url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
                response.raise_for_status()
                
            soup = BeautifulSoup(response.content, 'lxml')
            all_rates_data = []
            
            # --- Standard Rates ---
            standard_header = soup.find('p', string=re.compile(r'Fixed Deposits \(LKR\)', re.IGNORECASE))
            if standard_header and (standard_table := standard_header.find_next_sibling('table')):
                for row in standard_table.select('tbody > tr')[1:]:
                    cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
                    if len(cells) != 6: continue
                    term_months = parse_term_to_months(cells[0])
                    if not term_months: continue
                    
                    if rate := clean_rate(cells[1]): 
                        all_rates_data.append({'Bank Name': self.name, 'FD Type': 'Standard', 'Institution Type': self.institution_type, 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': clean_rate(cells[2])})
                    if rate := clean_rate(cells[3]): 
                        all_rates_data.append({'Bank Name': self.name, 'FD Type': 'Standard', 'Institution Type': self.institution_type, 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': clean_rate(cells[4])})
                    if rate := clean_rate(cells[5]): 
                        all_rates_data.append({'Bank Name': self.name, 'FD Type': 'Standard', 'Institution Type': self.institution_type, 'Term (Months)': term_months, 'Payout Schedule': 'Annually', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': None})

            # --- Senior Rates ---
            senior_header = soup.find('p', string=re.compile(r'Senior Citizen Fixed Deposits', re.IGNORECASE))
            if senior_header and (senior_table := senior_header.find_next_sibling('table')):
                for row in senior_table.select('tbody > tr')[1:]:
                    cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
                    if len(cells) != 3: continue
                    term_months = parse_term_to_months(cells[0])
                    if not term_months: continue
                    
                    if rate := clean_rate(cells[1]): 
                        all_rates_data.append({'Bank Name': self.name, 'FD Type': 'Senior Citizen', 'Institution Type': self.institution_type, 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': None})
                    if rate := clean_rate(cells[2]): 
                        all_rates_data.append({'Bank Name': self.name, 'FD Type': 'Senior Citizen', 'Institution Type': self.institution_type, 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': None})

            if not all_rates_data:
                print(f"--- FAILED: {self.name} - No data extracted.")
                return pd.DataFrame()
            
            print(f"--- SUCCESS: {self.name} extracted {len(all_rates_data)} records.")
            return pd.DataFrame(all_rates_data)

        except Exception as e:
            print(f"--- FAILED: {self.name} scraper threw an error. --- \nError: {e}", file=sys.stderr)
            # Re-raise the exception so the orchestrator (run_all.py) can catch it
            raise e