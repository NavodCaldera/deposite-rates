import pandas as pd
import requests
import sys 
from bs4 import BeautifulSoup
from scraper.base import BaseScraper
from scraper.utils import clean_rate, parse_term_to_months 

class AllianceScraper(BaseScraper):
    """
    A 'child' scraper for Alliance Finance.
    """
    def __init__(self):
        super().__init__(
            name="Alliance Finance",
            institution_type="Finance Company",
            url='https://www.alliancefinance.lk/investments/fixed-deposits/'
        )

    def scrape(self) -> pd.DataFrame:
        """
        This is the unique scraping logic for Alliance Finance.
        """
        try:
            response = requests.get(self.url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            table = soup.find('table', id='tablepress-1')
            
            if not table:
                print(f"--- FAILED: {self.name} - Could not find rates table.")
                return pd.DataFrame()

            all_rates_data = []
            for row in table.select('tbody > tr'):
                cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
                if len(cells) != 5: continue
                
                term_months = parse_term_to_months(cells[0])
                if not term_months: continue
                
                if monthly_rate := clean_rate(cells[1]):
                    all_rates_data.append({'Bank Name': self.name, 'FD Type': 'Standard', 'Institution Type': self.institution_type, 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
                
                if maturity_rate := clean_rate(cells[3]):
                    all_rates_data.append({'Bank Name': self.name, 'FD Type': 'Standard', 'Institution Type': self.institution_type, 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
            
            if not all_rates_data:
                print(f"--- FAILED: {self.name} - No data extracted.")
                return pd.DataFrame()
            
            print(f"--- SUCCESS: {self.name} extracted {len(all_rates_data)} records.")
            return pd.DataFrame(all_rates_data)

        except Exception as e:
            print(f"--- FAILED: {self.name} scraper threw an error. --- \nError: {e}", file=sys.stderr)
            raise e

            