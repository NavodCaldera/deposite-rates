import pandas as pd
import httpx 
import re
import sys 
from bs4 import BeautifulSoup
from .base import BaseScraper                # <-- FIXED: Now a simple relative import
from .utils import clean_rate, parse_term_to_months # <-- FIXED: Now a simple relative import

class CargillsScraper(BaseScraper):
    def __init__(self):
        super().__init__(
            name="Cargills Bank",
            institution_type="Bank",
            url='https://www.cargillsbank.com/deposit-interest-rates'
        )

    async def scrape(self) -> pd.DataFrame:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
                response.raise_for_status()
                
            soup = BeautifulSoup(response.content, 'lxml')
            all_rates_data = []
            
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
            raise e