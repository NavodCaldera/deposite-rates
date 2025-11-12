import asyncio
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import sys
import os 
from supabase import create_client, Client 

# --- SUPABASE UPLOAD FUNCTION ---

async def update_supabase_for_institution(supabase_client: Client, df: pd.DataFrame, institution_name: str):
    """
    Atomically updates Supabase for a single institution.
    1. Deletes all old records for that bank.
    2. Inserts all new records from the dataframe.
    """
    if not institution_name or df.empty:
        print(f"--- FAILED (Upload): Invalid data for {institution_name}. Skipping upload.")
        return

    print(f"    - Starting atomic update for: '{institution_name}'")
    
    try:
        # 1. Standardize and clean the DataFrame
        records = clean_and_rename_df(df).to_dict('records')
        
        # 2. Delete all existing records for this bank
        print(f"    - Deleting old records for '{institution_name}'...")
        delete_response = await asyncio.to_thread(
            supabase_client.from_("public-rates")
            .delete()
            .eq("bankName", institution_name)
            .execute
        )
        if delete_response.data:
            print(f"    - Deleted {len(delete_response.data)} old records.")
        else:
             print(f"    - No old records found for '{institution_name}'.")

        # 3. Insert all new records
        print(f"    - Inserting {len(records)} new records for '{institution_name}'...")
        insert_response = await asyncio.to_thread(
            supabase_client.from_("public-rates")
            .insert(records)
            .execute
        )

        if len(insert_response.data) == len(records):
            print(f"    - SUCCESS (Upload): '{institution_name}' -> Atomically updated {len(records)} records.")
        else:
            print(f"--- WARNING (Upload): Mismatch for '{institution_name}'. Expected {len(records)}, inserted {len(insert_response.data)}.")
            
    except Exception as e:
        print(f"--- FAILED (Upload): Batch commit failed for '{institution_name}'. Error: {e}", file=sys.stderr)

# --- THIS FUNCTION IS UPDATED ---
def clean_and_rename_df(df):
    """
    Standardizes the DataFrame before it's uploaded to Supabase.
    """
    df_renamed = df.rename(columns={
        'Bank Name': 'bankName',
        'FD Type': 'fdType',
        'Institution Type': 'institutionType', # <-- ADDED
        'Term (Months)': 'termMonths',
        'Payout Schedule': 'payoutSchedule',
        'Interest Rate (p.a.)': 'interestRate',
        'Annual Effective Rate': 'aer'
    })
    
    # Ensure all required columns exist, add if missing
    required_cols = ['bankName', 'fdType', 'institutionType', 'termMonths', 'payoutSchedule', 'interestRate', 'aer'] # <-- ADDED 'institutionType'
    for col in required_cols:
        if col not in df_renamed.columns:
            df_renamed[col] = None
            
    # Select only the required columns and handle NaN/NaT
    df_final = df_renamed[required_cols]
    df_final = df_final.where(pd.notna(df_final), None)
    return df_final.sort_values(by=['bankName', 'termMonths']).reset_index(drop=True)


# ==============================================================================
# --- BANK SCRAPERS (9 TOTAL) ---
# ==============================================================================
def scrape_cargills_bank_fd_rates():
    """Scrapes FD rates from the Cargills Bank website."""
    
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None

    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            term_text = term_text.lower()
            if 'year' in term_text:
                match = re.search(r'(\d+)', term_text)
                return int(match.group(1)) * 12 if match else None
            elif 'month' in term_text:
                match = re.search(r'(\d+)', term_text)
                return int(match.group(1)) if match else None
        return None
        
    url = 'https://www.cargillsbank.com/deposit-interest-rates'
    print(f"--- Starting: Cargills Bank ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
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
                if rate := clean_rate(cells[1]): all_rates_data.append({'Bank Name': 'Cargills Bank', 'FD Type': 'Standard', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': clean_rate(cells[2])})
                if rate := clean_rate(cells[3]): all_rates_data.append({'Bank Name': 'Cargills Bank', 'FD Type': 'Standard', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': clean_rate(cells[4])})
                if rate := clean_rate(cells[5]): all_rates_data.append({'Bank Name': 'Cargills Bank', 'FD Type': 'Standard', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'Annually', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': None})

        senior_header = soup.find('p', string=re.compile(r'Senior Citizen Fixed Deposits', re.IGNORECASE))
        if senior_header and (senior_table := senior_header.find_next_sibling('table')):
            for row in senior_table.select('tbody > tr')[1:]:
                cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
                if len(cells) != 3: continue
                term_months = parse_term_to_months(cells[0])
                if not term_months: continue
                if rate := clean_rate(cells[1]): all_rates_data.append({'Bank Name': 'Cargills Bank', 'FD Type': 'Senior Citizen', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': None})
                if rate := clean_rate(cells[2]): all_rates_data.append({'Bank Name': 'Cargills Bank', 'FD Type': 'Senior Citizen', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': None})

        if not all_rates_data:
            print("--- FAILED: Cargills Bank - No data extracted.")
            return None
        
        print(f"--- SUCCESS: Cargills Bank extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)

    except Exception as e:
        print(f"--- FAILED: Cargills Bank scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

async def scrape_commercial_bank_fd_rates():
    """Scrapes Commercial Bank's Fixed Deposit rates using Playwright."""
    url = 'https://www.combank.lk/rates-tariff'
    print("--- Starting: Commercial Bank ---")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            fd_dropdown_locator = page.locator('a.expand-link:has-text("Fixed Deposits")')
            await fd_dropdown_locator.wait_for(state="visible", timeout=20000)
            await fd_dropdown_locator.click()
            table_selector = 'div.expand-block:has(a:has-text("Fixed Deposits")) table.with-radius'
            await page.wait_for_selector(table_selector, state='visible', timeout=15000)
            html_content = await page.content()
            await browser.close()
    except Exception as e:
        print(f"--- FAILED: Commercial Bank scraper threw an error during browser automation. --- \nError: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    fd_link = next((link for link in soup.find_all('a', class_='expand-link') if "Fixed Deposits" in link.get_text(separator=" ", strip=True)), None)
    if not (fd_link and (fd_parent_block := fd_link.find_parent('div', class_='expand-block')) and (table_element := fd_parent_block.find('table', class_='with-radius'))):
        print("--- FAILED: Commercial Bank - Could not parse the rates table from HTML.")
        return None

    data_rows = []
    for row in table_element.select('tbody > tr'):
        cells = row.find_all('td')
        if len(cells) != 4: continue
        try:
            description_raw = cells[0].get_text(separator=" ", strip=True)
            term_match = re.search(r'(\d+)\s*Months?', description_raw, re.IGNORECASE)
            payout_schedule = 'Monthly' if 'monthly' in description_raw.lower() else 'Annually' if 'annually' in description_raw.lower() else 'At Maturity'
            data_rows.append({
                'Bank Name': 'Commercial Bank', 
                'FD Type': 'Standard',
                'Institution Type': 'Bank',
                'Term (Months)': int(term_match.group(1)) if term_match else None,
                'Payout Schedule': payout_schedule,
                'Interest Rate (p.a.)': float(cells[1].get_text(strip=True)),
                'Annual Effective Rate': float(cells[2].get_text(strip=True)),
            })
        except (ValueError, IndexError): continue

    if not data_rows:
        print("--- FAILED: Commercial Bank - No data rows extracted.")
        return None
        
    print(f"--- SUCCESS: Commercial Bank extracted {len(data_rows)} records.")
    return pd.DataFrame(data_rows)

def scrape_dfcc_fd_rates_final():
    """Scrapes FD rates from the DFCC Bank website."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '']:
            try: return float(rate_text.replace('%', '').strip())
            except ValueError: return None
        return None

    def parse_term_to_months(term_text):
        match = re.search(r'(\d+)\s*(Month|Year)s?', term_text, re.IGNORECASE)
        if match:
            value, unit = int(match.group(1)), match.group(2).lower()
            return value if unit == 'month' else value * 12
        return None

    def process_dfcc_table(table):
        data = []
        all_rows_in_body = table.select('tbody > tr')
        if len(all_rows_in_body) < 2: return data
        
        headers = [th.get_text(strip=True) for th in all_rows_in_body[0].find_all('th')]
        data_rows = all_rows_in_body[1:]

        for i in range(0, len(data_rows), 2):
            if i + 1 >= len(data_rows): continue
            rate_row_cells = data_rows[i].find_all('td')
            aer_row_cells = data_rows[i+1].find_all('td')
            if not rate_row_cells or not aer_row_cells: continue

            payout_schedule = 'At Maturity' if 'Nominal' in rate_row_cells[0].get_text(strip=True) else rate_row_cells[0].get_text(strip=True)

            for col_index in range(1, len(headers)):
                term_months = parse_term_to_months(headers[col_index])
                if not term_months: continue
                if col_index < len(rate_row_cells) and col_index < len(aer_row_cells):
                    rate = clean_rate(rate_row_cells[col_index].get_text())
                    aer = clean_rate(aer_row_cells[col_index].get_text())
                    if rate is not None:
                        data.append({
                            'Bank Name': 'DFCC Bank', 
                            'FD Type': 'Standard',
                            'Institution Type': 'Bank',
                            'Term (Months)': term_months, 
                            'Payout Schedule': payout_schedule,
                            'Interest Rate (p.a.)': rate, 
                            'Annual Effective Rate': aer
                        })
        return data

    url = 'https://www.dfcc.lk/interest-rates/fd-rates/'
    print(f"--- Starting: DFCC Bank ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        main_content = soup.find('div', id='ratest-tab-04')
        if not main_content:
            print("--- FAILED: DFCC Bank - Could not find main content container.")
            return None

        all_rates = []
        for header in main_content.find_all('h3'):
            if "FD Rates" in header.get_text(strip=True):
                if table := header.find_next('table'):
                    all_rates.extend(process_dfcc_table(table))
        
        if not all_rates:
            print("--- FAILED: DFCC Bank - No data extracted.")
            return None
            
        print(f"--- SUCCESS: DFCC Bank extracted {len(all_rates)} records.")
        return pd.DataFrame(all_rates)

    except Exception as e:
        print(f"--- FAILED: DFCC Bank scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

async def scrape_hnb_fd_rates_final():
    """Scrapes HNB Fixed Deposit rates using Playwright."""
    url = 'https://www.hnb.lk/interest-rates'
    print("--- Starting: Hatton National Bank (HNB) ---")
    
    def clean_rate(text):
        if text and text.strip() != '-':
            try: return float(text.replace('%', '').strip())
            except (ValueError, TypeError): return None
        return None

    def parse_term(text):
        match = re.search(r'(\d+)\s*Month', text, re.IGNORECASE)
        return int(match.group(1)) if match else None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            sidebar = page.locator('nav.grid')
            fd_link_locator = sidebar.get_by_text("Fixed Deposits", exact=True)
            await fd_link_locator.wait_for(state="visible", timeout=20000)
            await fd_link_locator.click()
            await page.wait_for_selector('h2:has-text("Fixed Deposits Interest Rates")', state='visible', timeout=15000)
            html_content = await page.content()
            await browser.close()
    except Exception as e:
        print(f"--- FAILED: HNB scraper threw an error during browser automation. --- \nError: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    all_rates_data = []
    content_area = soup.find('div', class_=lambda c: 'w-3/4' in (c or ''))
    if not content_area:
        print("--- FAILED: HNB - Could not find main content area.")
        return None

    std_header = content_area.find('h2', string='Fixed Deposits Interest Rates')
    if std_header and (std_table := std_header.find_next_sibling('table')):
        headers = [th.get_text(strip=True) for th in std_table.find_all('th')]
        for row in std_table.select('tbody > tr'):
            cells = row.find_all('td')
            if not cells or not (term_months := parse_term(cells[0].get_text(strip=True))): continue
            aer = clean_rate(cells[-1].get_text())
            for i, schedule in enumerate(headers[1:-1]):
                if rate := clean_rate(cells[i + 1].get_text()):
                    all_rates_data.append({'Bank Name': 'Hatton National Bank (HNB)', 'FD Type': 'Standard', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': schedule.title(), 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': aer})
    
    if not all_rates_data:
        print("--- FAILED: HNB - No data extracted.")
        return None
        
    print(f"--- SUCCESS: HNB extracted {len(all_rates_data)} records.")
    return pd.DataFrame(all_rates_data)

def scrape_nsb_fd_rates():
    """Scrapes NSB's Rupee Term Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip():
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None

    def parse_term_from_details(details_text):
        match = re.search(r'(\d+)\s+Months?', details_text, re.IGNORECASE)
        return int(match.group(1)) if match else None

    url = 'https://www.nsb.lk/rates-tarriffs/rupee-deposit-rates/'
    print(f"--- Starting: National Savings Bank (NSB) ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        header = soup.find('a', string=re.compile(r'Term Deposits'))
        if not header or not (card_div := header.find_parent('div', class_='card')) or not (table := card_div.find('table')):
            print("--- FAILED: NSB - Could not find the rates table.")
            return None

        all_rates = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 6 or 'Endowment' in cells[0]: continue
            if not (term_months := parse_term_from_details(cells[0])): continue
            if interest_rate := clean_rate(cells[2]):
                effective_rate = clean_rate(cells[5])
                all_rates.append({
                    'Bank Name': 'National Savings Bank (NSB)', 
                    'FD Type': 'Standard',
                    'Institution Type': 'Bank',
                    'Term (Months)': term_months, 
                    'Payout Schedule': "At Maturity" if "Maturity" in cells[4] else "Monthly",
                    'Interest Rate (p.a.)': interest_rate,
                    'Annual Effective Rate': effective_rate if effective_rate else interest_rate
                })
        
        if not all_rates:
            print("--- FAILED: NSB - No data was extracted.")
            return None
            
        print(f"--- SUCCESS: NSB extracted {len(all_rates)} records.")
        return pd.DataFrame(all_rates)

    except Exception as e:
        print(f"--- FAILED: NSB scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_ntb_fd_rates_final():
    """Scrapes Fixed Deposit rates for LKR from the NTB website."""
    def parse_rate_and_aer(cell_content):
        text = cell_content.decode_contents(formatter="html").replace('<br/>', ' ').replace('<br>', ' ').strip()
        numbers = re.findall(r'[\d.]+', text)
        if not numbers: return None, None
        rate = float(numbers[0])
        aer = float(numbers[1]) if len(numbers) > 1 else rate
        return rate, aer

    def parse_term_to_months(header_text):
        match = re.search(r'(\d+)\s*month', header_text, re.IGNORECASE)
        return int(match.group(1)) if match else None

    url = 'https://www.nationstrust.com/deposit-rates'
    print(f"--- Starting: Nations Trust Bank (NTB) ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        fd_header = soup.find('div', class_='section_heading', string='Fixed Deposit Rates')
        if not fd_header or not (table := fd_header.find_next('table')):
            print("--- FAILED: NTB - Could not find the rates table.")
            return None

        all_rates_data = []
        headers = table.find('thead').find_all('th')
        term_months = [parse_term_to_months(h.get_text(strip=True)) for h in headers[2:]]
        current_currency = ''
        
        for row in table.select('tbody > tr'):
            cells = row.find_all('td')
            if not cells or len(cells) < len(headers): continue
            if currency_in_cell := cells[0].get_text(strip=True): current_currency = currency_in_cell
            if current_currency != 'LKR': continue
            
            deposit_type_text = cells[1].get_text(strip=True)
            if 'Maturity' in deposit_type_text: payout_schedule = 'At Maturity'
            elif 'Monthly' in deposit_type_text: payout_schedule = 'Monthly'
            elif 'Annually' in deposit_type_text: payout_schedule = 'Annually'
            else: continue
                    
            for i, cell in enumerate(cells[2:]):
                rate, aer = parse_rate_and_aer(cell)
                if rate is not None and i < len(term_months) and term_months[i] is not None:
                    all_rates_data.append({'Bank Name': 'Nations Trust Bank', 'FD Type': 'Standard', 'Institution Type': 'Bank', 'Term (Months)': term_months[i], 'Payout Schedule': payout_schedule, 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': aer})

        if not all_rates_data:
            print("--- FAILED: NTB - No LKR rate data was extracted.")
            return None

        print(f"--- SUCCESS: NTB extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)

    except Exception as e:
        print(f"--- FAILED: NTB scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

async def scrape_pan_asia_fd_rates_final():
    """Scrapes Pan Asia Bank rates using Playwright."""
    def clean_rate(text):
        if isinstance(text, str) and text.strip():
            match = re.search(r'([\d.]+)', text)
            return float(match.group(1)) if match else None
        return None

    def parse_term_to_months(text):
        if isinstance(text, str):
            match = re.search(r'(\d+)\s*Month', text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None

    def process_pan_asia_table(table, fd_type):
        records = []
        if not (thead := table.find('thead')) or not (header_row := thead.find('tr')): return []
        terms = [td.get_text(strip=True) for td in header_row.find_all(['th', 'td'])]
        body_rows = table.select('tbody > tr')
        if len(body_rows) < 3: return []
        
        payout_schedules = [td.get_text(strip=True) for td in body_rows[0].find_all(['th', 'td'])]
        interest_rates = [td.get_text(strip=True) for td in body_rows[1].find_all(['th', 'td'])]
        aer_rates = [td.get_text(strip=True) for td in body_rows[2].find_all(['th', 'td'])]

        for i in range(1, len(terms)):
            if not (term_months := parse_term_to_months(terms[i])): continue
            if rate := clean_rate(interest_rates[i]):
                records.append({'Bank Name': 'Pan Asia Bank', 'FD Type': 'Standard', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': payout_schedules[i], 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': clean_rate(aer_rates[i])})
        return records

    url = 'https://www.pabcbank.com/personal-banking/savings-investments/fixed-deposits/general-fixed-deposits/'
    print(f"--- Starting: Pan Asia Bank ---")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await page.wait_for_selector('h2#cRate', state='visible', timeout=20000)
            html_content = await page.content()
            await browser.close()
    except Exception as e:
        print(f"--- FAILED: Pan Asia Bank scraper threw an error during browser automation. --- \nError: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    content_anchor = soup.find('h2', id='cRate')
    if not content_anchor or not (content_section := content_anchor.parent.parent):
        print("--- FAILED: Pan Asia Bank - Could not find content anchor.")
        return None
        
    all_rates = []
    for table_fig in content_section.find_all('figure', class_='wp-block-table'):
        if table := table_fig.find('table'):
            all_rates.extend(process_pan_asia_table(table, 'Standard')) 

    if not all_rates:
        print("--- FAILED: Pan Asia Bank - No rate data was extracted.")
        return None
        
    print(f"--- SUCCESS: Pan Asia Bank extracted {len(all_rates)} records.")
    return pd.DataFrame(all_rates)

def scrape_peoples_bank_fd_rates():
    """Scrapes all relevant LKR FD rates from the People's Bank website."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None

    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None

    def process_standard_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr'):
            cells = row.find_all('td')
            if len(cells) < 3 or not (term_months := parse_term_to_months(cells[0].get_text(strip=True))): continue
            if maturity_rate := clean_rate(cells[1].get_text()):
                records.append({'Bank Name': "People's Bank", 'FD Type': fd_type, 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': None})
            if monthly_rate := clean_rate(cells[2].get_text()):
                records.append({'Bank Name': "People's Bank", 'FD Type': fd_type, 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': None})
        return records

    url = 'https://www.peoplesbank.lk/interest-rates/'
    print(f"--- Starting: People's Bank ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []

        main_fd_header = soup.find('h4', string=re.compile(r'Fixed deposits \(Minimum deposit'))
        if main_fd_header and (main_fd_table := main_fd_header.find_next('table')):
            all_rates_data.extend(process_standard_table(main_fd_table, 'Standard'))

        if not all_rates_data:
            print("--- FAILED: People's Bank - No data was extracted.")
            return None

        print(f"--- SUCCESS: People's Bank extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)

    except Exception as e:
        print(f"--- FAILED: People's Bank scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

async def scrape_sampath_fd_rates_final_final():
    """Scrapes Sampath Bank FD rates using Playwright."""
    def extract_rate_and_aer(text):
        if not isinstance(text, str) or text.strip() in ['-', '']: return None, None
        match = re.search(r'([\d.]+)\s*%\s*(?:\(AER\s*([\d.]+)\s*%\))?', text, re.IGNORECASE)
        if match:
            main_rate = float(match.group(1)) if match.group(1) else None
            aer_rate = float(match.group(2)) if match.group(2) else main_rate
            return main_rate, aer_rate
        return None, None

    def parse_term_months(period_text):
        if 'month' in period_text.lower():
            match = re.search(r'(\d+)', period_text)
            return int(match.group(1)) if match else None
        return None

    url = 'https://www.sampath.lk/personal-banking/term-deposit-accounts/regular-deposits/Fixed-Deposits?category=personal_banking'
    print("--- Starting: Sampath Bank ---")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            rates_button_locator = page.locator('div.type-info-block-icon-outer:has(p:has-text("Rates"))')
            await rates_button_locator.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)
            await rates_button_locator.click()
            await page.wait_for_selector('p:has-text("Normal Fixed Deposit")', state='visible', timeout=15000)
            html_content = await page.content()
            await browser.close()
    except Exception as e:
        print(f"--- FAILED: Sampath Bank scraper threw an error during browser automation. --- \nError: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    all_rates_data = []
    fd_header = soup.find('p', string=re.compile(r'Normal Fixed Deposit', re.IGNORECASE))
    if not fd_header or not (table_container := fd_header.find_parent('div', class_='rates-info-heading')) or not (fd_table := table_container.find_next_sibling('table')):
        print("--- FAILED: Sampath Bank - Could not parse the rates table.")
        return None

    for row in fd_table.select('tbody > tr'):
        cells = row.find_all('td')
        if len(cells) < 4 or not (term_months := parse_term_months(cells[0].get_text(strip=True))): continue
        maturity_rate, maturity_aer = extract_rate_and_aer(cells[1].get_text(strip=True))
        monthly_rate, monthly_aer = extract_rate_and_aer(cells[2].get_text(strip=True))
        annually_rate, annually_aer = extract_rate_and_aer(cells[3].get_text(strip=True))
        if maturity_rate: all_rates_data.append({'Bank Name': 'Sampath Bank', 'FD Type': 'Normal', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': maturity_aer})
        if monthly_rate: all_rates_data.append({'Bank Name': 'Sampath Bank', 'FD Type': 'Normal', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': monthly_aer})
        if annually_rate: all_rates_data.append({'Bank Name': 'Sampath Bank', 'FD Type': 'Normal', 'Institution Type': 'Bank', 'Term (Months)': term_months, 'Payout Schedule': 'Annually', 'Interest Rate (p.a.)': annually_rate, 'Annual Effective Rate': annually_aer})

    if not all_rates_data:
        print("--- FAILED: Sampath Bank - No data was extracted from table.")
        return None
    
    print(f"--- SUCCESS: Sampath Bank extracted {len(all_rates_data)} records.")
    return pd.DataFrame(all_rates_data)

# ==============================================================================
# --- FINANCE COMPANY SCRAPERS (17 TOTAL) ---
# ==============================================================================
def scrape_alliance_finance_fd_rates():
    """Scrapes Alliance Finance PLC's Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    url = 'https://www.alliancefinance.lk/investments/fixed-deposits/'
    print(f"--- Starting: Alliance Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        table = soup.find('table', id='tablepress-1')
        if not table: return None
        all_rates_data = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5: continue
            if not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                all_rates_data.append({'Bank Name': 'Alliance Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                all_rates_data.append({'Bank Name': 'Alliance Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        if not all_rates_data: return None
        print(f"--- SUCCESS: Alliance Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Alliance Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

async def scrape_cdb_finance_fd_rates_final():
    """Scrapes CDB Finance rates using Playwright."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and '%' in rate_text:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    url = 'https://www.cdb.lk/products/cards/fd/cdb-dsfd'
    print(f"--- Starting: CDB Finance ---")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await page.wait_for_selector('div#collapsefirst', state='visible', timeout=20000)
            html_content = await page.content()
            await browser.close()
    except Exception as e:
        print(f"--- FAILED: CDB Finance scraper (Playwright). --- \nError: {e}", file=sys.stderr)
        return None
    soup = BeautifulSoup(html_content, 'lxml')
    if not (interest_rates_accordion := soup.find('div', id='collapsefirst')) or not (table := interest_rates_accordion.find('table', class_='table-striped')): return None
    all_rates_data = []
    for tbody in table.find_all('tbody'):
        if not (row := tbody.find('tr')): continue
        cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
        if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
        if monthly_rate := clean_rate(cells[1]):
            all_rates_data.append({'Bank Name': 'CDB Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[3])})
        if maturity_rate := clean_rate(cells[2]):
            all_rates_data.append({'Bank Name': 'CDB Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
    if not all_rates_data: return None
    print(f"--- SUCCESS: CDB Finance extracted {len(all_rates_data)} records.")
    return pd.DataFrame(all_rates_data)

def scrape_commercial_credit_fd_rates():
    """Scrapes Commercial Credit's Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip():
            match = re.search(r'([\d.]+)', rate_text)
            if match and (rate := float(match.group(1))) > 0: return rate
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)', term_text)
            return int(match.group(1)) if match else None
        return None
    url = 'https://www.cclk.lk/products/deposits/fixed-deposit/en'
    print(f"--- Starting: Commercial Credit ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        rate_header = soup.find('h3', string=re.compile(r'Non Senior Citizen Rates', re.IGNORECASE))
        if not rate_header or not (table := rate_header.find_next('table')): return None
        all_rates_data = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                all_rates_data.append({'Bank Name': 'Commercial Credit', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                all_rates_data.append({'Bank Name': 'Commercial Credit', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        if not all_rates_data: return None
        print(f"--- SUCCESS: Commercial Credit extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Commercial Credit scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_dialog_finance_fd_rates():
    """Scrapes all FD rates from the Dialog Finance website."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            if match and (rate := float(match.group(1))) > 0: return rate
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*MONTH', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    def process_dialog_finance_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if maturity_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'Dialog Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if monthly_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'Dialog Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.dialogfinance.lk/for-you/fixed-deposits'
    print(f"--- Starting: Dialog Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []
        for header in soup.find_all('h3', class_='section-title'):
            header_text = header.get_text(strip=True).lower()
            if not (table := header.find_next_sibling('div', class_='table-responsive')): continue
            if 'non-senior citizen' in header_text:
                all_rates_data.extend(process_dialog_finance_table(table, 'Standard'))
            elif 'senior citizen' in header_text:
                all_rates_data.extend(process_dialog_finance_table(table, 'Senior Citizen'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: Dialog Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Dialog Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_hnb_finance_fd_rates():
    """Scrapes HNB Finance Fixed Deposit rates."""
    def clean_rate(text):
        if isinstance(text, str) and text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(text):
        if isinstance(text, str):
            match = re.search(r'(\d+)\s*Month', text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    def process_hnb_finance_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all(['th', 'td'])]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'HNB Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'HNB Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.hnbfinance.lk/fixed-deposits/'
    print(f"--- Starting: HNB Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []
        standard_header = soup.find('h2', string='Interest Rates')
        if standard_header and (standard_table := standard_header.find_next('table')):
            all_rates_data.extend(process_hnb_finance_table(standard_table, 'Standard'))
        senior_header = soup.find('h2', string='Interest Rates – Senior Citizens')
        if senior_header and (senior_table := senior_header.find_next('table')):
            all_rates_data.extend(process_hnb_finance_table(senior_table, 'Senior Citizen'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: HNB Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: HNB Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_janashakthi_fd_rates():
    """Scrapes Janashakthi Finance's Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            term_text = term_text.lower()
            if 'year' in term_text: return int(re.search(r'(\d+)', term_text).group(1)) * 12
            elif 'month' in term_text: return int(re.search(r'(\d+)', term_text).group(1))
        return None
    def process_janashakthi_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'Janashakthi Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'Janashakthi Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.janashakthifinance.lk/services/fixed-deposits/'
    print(f"--- Starting: Janashakthi Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []
        if (standard_container := soup.find('div', id='fd-pop-cont_fd_r_normal')) and (standard_table := standard_container.find('table')):
            all_rates_data.extend(process_janashakthi_table(standard_table, 'Standard'))
        if (senior_container := soup.find('div', id='fd-pop-cont_fd_r_senior')) and (senior_table := senior_container.find('table')):
            all_rates_data.extend(process_janashakthi_table(senior_table, 'Senior Citizen'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: Janashakthi Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Janashakthi Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_lolc_finance_fd_rates():
    """Scrapes all FD rates from the LOLC Finance website."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'^(\d+)', term_text.strip())
            return int(match.group(1)) if match else None
        return None
    def process_lolc_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr')[1:]: # Skip sub-header row
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 7 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'LOLC Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if annually_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'LOLC Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Annually', 'Interest Rate (p.a.)': annually_rate, 'Annual Effective Rate': clean_rate(cells[4])})
            if maturity_rate := clean_rate(cells[5]):
                records.append({'Bank Name': 'LOLC Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[6])})
        return records
    url = 'https://www.lolcfinance.com/rates-and-returns/interest-rates/'
    print(f"--- Starting: LOLC Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []
        if (general_tab := soup.find('div', id='GeneralFD')) and (general_table := general_tab.find('table')):
            all_rates_data.extend(process_lolc_table(general_table, 'Normal'))
        if (senior_tab := soup.find('div', id='SCFD')) and (senior_table := senior_tab.find('table')):
            all_rates_data.extend(process_lolc_table(senior_table, 'Senior Citizen'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: LOLC Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: LOLC Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_mbsl_fd_rates():
    """Scrapes all FD rates from the MBSL Bank website."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str) and 'day' not in term_text.lower():
            match = re.search(r'(\d+)', term_text.strip())
            return int(match.group(1)) if match else None
        return None
    def process_mbsl_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'MBSL Bank', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'MBSL Bank', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.mbslbank.com/en/services/personal-services/deposits/fixed-deposits/'
    print(f"--- Starting: MBSL Bank ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_tables = soup.find_all('table', class_='table-bordered')
        if len(all_tables) < 2: return None
        all_rates_data = []
        all_rates_data.extend(process_mbsl_table(all_tables[0], 'Normal'))
        all_rates_data.extend(process_mbsl_table(all_tables[1], 'Senior Citizen'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: MBSL Bank extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: MBSL Bank scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_mercantile_fd_rates():
    """Scrapes Mercantile Investments Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() and '%' in rate_text:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    def process_mercantile_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'Mercantile Investments', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'Mercantile Investments', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.mi.com.lk/en/products-and-services/main-products/fixed-deposit'
    print(f"--- Starting: Mercantile Investments ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []
        if (divimithuru_img := soup.find('img', src=re.compile(r'divimithuru-en.png'))) and (divimithuru_table := divimithuru_img.find_parent('div').find_next_sibling('div', class_='table-wrapper')):
            all_rates_data.extend(process_mercantile_table(divimithuru_table, 'Divimithuru (Standard)'))
        if (kruthaguna_img := soup.find('img', src=re.compile(r'kruthaguna-en.png'))) and (kruthaguna_table := kruthaguna_img.find_parent('div').find_next_sibling('div', class_='table-wrapper')):
            all_rates_data.extend(process_mercantile_table(kruthaguna_table, 'Kruthaguna (Senior Citizen)'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: Mercantile Investments extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Mercantile Investments scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

async def scrape_nation_lanka_fd_rates_final():
    """Scrapes rates using Playwright to handle the JavaScript-rendered table."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() and '%' in rate_text:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    url = 'https://www.nationlanka.com/deposits'
    print(f"--- Starting: Nation Lanka Finance ---")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await page.wait_for_selector('table >> th:has-text("Period")', state='visible', timeout=20000)
            html_content = await page.content()
            await browser.close()
    except Exception as e:
        print(f"--- FAILED: Nation Lanka Finance scraper (Playwright). --- \nError: {e}", file=sys.stderr)
        return None
    soup = BeautifulSoup(html_content, 'lxml')
    if not (rates_table := soup.find('table', class_='table-auto')): return None
    all_rates_data = []
    header_cells = rates_table.select_one('thead > tr').find_all('th')
    payout_schedules = [cell.get_text(strip=True) for cell in header_cells[1:]]
    for row in rates_table.select('tbody > tr'):
        cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
        if len(cells) < len(header_cells) or not (term_months := parse_term_to_months(cells[0])): continue
        for i, rate_text in enumerate(cells[1:]):
            if rate := clean_rate(rate_text):
                all_rates_data.append({'Bank Name': 'Nation Lanka Finance', 'FD Type': 'Non-Senior Citizen', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': payout_schedules[i], 'Interest Rate (p.a.)': rate, 'Annual Effective Rate': None})
    if not all_rates_data: return None
    print(f"--- SUCCESS: Nation Lanka Finance extracted {len(all_rates_data)} records.")
    return pd.DataFrame(all_rates_data)

def scrape_plc_fd_rates():
    """Scrapes People's Leasing & Finance PLC Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str) and 'month' in term_text.lower():
            match = re.search(r'(\d+)', term_text)
            return int(match.group(1)) if match else None
        return None
    url = 'https://www.plc.lk/products/fixed-deposits-savings/fixed-deposits/'
    print(f"--- Starting: People's Leasing & Finance (PLC) ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        fd_header = soup.find('h4', class_='wp-block-heading', string='Normal Fixed Deposit')
        if not fd_header or not (table := fd_header.find_next_sibling('table')): return None
        all_rates_data = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if maturity_rate := clean_rate(cells[1]):
                all_rates_data.append({'Bank Name': "People's Leasing & Finance", 'FD Type': 'Normal', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if monthly_rate := clean_rate(cells[3]):
                all_rates_data.append({'Bank Name': "People's Leasing & Finance", 'FD Type': 'Normal', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        if not all_rates_data: return None
        print(f"--- SUCCESS: PLC extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: PLC scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_pmf_fd_rates():
    """Scrapes PMF Finance's Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() and '%' in rate_text:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    def process_pmf_table(table, fd_type):
        records = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'PMF Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'PMF Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://pmf.lk/en/fixed-deposit/'
    print(f"--- Starting: PMF Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []
        if (standard_div := soup.find('div', id='normal-fd-rates-table')) and (standard_table := standard_div.find('table')):
            all_rates_data.extend(process_pmf_table(standard_table, 'Standard'))
        if (senior_div := soup.find('div', id='se-citizen-fd-rates-table')) and (senior_table := senior_div.find('table')):
            all_rates_data.extend(process_pmf_table(senior_table, 'Senior Citizen'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: PMF Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: PMF Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

async def scrape_senkadagala_fd_rates_final():
    """Scrapes rates using Playwright to handle JavaScript rendering."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*months?', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    def process_senkadagala_table(table, fd_type):
        records = []
        for row in table.find_all('tr')[1:]:
            cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
            if any('period' in c.lower() for c in cells) or len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'Senkadagala Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'Senkadagala Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.senfin.com/personal.html'
    print(f"--- Starting: Senkadagala Finance ---")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await page.wait_for_selector('table#SeniorDeposits', state='visible', timeout=20000)
            await page.wait_for_selector('table#GeneralDeposits', state='visible', timeout=20000)
            html_content = await page.content()
            await browser.close()
    except Exception as e:
        print(f"--- FAILED: Senkadagala Finance scraper (Playwright). --- \nError: {e}", file=sys.stderr)
        return None
    soup = BeautifulSoup(html_content, 'lxml')
    all_rates_data = []
    if senior_table := soup.find('table', id='SeniorDeposits'):
        all_rates_data.extend(process_senkadagala_table(senior_table, 'Senior Citizen'))
    if general_table := soup.find('table', id='GeneralDeposits'):
        all_rates_data.extend(process_senkadagala_table(general_table, 'General'))
    if not all_rates_data: return None
    print(f"--- SUCCESS: Senkadagala Finance extracted {len(all_rates_data)} records.")
    return pd.DataFrame(all_rates_data)

def scrape_singer_finance_fd_rates():
    """Scrapes Singer Finance's Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() and '%' in rate_text:
            match = re.search(r'([\d.]+)', rate_text)
            if match and (rate := float(match.group(1))) > 0: return rate
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    url = 'https://singerfinance.com/en/products/fixed-deposit/standard-fixed-deposits'
    print(f"--- Starting: Singer Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        rate_header = soup.find('h2', string='Interest Paid Rate')
        if not rate_header or not (table := rate_header.find_next('table', class_='rating-table__wrap')): return None
        all_rates_data = []
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                all_rates_data.append({'Bank Name': 'Singer Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                all_rates_data.append({'Bank Name': 'Singer Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        if not all_rates_data: return None
        print(f"--- SUCCESS: Singer Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Singer Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_siyapatha_finance_fd_rates():
    """Scrapes all FD rates from the Siyapatha Finance website."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() and '%' in rate_text:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    def process_siyapatha_div_table(div_container, fd_type):
        records = []
        for row_div in div_container.find_all('div', class_='col-sm-12')[2:]:
            cells = [cell.get_text(strip=True) for cell in row_div.find_all('div', class_='col')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if maturity_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'Siyapatha Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if monthly_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'Siyapatha Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.siyapatha.lk/fixed-deposits/'
    print(f"--- Starting: Siyapatha Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        all_rates_data = []
        if (general_header := soup.find('h5', string=re.compile(r'General Public', re.IGNORECASE))) and (general_table_container := general_header.find_next_sibling('div', class_='b_ron')):
            all_rates_data.extend(process_siyapatha_div_table(general_table_container, 'General Public'))
        if (senior_header := soup.find('h5', string=re.compile(r'Senior Citizens', re.IGNORECASE))) and (senior_table_container := senior_header.find_next_sibling('div', class_='b_ron')):
            all_rates_data.extend(process_siyapatha_div_table(senior_table_container, 'Senior Citizen'))
        if not all_rates_data: return None
        print(f"--- SUCCESS: Siyapatha Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Siyapatha Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_smb_finance_fd_rates():
    """Scrapes SMB Finance PLC's Fixed Deposit rates."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() and '%' not in rate_text: rate_text += '%'
        if isinstance(rate_text, str) and rate_text.strip():
            match = re.search(r'([\d.]+)', rate_text)
            if match and (rate := float(match.group(1))) > 0: return rate
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            match = re.search(r'(\d+)\s*Month', term_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        return None
    url = 'https://www.smblk.com/products-services/fixed-deposits/'
    print(f"--- Starting: SMB Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        rates_table = next((table for table in soup.find_all('table', class_='tablebg') if (th := table.find('th')) and 'period' in th.get_text(strip=True).lower()), None)
        if not rates_table: return None
        all_rates_data = []
        for row in rates_table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                all_rates_data.append({'Bank Name': 'SMB Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                all_rates_data.append({'Bank Name': 'SMB Finance', 'FD Type': 'Standard', 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        if not all_rates_data: return None
        print(f"--- SUCCESS: SMB Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: SMB Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None

def scrape_vallibel_finance_fd_rates():
    """Scrapes all FD rates from the Vallibel Finance website."""
    def clean_rate(rate_text):
        if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
            match = re.search(r'([\d.]+)', rate_text)
            return float(match.group(1)) if match else None
        return None
    def parse_term_to_months(term_text):
        if isinstance(term_text, str):
            term_text = term_text.lower()
            match = re.search(r'(\d+)\s*(month|year)', term_text, re.IGNORECASE)
            if match:
                value, unit = int(match.group(1)), match.group(2)
                return value * 12 if 'year' in unit else value
        return None
    def process_vallibel_table(table_container):
        records = []
        if not (header := table_container.find('h3')) or not (table := table_container.find('table', class_='rg-table')): return records
        fd_type = header.get_text(strip=True)
        for row in table.select('tbody > tr'):
            cells = [cell.get_text(strip=True) for cell in row.find_all('td')]
            if len(cells) != 5 or not (term_months := parse_term_to_months(cells[0])): continue
            if monthly_rate := clean_rate(cells[1]):
                records.append({'Bank Name': 'Vallibel Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'Monthly', 'Interest Rate (p.a.)': monthly_rate, 'Annual Effective Rate': clean_rate(cells[2])})
            if maturity_rate := clean_rate(cells[3]):
                records.append({'Bank Name': 'Vallibel Finance', 'FD Type': fd_type, 'Institution Type': 'Finance Company', 'Term (Months)': term_months, 'Payout Schedule': 'At Maturity', 'Interest Rate (p.a.)': maturity_rate, 'Annual Effective Rate': clean_rate(cells[4])})
        return records
    url = 'https://www.vallibelfinance.com/product/fixed-deposits'
    print(f"--- Starting: Vallibel Finance ---")
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        rate_containers = soup.find_all('div', class_='rg-container')
        if not rate_containers: return None
        all_rates_data = []
        for container in rate_containers:
            all_rates_data.extend(process_vallibel_table(container))
        if not all_rates_data: return None
        print(f"--- SUCCESS: Vallibel Finance extracted {len(all_rates_data)} records.")
        return pd.DataFrame(all_rates_data)
    except Exception as e:
        print(f"--- FAILED: Vallibel Finance scraper threw an error. --- \nError: {e}", file=sys.stderr)
        return None


# ==============================================================================
# --- MAIN ORCHESTRATOR (MODIFIED) ---
# ==============================================================================
async def main():
    """
    Main function to run all scrapers and upload results to Supabase individually.
    """
    # --- PART 1: Initialize Supabase Client ---
    try:
        # Get secrets from environment variables (set by GitHub Actions)
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
        
        if not supabase_url or not supabase_key:
            print("---!!! FAILED to initialize Supabase. SUPABASE_URL or SUPABASE_SERVICE_KEY not set. !!!---", file=sys.stderr)
            print("---!!! Make sure you set these in your GitHub Actions secrets. !!!---", file=sys.stderr)
            return

        supabase_client: Client = create_client(supabase_url, supabase_key)
        print("✅ Supabase client initialized successfully.")
    except Exception as e:
        print(f"---!!! FAILED to initialize Supabase. !!!---", file=sys.stderr)
        print(f"Error: {e}", file=sys.stderr)
        return

    # --- PART 2: Define and Run All Scrapers ---
    print("\n>>> ========================================================== <<<")
    print(">>> Starting All Bank & Finance Company FD Rate Scrapers... <<<")
    print(">>> ========================================================== <<<")
    
    # We group scrapers by their function call to link results back
    sync_scrapers = {
        "Cargills Bank": scrape_cargills_bank_fd_rates,
        "DFCC Bank": scrape_dfcc_fd_rates_final,
        "National Savings Bank (NSB)": scrape_nsb_fd_rates,
        "Nations Trust Bank": scrape_ntb_fd_rates_final,
        "People's Bank": scrape_peoples_bank_fd_rates,
        "Alliance Finance": scrape_alliance_finance_fd_rates,
        "Commercial Credit": scrape_commercial_credit_fd_rates,
        "Dialog Finance": scrape_dialog_finance_fd_rates,
        "HNB Finance": scrape_hnb_finance_fd_rates,
        "Janashakthi Finance": scrape_janashakthi_fd_rates,
        "LOLC Finance": scrape_lolc_finance_fd_rates,
        "MBSL Bank": scrape_mbsl_fd_rates,
        "Mercantile Investments": scrape_mercantile_fd_rates,
        "People's Leasing & Finance": scrape_plc_fd_rates,
        "PMF Finance": scrape_pmf_fd_rates,
        "Singer Finance": scrape_singer_finance_fd_rates,
        "Siyapatha Finance": scrape_siyapatha_finance_fd_rates,
        "SMB Finance": scrape_smb_finance_fd_rates,
        "Vallibel Finance": scrape_vallibel_finance_fd_rates,
    }
    
    async_scrapers = {
        "Commercial Bank": scrape_commercial_bank_fd_rates,
        "Hatton National Bank (HNB)": scrape_hnb_fd_rates_final,
        "Pan Asia Bank": scrape_pan_asia_fd_rates_final,
        "Sampath Bank": scrape_sampath_fd_rates_final_final,
        "CDB Finance": scrape_cdb_finance_fd_rates_final,
        "Nation Lanka Finance": scrape_nation_lanka_fd_rates_final,
        "Senkadagala Finance": scrape_senkadagala_fd_rates_final,
    }
    
    # Create tasks for all scrapers
    sync_tasks = [asyncio.to_thread(func) for func in sync_scrapers.values()]
    async_tasks = [func() for func in async_scrapers.values()]
    
    # Link names back to the tasks
    all_scraper_names = list(sync_scrapers.keys()) + list(async_scrapers.keys())
    all_tasks = sync_tasks + async_tasks
    
    print(f"--- Running {len(all_tasks)} scrapers in parallel... ---")
    results = await asyncio.gather(*all_tasks, return_exceptions=True)
    
    # --- PART 3: Process and Upload Results Individually ---
    print("\n>>> ============================================== <<<")
    print(">>> Scraping Complete. Processing and Uploading... <<<")
    print(">>> ============================================== <<<")
    
    successful_scrapes = 0
    failed_scrapes = 0
    upload_tasks = []

    for name, result in zip(all_scraper_names, results):
        if isinstance(result, Exception):
            print(f"---! FAILED (Scrape): '{name}' threw an exception: {result}", file=sys.stderr)
            failed_scrapes += 1
        elif isinstance(result, pd.DataFrame) and not result.empty:
            print(f"---✅ SUCCESS (Scrape): '{name}' returned {len(result)} records.")
            successful_scrapes += 1
            # Add the atomic upload as a new task
            upload_tasks.append(update_supabase_for_institution(supabase_client, result, name))
        else:
            print(f"---! FAILED (Scrape): '{name}' returned no data or an invalid result.")
            failed_scrapes += 1
            
    # --- PART 4: Run all uploads in parallel ---
    if upload_tasks:
        print(f"\n--- Starting {len(upload_tasks)} Supabase uploads in parallel... ---")
        await asyncio.gather(*upload_tasks)
    
    print("\n\n--- MASTER SCRIPT FINISHED ---")
    print(f"✅ Successful Scrapes: {successful_scrapes}")
    print(f"❌ Failed Scrapes: {failed_scrapes}")
    print(f"--- Run complete. ---")


if __name__ == "__main__":
    asyncio.run(main())