import asyncio
import os
import sys
from supabase import create_client, Client
import pandas as pd
from datetime import datetime

# --- Imports are now simple and relative ---
from .cargills import CargillsScraper
from .alliance import AllianceScraper
from .commercial_bank import CommercialBankScraper
# TODO: from .hnb import HNBScraper
# ... add all 26 relative imports here ...

from .utils import clean_and_rename_df
from .base import BaseScraper


async def run_scraper_orchestrator():
    """
    This is the new "main" function.
    It runs every scraper one-by-one and logs the result.
    """
    
    # 1. Initialize Supabase
    try:
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase URL/Key not set in environment variables")
        
        supabase_client: Client = create_client(supabase_url, supabase_key)
        print("✅ Supabase client initialized.")
    except Exception as e:
        print(f"---!!! FAILED to initialize Supabase. !!!--- \nError: {e}", file=sys.stderr)
        return

    # 2. This list is your "To-Do" list.
    scrapers_to_run = [
        CargillsScraper(),
        AllianceScraper(),
        # TODO: Add all 26 of your scraper objects here
    ]

    print(f"\n>>> Starting all {len(scrapers_to_run)} scrapers... <<<")
    
    # 3. This loop runs each scraper one by one, in isolation.
    all_logs = [s.get_log_data() for s in scrapers_to_run]
    
    tasks = []
    for i, scraper in enumerate(scrapers_to_run):
        tasks.append(run_single_scraper(supabase_client, scraper, all_logs[i]))

    # Run all scraper tasks in parallel
    await asyncio.gather(*tasks)
    
    # 4. Upload all the logs to your dashboard table in one go
    print("\n\n>>> Scraping complete. Uploading logs to dashboard... <<<")
    try:
        for log in all_logs:
            log['lastRun'] = datetime.now().isoformat()

        await asyncio.to_thread(
            supabase_client.from_("scraper_logs").upsert(all_logs, on_conflict="name").execute
        )
        print("✅ Successfully updated scraper_logs dashboard.")
    except Exception as e:
        print(f"---! FAILED (Log Upload): Could not update dashboard. \nError: {e}", file=sys.stderr)
    
    print("\n--- MASTER SCRIPT FINISHED ---")


async def run_single_scraper(supabase_client: Client, scraper: BaseScraper, log_entry: dict):
    """
    A helper function to run one scraper and handle its success or failure.
    """
    try:
        df = await scraper.scrape()
        
        if df.empty:
            print(f"---! FAILED (Scrape): '{scraper.name}' returned no data.")
            log_entry['status'] = 'Failed'
            log_entry['errorMessage'] = 'No data extracted'
            return

        records = clean_and_rename_df(df).to_dict('records')
        
        await asyncio.to_thread(
            supabase_client.from_("public-rates").delete().eq("bankName", scraper.name).execute
        )
        
        await asyncio.to_thread(
            supabase_client.from_("public-rates").insert(records).execute
        )

        print(f"---✅ SUCCESS (Scrape): '{scraper.name}' updated {len(records)} records.")
        log_entry['status'] = 'Success'
        log_entry['recordsUpdated'] = len(records)
        log_entry['errorMessage'] = 'N/A'

    except Exception as e:
        print(f"---! FAILED (Scrape): '{scraper.name}' threw an error. --- \nError: {e}", file=sys.stderr)
        log_entry['status'] = 'Failed'
        log_entry['errorMessage'] = str(e)


if __name__ == "__main__":
    asyncio.run(run_scraper_orchestrator())