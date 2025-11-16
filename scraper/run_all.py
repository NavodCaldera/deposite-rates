import asyncio
import os
import sys # <-- THIS IS THE FIX
from supabase import create_client, Client
import pandas as pd

# We will import all your scraper classes here
from .scrapers.cargills import CargillsScraper
from .scrapers.alliance import AllianceScraper
# TODO: Add your other 24 scraper classes here
# from scrapers.hnb import HNBScraper
# from scrapers.commercial_bank import CommercialBankScraper
# ... etc.

# We also need the helper function to clean the data before upload
# Let's import it from your old file for now
from fd_scraper_v2 import clean_and_rename_df 

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
    # Add all your scrapers to this list.
    scrapers_to_run = [
        CargillsScraper(),
        AllianceScraper(),
        # TODO: Add your other 24 scrapers here
        # HNBScraper(),
        # CommercialBankScraper(),
    ]

    print(f"\n>>> Starting all {len(scrapers_to_run)} scrapers... <<<")
    
    # 3. This loop runs each scraper one by one, in isolation.
    all_logs = [s.get_log_data() for s in scrapers_to_run]
    
    for i, scraper in enumerate(scrapers_to_run):
        try:
            # Run the unique 'scrape' method for this company
            # We use to_thread because your 'scrape' methods use 'requests', which is synchronous
            df = await asyncio.to_thread(scraper.scrape)
            
            if df.empty:
                print(f"---! FAILED (Scrape): '{scraper.name}' returned no data.")
                all_logs[i]['status'] = 'Failed'
                all_logs[i]['errorMessage'] = 'No data extracted'
                continue

            # 4. If scrape succeeds, update the database
            
            # 4a. Clean the DataFrame
            records = clean_and_rename_df(df).to_dict('records')
            
            # 4b. Delete old data
            await asyncio.to_thread(
                supabase_client.from_("public-rates").delete().eq("bankName", scraper.name).execute
            )
            
            # 4c. Insert new data
            await asyncio.to_thread(
                supabase_client.from_("public-rates").insert(records).execute
            )

            print(f"---✅ SUCCESS (Scrape): '{scraper.name}' updated {len(records)} records.")
            all_logs[i]['status'] = 'Success'
            all_logs[i]['recordsUpdated'] = len(records)

        except Exception as e:
            # If this one scraper fails, it won't crash the whole script!
            print(f"---! FAILED (Scrape): '{scraper.name}' threw an error. --- \nError: {e}", file=sys.stderr)
            all_logs[i]['status'] = 'Failed'
            all_logs[i]['errorMessage'] = str(e)
    
    # 5. Upload all the logs to your dashboard table in one go
    print("\n\n>>> Scraping complete. Uploading logs to dashboard... <<<")
    try:
        # Use upsert to create or update the log for each scraper
        await asyncio.to_thread(
            supabase_client.from_("scraper_logs").upsert(all_logs, on_conflict="name").execute
        )
        print("✅ Successfully updated scraper_logs dashboard.")
    except Exception as e:
        print(f"---! FAILED (Log Upload): Could not update dashboard. \nError: {e}", file=sys.stderr)
    
    print("\n--- MASTER SCRIPT FINISHED ---")

if __name__ == "__main__":
    asyncio.run(run_scraper_orchestrator())