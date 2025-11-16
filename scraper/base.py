import pandas as pd

class BaseScraper:
    """
    The 'parent' class that all specific scrapers will inherit from.
    It holds the common logic and a blueprint for what all scrapers must have.
    """
    
    def __init__(self, name: str, institution_type: str, url: str):
        """
        This method is called when a new scraper is created.
        It sets the common properties for every scraper.
        """
        self.name = name
        self.institution_type = institution_type
        self.url = url
        # This print statement is great for logging in GitHub Actions
        print(f"--- Starting: {self.name} ---")

    async def scrape(self) -> pd.DataFrame:
        """
        This is the main scraping method that each 'child' class must create.
        We make it 'async' so it works with both httpx and Playwright.
        """
        # This error is a safeguard. If you forget to create a 'scrape' method
        # in a child scraper, your program will crash with this helpful error.
        raise NotImplementedError(f"{self.name} must have its own .scrape() method!")

    def get_log_data(self) -> dict:
        """
        A helper to return a consistent, default log entry for the dashboard.
        """
        return {
            'name': self.name,
            'institutionType': self.institution_type,
            'status': 'Pending',
            'recordsUpdated': 0,
            'errorMessage': 'N/A'
        }