class CustomScrapingError(Exception):
    def __init__(self, message: str, page: int, day: str, original_error: Exception = None):
        self.page = page
        self.day = day
        self.original_error = original_error
        full_message = f"Scraping Error on day {self.day}, page {self.page}: {message}"
        super().__init__(full_message)

    def __str__(self):
        return f"Day: {self.day}, Page: {self.page} - {self.args[0]}"