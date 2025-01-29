

class StopScrapError(Exception):
    """Exception raised to stop the Scrap."""
    def __init__(self, message=None):
        self.message = message
        super().__init__(message)

    def __str__(self):
        return self.message if self.message else ""

