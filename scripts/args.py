import argparse
import logging


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrap properties from https://ariamarz.com.")

    parser.add_argument(
        "city",
        type=str.lower,
        help="Specify the city name for which the property data should be scraped."
    )

    parser.add_argument(
        "type",
        type=str.lower,
        choices=("apartment", "commercial_shop", "villa", "office", "industrial_agricultural", "old_house"),
        help="Determine which type of properties should be scraped."
    )

    parser.add_argument(
        "status",
        type=str.lower,
        choices=("buy", "rent"),
        help="Specify whether to scrape properties for sale (buy) or rent."
    )

    parser.add_argument(
        "-e",
        "--ext",
        type=str.lower,
        choices=("csv", "xlsx"),
        help="Export format for scraped data (csv or xlsx). Default: csv.",
        default="csv"
    )

    parser.add_argument(
        "-f",
        "--filename",
        type=str,
        help="Name of the output file (without extension). Default: 'data'.",
        default="data"
    )

    return parser.parse_args()
