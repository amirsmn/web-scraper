import asyncio
import logging
from scripts.args import parse_args
from scraper.scraper import Scraper
from scraper.exceptions import StopScrapError
from database.database import Database


def create_logger() -> logging.Logger:
    logging.basicConfig(
        filename="scraper.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return logging.getLogger(__name__)


def get_url(args) -> str:
    return f"https://www.ariamarz.com/{args.status}-{args.type}/{args.city}"


def get_file(args) -> str:
    return f"{args.filename}.{args.ext}"


async def scrap() -> None:
    args = parse_args()
    scraper = Scraper(get_url(args))
    db = Database()

    try:
        file = get_file(args)
        async for props in scraper.scrap():
            db.write(file=file, data=props, mode="a")

    except StopScrapError as e:
        logger.critical("Too many consecutive errors. Halting the scraping process")


if __name__ == "__main__":
    logger = create_logger()

    asyncio.run(scrap())
