import asyncio
import aiohttp
import logging
import random
import re
from selectolax.lexbor import LexborHTMLParser
from scraper.exceptions import StopScrapError
from typing import AsyncGenerator, Dict, List, Optional, Union, Tuple


logger = logging.getLogger(__name__)


class Scraper:
    def __init__(
        self,
        url: str,
        /,
        *,
        timeout: int = 5,
        concurrent_limit: int = 5,
        max_retry: int = 2,
        retry_after: float = 1.0,
        consecutive_errors: int = 5,
        user_agents: Optional[Tuple[str, ...]] = None,
        headers: Optional[Dict[str, str]] = None 
    ) -> None:
        self.url = url
        self.timeout = timeout
        self.concurrent_limit = concurrent_limit
        self.max_retry = max_retry
        self.retry_after = retry_after
        self.consecutive_errors = consecutive_errors
        self.user_agents = user_agents
        self.headers = headers

    async def scrap(self) -> AsyncGenerator[List[Dict[str, Union[int, str, None]]], None]:
        logger.info(f"Starting to scrape: {self.url}")
        async for properties in self.scrap_pages():
            yield properties

    async def scrap_pages(self) -> AsyncGenerator[List[Dict[str, Union[int, str, None]]], None]:
        async with aiohttp.ClientSession() as session:
            page_no = 1
            task_attempts = {}
            consecutive_errors = 0
            should_create_task = True

            while should_create_task or task_attempts:
                if should_create_task and not len(task_attempts):
                    for _ in range(self.concurrent_limit):
                        task_attempts[asyncio.create_task(self._scrap_page(session, page_no))] = 1
                        page_no += 1

                for task in tuple(task_attempts.keys())[:self.concurrent_limit]:
                    try:
                        properties = await task

                    except Exception as e:
                        logger.error(f"'{e.__class__.__name__}' occurred while scraping page: {e.page_url}")
                        consecutive_errors += 1
                        if consecutive_errors >= self.consecutive_errors:
                            raise StopScrapError("Too many consecutive errors")

                        task_attempts[task] += 1
                        if task_attempts[task] > self.max_retry:
                            logger.warning(f"Discarding page '{e.page_url}' after {self.max_retry} retries")
                        else:
                            task_attempts[
                                asyncio.create_task( self._scrap_page(session, e.page_no, make_delay=True) )
                            ] = task_attempts[task]

                    else:
                        consecutive_errors = 0
                        if not properties:
                            should_create_task = False
                        else:
                            yield properties

                    finally:
                        del task_attempts[task]

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, value: str) -> None:
        if not re.match(r"^(https?://)?(www\.)?(ariamarz)(\.com)(/[a-zA-Z-_]+)+(/)?$", value):
            raise ValueError(f"Invalid URL: {value}")
        value = value.rstrip("/")
        self._city = value.rsplit("/", 1)[1]
        self._url = f"{value}?in=&page="

    @property
    def timeout(self) -> int:
        return self._timeout

    @timeout.setter
    def timeout(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("'timeout' should be an integer")
        self._timeout = value if (5 <= value <= 15) else 5

    @property
    def concurrent_limit(self) -> int:
        return self._concurrent_limit

    @concurrent_limit.setter
    def concurrent_limit(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("'concurrent_limit' should be an integer")
        self._concurrent_limit = value if (1 <= value <= 10) else 10

    @property
    def max_retry(self) -> int:
        return self._max_retry

    @max_retry.setter
    def max_retry(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("'max_retry' should be an integer")
        self._max_retry = value if (1 <= value <= 4) else 2

    @property
    def retry_after(self) -> float:
        return self._retry_after

    @retry_after.setter
    def retry_after(self, value: float) -> None:
        if not isinstance(value, (float, int)):
            raise TypeError("'retry_after' should be a floating point number")
        self._retry_after = value if (0.5 <= value <= 2.0) else 1.0

    @property
    def consecutive_errors(self) -> int:
        return self._consecutive_errors

    @consecutive_errors.setter
    def consecutive_errors(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError("'consecutive_errors' should be an integer")
        self._consecutive_errors = value if (5 <= value <= 15) else 5

    @property
    def user_agents(self) -> Tuple[str, ...]:
        return self._user_agents

    @user_agents.setter
    def user_agents(self, user_agents: Optional[Tuple[str, ...]]) -> None:
        if user_agents is not None and not isinstance(user_agents, tuple):
            raise ValueError("'user_agents should be a tuple")
        self._user_agents = (
            user_agents or
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0/sUZ6nayS3umMDe4j",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/55.0.649.87 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0",
                "Mozilla/5.0 (Linux; Android 12; XQ-BC72) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/82.0.4062.3 Mobile Safari/537.36",
                "Mozilla/5.0 (compatible; MSIE 10.0; AOL 9.7; AOLBuild 4343.55; Windows NT 6.2; WOW64; Trident/6.0)-620",
            )
        )

    @property
    def headers(self) -> Dict[str, str]:
        return self._headers

    @headers.setter
    def headers(self, headers: Optional[Dict[str, str]]) -> None:
        if headers is not None and not isinstance(headers, dict):
            raise ValueError("'headers' should be a dictionary")
        self._headers = (
            headers or
            {
                "User-Agent": random.choice(self.user_agents),
                "Referer": "https://www.ariamarz.com/",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
            }
        )

    async def _scrap_page(
        self,
        session: aiohttp.ClientSession,
        page_no: int,
        make_delay: bool = False
    ) -> List[Dict[str, Union[int, str, None]]]:
        def int_price(price: str) -> int:
            return int(price.replace(",", ""))

        fa_to_en = {
            "خرید و فروش": "buy",
            "رهن و اجاره": "rent",
            "آپارتمان": "apartment",
            "مغازه و تجاری": "commercial",
            "خانه ویلایی حیاط دار": "villa",
            "خانه حیاط دار ویلایی": "villa",
            "ویلا، خانه ویلایی و باغ ویلا": "villa",
            "دفتر کار و اداری": "office",
            "صنعتی، کشاورزی": "industrial",
            "زمین و کلنگی": "old house",
        }

        page_url = f"{self.url}{page_no}"
        properties = []

        if page_no != 1:
            self.headers["Referer"] = f"{self.url}{page_no - 1}"

        try:
            if make_delay:
                await asyncio.sleep(self.retry_after)

            logger.info(f"Scraping page: {page_url}")
            async with session.get(
                page_url,
                timeout=self.timeout,
                headers=self.headers,
                raise_for_status=True
            ) as response:
                for plate in LexborHTMLParser(await response.text()).body.css(".product-plate-detail"):
                    if price := plate.css_first("li").text(strip=True):
                        prop = {
                            "id": int(plate.parent.css_first("img").attributes["data-id"]),
                            "city": self._city,
                            "status": fa_to_en[plate.css_first("img").attributes["alt"]],
                            "type": fa_to_en[plate.css_first("img").parent.text(strip=True)],
                            "price": int_price(price.split(" ", 2)[1]),
                        }

                        if prop["status"] == "rent":
                            try:
                                prop["deposit-amount"] = int_price(
                                    plate.css_first(".col-7 li:nth-child(2)").text(strip=True).split(" ", 1)[0]
                                )
                            except ValueError:
                                prop["deposit-amount"] = None

                        if prop["type"] in {"apartment", "commercial", "villa", "office"}:
                            prop["area"] = int(
                                plate.css_first(".col-5 li:nth-child(2)").text(strip=True).split(" ", 1)[0]
                            )
                        else:
                            prop["area"] = int(
                                plate.css_first(".col-5 li:nth-child(3)").text(strip=True).split(" ", 1)[0]
                            )

                        if prop["type"] != "old house":
                            try:
                                prop["year_built"] = int(
                                    plate.css_first(".col-5 li:nth-child(4)").text(strip=True).rsplit(" ", 1)[1]
                                )
                            except ValueError:
                                prop["year_built"] = None

                        properties.append(prop)

                return properties

        except Exception as e:
            e.page_no = page_no
            e.page_url = page_url
            raise
