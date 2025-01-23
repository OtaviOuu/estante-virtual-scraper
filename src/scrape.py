from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response

from random_user_agent.user_agent import UserAgent
from slugify import slugify
import sqlite3
import json

user_agent_rotator = UserAgent()


class EstanteVirtual(Spider):
    name = "estante_virtual"
    base_url = f"https://www.estantevirtual.com.br"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = sqlite3.connect("estante_virtual.db")
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                book_name TEXT,
                author TEXT,
                book_id TEXT,
                book_group_id TEXT,
                description TEXT,
                is_group BOOLEAN,
                type TEXT,
                list_price REAL,
                sale_price REAL,
                image TEXT,
                link TEXT,
                attributes TEXT,
                category TEXT
            )
            """
        )
        self.conn.commit()

    def start_requests(self):
        self.base_header = {
            "cookie": "for some reason this is needed",
            "user-agent": user_agent_rotator.get_random_user_agent(),
        }

        yield Request(
            url=f"{self.base_url}/categoria",
            headers=self.base_header,
            callback=self.get_categorys,
        )

    def get_categorys(self, response: Response):
        categorys = response.css(
            ".estantes-list-container ul li a::attr(href)"
        ).getall()

        for category in categorys:
            url = f"{self.base_url}{category}?tipo-de-livro=novo"

            yield Request(
                url=url,
                headers=self.base_header,
                callback=self.get_max_pagination,
            )
            """for page_index in range(1, 30):
            url = f"{self.base_url}{category}?tipo-de-livro=novo&page={page_index}"
            yield Request(
                url=url,
                headers={
                    "cookie": "for some reason this is needed",
                    "user-agent": user_agent_rotator.get_random_user_agent(),
                },
                callback=self.get_books,
            )
            """

    def get_max_pagination(self, response: Response):
        # last_page = response.css(".pagination__page::text").getall()
        # last_page_index = last_page[-1].strip() if last_page else "1"

        results = response.css(".product-list-header__sort__text::text").get()
        if results:
            query_result = int(
                results.strip().split("de ")[1].split(" resultados")[0].replace(".", "")
            )
            last_page_index = query_result // 44
            if last_page_index >= 238:
                with open("./logs/max_pagination.txt", "a") as f:
                    f.write(f"{response.url} - {last_page_index}")
            else:
                return
            last_page_index = min(last_page_index, 682)
            for index in range(1, int(last_page_index) + 1):
                url = f"{response.url}&page={index}"
                yield Request(
                    url=url,
                    headers={
                        "cookie": "for some reason this is needed",
                        "user-agent": user_agent_rotator.get_random_user_agent(),
                    },
                    callback=self.get_books,
                )

    def get_books(self, response: Response):
        data_layer = json.loads(
            response.css("script::text")[1].get().strip().split("= ")[1]
        )[0]

        books = data_layer["ecommerce"]["impressions"]
        for book in books:
            book_name = book["name"]
            book_id = book["item_id"]
            book_author = book["brand"]
            book_price = book["price"]
            book_link = f"{self.base_url}/livro/{slugify(book_name)}-{book_id}"

            yield Request(
                url=book_link,
                headers={
                    "cookie": "for some",
                    "user-agent": user_agent_rotator.get_random_user_agent(),
                },
                callback=self.get_book_data,
                meta={
                    "book_name": book_name,
                    "book_author": book_author,
                    "book_price": book_price,
                    "book_id": book_id,
                    "book_link": book_link,
                },
            )

    def get_book_data(self, response: Response):
        data_layer = (
            response.css("script")[-3]
            .get()
            .strip()
            .replace("<script>window.__INITIAL_STATE__=", "")
            .replace("</script>", "")
        )

        data_json = json.loads(data_layer)

        formated_atributes = data_json["Product"]["formattedAttributes"]
        author = data_json["Product"]["author"]

        grup_book_id = data_json["Product"].get("internalGroupSlug", "")
        grup_book_id = grup_book_id.split("-")[-4:]
        grup_book_id = "-".join(grup_book_id).strip('"')

        group_book_api_url = f"{self.base_url}/pdp-api/api/searchProducts/{grup_book_id}/novo?pageSize=-1"

        yield Request(
            url=group_book_api_url,
            headers={
                "cookie": "for some",
                "user-agent": user_agent_rotator.get_random_user_agent(),
            },
            callback=self.get_grup_book_data,
            meta={
                "name": response.meta["book_name"],
                "author": author,
                "price": response.meta["book_price"],
                "link": response.meta["book_link"],
                "id": response.meta["book_id"],
                "group_book_id": grup_book_id,
                "formatted_atributes": formated_atributes,
            },
        )

    def get_grup_book_data(self, response: Response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            with open("./logs/get_grup_book_data/erro.txt", "w") as f:
                f.write(response.url)
            return

        aggregates = data["aggregates"]

        for aggregate in aggregates:
            if aggregate["keyName"] == "Categoria":
                category = aggregate

        skus = data["parentSkus"]
        for sku in skus:
            book_unit = {
                "book_name": response.meta["name"],
                "author": response.meta["author"],
                "book_id": sku["productCode"],
                "book_group_id": sku["itemGroupId"],
                "description": sku["description"],
                "is_group": sku["productGroup"],
                "type": sku["productType"],
                "list_price": sku["listPrice"],
                "sale_price": sku["salePrice"],
                "image": sku["image"],
                "link": f"{self.base_url}/livro/{slugify(sku['name'])}-{sku['productCode']}",
                "attributes": json.dumps(response.meta["formatted_atributes"]),
                "category": json.dumps(category),
            }

            self.cursor.execute(
                """
                    INSERT INTO books (
                        book_name, author, book_id, book_group_id, description,
                        is_group, type, list_price, sale_price, image, link,
                        attributes, category
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                (
                    book_unit["book_name"],
                    book_unit["author"],
                    book_unit["book_id"],
                    book_unit["book_group_id"],
                    book_unit["description"],
                    book_unit["is_group"],
                    book_unit["type"],
                    book_unit["list_price"] / 100,
                    book_unit["sale_price"] / 100,
                    book_unit["image"],
                    book_unit["link"],
                    book_unit["attributes"],
                    book_unit["category"],
                ),
            )
            self.conn.commit()


process = CrawlerProcess(
    settings={
        "FEEDS": {
            "books.json": {
                "format": "json",
                "indent": 4,
                "encoding": "utf-8",
                "ensure_ascii": False,
            },
        },
        "CONCURRENT_REQUESTS": 100,  
        "DOWNLOAD_DELAY": 0,
        "RETRY_ENABLED": False,
    }
)

process.crawl(EstanteVirtual)
process.start()
