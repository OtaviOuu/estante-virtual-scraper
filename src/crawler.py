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
        super(EstanteVirtual, self).__init__(*args, **kwargs)
        self.conn = sqlite3.connect("books_categ.db")
        self.cursor = self.conn.cursor()

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                book_name TEXT,
                author TEXT,
                book_id TEXT PRIMARY KEY,
                book_group_id TEXT,
                description TEXT,
                is_group TEXT,
                type TEXT,
                list_price REAL,
                sale_price REAL,
                image TEXT,
                link TEXT,
                publisher TEXT,
                year TEXT,
                language TEXT,
                isbn TEXT,
                handling_time TEXT,
                category TEXT,
                condition TEXT
            )
        """
        )
        self.conn.commit()

    def closed(self, reason):
        self.conn.close()

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
            url_usada = f"{self.base_url}{category}?tipo-de-livro=usado"
            url_nova = f"{self.base_url}{category}?tipo-de-livro=novo"

            yield Request(
                url=url_usada,
                headers=self.base_header,
                callback=self.get_max_pagination,
                meta={"condition": "usado"},
            )

            yield Request(
                url=url_nova,
                headers=self.base_header,
                callback=self.get_max_pagination,
                meta={"condition": "novo"},
            )

    def get_max_pagination(self, response: Response):
        results = response.css(".product-list-header__sort__text::text").get()
        if results:
            query_result = int(
                results.strip().split("de ")[1].split(" resultados")[0].replace(".", "")
            )
            last_page_index = query_result // 44
            if last_page_index >= 238:
                with open("./logs/max_pagination.txt", "a") as f:
                    f.write(f"{response.url} - {last_page_index}")

            last_page_index = min(last_page_index, 682)
            for index in range(1, int(last_page_index) + 1):
                url = f"{response.url}&page={index}"
                with open("./logs/urls.txt", "a") as f:
                    f.write(f"{url}\n")
                yield Request(
                    url=url,
                    headers={
                        "cookie": "for some reason this is needed",
                        "user-agent": user_agent_rotator.get_random_user_agent(),
                    },
                    callback=self.get_books,
                    meta={"condition": response.meta["condition"]},
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
                    "condition": response.meta["condition"],
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
        book_condition = response.meta["condition"]
        group_book_api_url = f"{self.base_url}/pdp-api/api/searchProducts/{grup_book_id}/{book_condition}?pageSize=-1"

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
                "condition": response.meta["condition"],
            },
        )

    def get_grup_book_data(self, response: Response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            with open("./logs/get_grup_book_data/erro.txt", "w") as f:
                f.write(response.url)

        try:
            aggregates = data["aggregates"]
            categorys = [
                agg["buckets"] for agg in aggregates if agg["keyName"] == "Categoria"
            ][0]

            """            
                category = next(
                (agg["buckets"] for agg in aggregates if agg["keyName"] == "Categoria"),
                [],
            ) 
            """
        except (KeyError, StopIteration):
            categorys = []

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
                "publisher": response.meta["formatted_atributes"].get("publisher", ""),
                "year": response.meta["formatted_atributes"].get("year", ""),
                "language": response.meta["formatted_atributes"].get("language", ""),
                "isbn": response.meta["formatted_atributes"].get("isbn", ""),
                "handling_time": response.meta["formatted_atributes"].get(
                    "handlingTime", ""
                ),
                "category": json.dumps(categorys),
                "condition": response.meta["condition"],
            }

            try:
                self.cursor.execute(
                    """
                    INSERT OR REPLACE INTO books (
                        book_name, author, book_id, book_group_id, description, 
                        is_group, type, list_price, sale_price, image, link, 
                        publisher, year, language, isbn, handling_time, category, condition
                    ) VALUES (
                        :book_name, :author, :book_id, :book_group_id, :description, 
                        :is_group, :type, :list_price, :sale_price, :image, :link, 
                        :publisher, :year, :language, :isbn, :handling_time, :category, :condition
                    )
                """,
                    book_unit,
                )
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"Database insertion error: {e}")
                with open("./logs/insertion_error.txt", "a") as f:
                    f.write(f"{book_unit['book_name']}\n")
                self.conn.rollback()


process = CrawlerProcess(
    settings={
        "CONCURRENT_REQUESTS": 1000,
        "DOWNLOAD_DELAY": 0,
    }
)

process.crawl(EstanteVirtual)
process.start()
