import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import Request
from scrapy.http import Response
import json
from random_user_agent.user_agent import UserAgent
from pprint import pp
from slugify import slugify


user_agent_rotator = UserAgent()


class EstanteVirtual(scrapy.Spider):
    name = "estante_virtual"
    base_url = f"https://www.estantevirtual.com.br"

    headers = {
        "cookie": "for some reason this is needed",
        "user-agent": user_agent_rotator.get_random_user_agent(),
    }

    def start_requests(self):
        for page_index in range(1, 2141):
            url_exatas = f"{self.base_url}/busca?tipo-de-livro=usado&categoria=ciencias-exatas&page={page_index}"
            yield Request(url=url_exatas, headers=self.headers, callback=self.get_books)

    def get_books(self, response: Response):
        try:
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

                # api_page = f"{self.base_url}/pdp-api/api/searchProducts/{book_id}/usado"

                yield Request(
                    url=book_link,
                    headers=self.headers,
                    callback=self.get_book_data,
                    meta={
                        "book_name": book_name,
                        "book_author": book_author,
                        "book_price": book_price,
                        "book_id": book_id,
                        "book_link": book_link,
                    },
                )
        except (IndexError, json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error parsing book data: {e}")

    def get_book_data(self, response: Response):
        data_layer = (
            response.css("script")[-3]
            .get()
            .strip()
            # ruim mas funciona
            .replace("<script>window.__INITIAL_STATE__=", "")
            .replace("</script>", "")
        )

        data_json = json.loads(data_layer)
        parents = data_json["Product"]["parents"]
        for parent in parents:
            skus = parent["skus"]
            desc = []
            for sku in skus:
                unit_description = sku["description"]
                unit_long_description = sku["longDescription"]

                desc.append(unit_description)

                yield {
                    "name": response.meta["book_name"],
                    "author": response.meta["book_author"],
                    "price": response.meta["book_price"],
                    "link": response.meta["book_link"],
                    "id": response.meta["book_id"],
                    "descriptions": len(desc),
                }


""" 
        yield { 
            "name": response.meta["book_name"],
            "author": response.meta["book_author"],
            "price": response.meta["book_price"],
            "link": response.meta["book_link"],
            "id": response.meta["book_id"],
            "quantity": json_book["total"],
            "pages": json_book["totalPages"],
        }
 """

process = CrawlerProcess(
    settings={
        "FEEDS": {
            "items.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 4,
            },
        },
    }
)


process.crawl(EstanteVirtual)
process.start()
