from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response

from random_user_agent.user_agent import UserAgent
from slugify import slugify
import json


user_agent_rotator = UserAgent()


class EstanteVirtual(Spider):
    name = "estante_virtual"
    base_url = f"https://www.estantevirtual.com.br"

    base_header = {
        "cookie": "for some reason this is needed",
        "user-agent": user_agent_rotator.get_random_user_agent(),
    }

    def start_requests(self):
        # TODO: achar alguma fomra de pegar o numero de paginas OU fazer
        # uma paginação (sair "clicando" la em baixo), n sei bem
        for page_index in range(1, 2141):
            url_exatas = f"{self.base_url}/busca?xpage={page_index}"
            yield Request(
                url=url_exatas, headers=self.base_header, callback=self.get_books
            )

    def get_books(self, response: Response):
        try:
            data_layer = json.loads(
                response.css("script::text")[1].get().strip().split("= ")[1]
            )[0]
            """             
            with open("1_get_books.json", "w") as f:
                json.dump(data_layer, f, ensure_ascii=False, indent=4)
             """
            books = data_layer["ecommerce"]["impressions"]
            for book in books:
                book_name = book["name"]
                book_id = book["item_id"]
                book_author = book["brand"]
                book_price = book["price"]
                book_link = f"{self.base_url}/livro/{slugify(book_name)}-{book_id}"

                yield Request(
                    url=book_link,
                    headers=self.base_header,
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
        try:
            data_layer = (
                response.css("script")[-3]
                .get()
                .strip()
                # ruim mas funciona
                .replace("<script>window.__INITIAL_STATE__=", "")
                .replace("</script>", "")
            )

            data_json = json.loads(data_layer)
            """ 
            with open("2_get_book_data.json", "w") as f:
                json.dump(data_json, f, ensure_ascii=False, indent=4)
            """
            formated_atributes = data_json["Product"]["formattedAttributes"]
            author = data_json["Product"]["author"]

            grup_book_id = data_json["Product"].get("internalGroupSlug", "")
            grup_book_id = grup_book_id.split("-")[-4:]
            grup_book_id = "-".join(grup_book_id).strip('"')

            group_book_api_url = f"{self.base_url}/pdp-api/api/searchProducts/{grup_book_id}/usado?pageSize=-1"

            yield Request(
                url=group_book_api_url,
                headers=self.base_header,
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
        except (IndexError, json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error parsing book data: {e}")

    def get_grup_book_data(self, response: Response):
        try:
            data = response.json()
            books_list = []

            """
            with open("3_get_group_book_data.json", "w") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            """

            aggregates = data["aggregates"]

            for aggregate in aggregates:
                if aggregate["keyName"] == "Categoria":
                    category = aggregate

            skus = data["parentSkus"]
            for sku in skus:
                book_unit = {
                    "name": sku["name"],
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
                    "attributes": response.meta["formatted_atributes"],
                    "category": category,
                }
                books_list.append(book_unit)

            book_data = {
                "book_name": response.meta["name"],
                "books_list": books_list,
            }

            yield book_data
            """
            with open("4_get_group_book_data.json", "a") as f:
                json.dump(book_data, f, ensure_ascii=False, indent=4)
                f.write("\n\n\n")
            """
        except (IndexError, json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error parsing group book data: {e}")


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
