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
        """ 
        with open("2_get_book_data.json", "w") as f:
            json.dump(data_json, f, ensure_ascii=False, indent=4)
        """
        grup_book_id = data_json["Product"].get("internalGroupSlug", "")
        grup_book_id = grup_book_id.split("-")[-4:]
        grup_book_id = "-".join(grup_book_id).strip('"')

        book_author = data_json["Product"]["author"]
        grup_book_page_url = f"{self.base_url}/livro{grup_book_id}"

        group_book_api_url = f"{self.base_url}/pdp-api/api/searchProducts/{grup_book_id}/usado?pageSize=-1"

        yield Request(
            url=group_book_api_url,
            headers=self.headers,
            callback=self.get_grup_book_data,
            meta={
                "name": response.meta["book_name"],
                "author": response.meta["book_author"],
                "price": response.meta["book_price"],
                "link": response.meta["book_link"],
                "id": response.meta["book_id"],
                "group_book_id": grup_book_id,
            },
        )

    def get_grup_book_data(self, response: Response):
        data = response.json()

        """
        with open("3_get_group_book_data.json", "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        """
        books_list = []
        skus = data["parentSkus"]
        for sku in skus:
            unit_name = sku["name"]
            unit_group_id = sku["itemGroupId"]
            unit_id = sku["productCode"]
            unit_description = sku["description"]
            unit_is_group = sku["productGroup"]
            unit_type = sku["productType"]
            unit_list_price = sku["listPrice"]
            unit_sale_price = sku["salePrice"]
            unit_discount_price = sku["discountPrice"]
            unit_is_avalilable = sku["available"]
            unit_image = sku["image"]
            unit_review_stars = sku["reviewStars"]
            unit_review_count = sku["reviewCount"]
            unit_attributes = sku["attributes"]

            book = {
                "name": unit_name,
                "author": response.meta["author"],
                "group_id": unit_group_id,
                "id": unit_id,
                "description": unit_description,
                "is_group": unit_is_group,
                "type": unit_type,
                "list_price": unit_list_price,
                "sale_price": unit_sale_price,
                "discount_price": unit_discount_price,
                "is_avalilable": unit_is_avalilable,
                "image": unit_image,
                "review_stars": unit_review_stars,
                "review_count": unit_review_count,
                # "attributes": unit_attributes,
            }
            books_list.append(book)

        book_json = {
            "book_name": response.meta["name"],
            "books_list": books_list,
        }

        with open("4_get_group_book_data.json", "a") as f:
            json.dump(book_json, f, ensure_ascii=False, indent=4)
            f.write("\n\n\n")


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
