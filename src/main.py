import requests
import json
from pprint import pp
from selectolax.parser import HTMLParser
from random_user_agent.user_agent import UserAgent


user_agent_rotator = UserAgent()

headers = {
    "cookie": "for some reason this is needed",
    "user-agent": user_agent_rotator.get_random_user_agent(),
}


def scrape():
    for page_index in range(1, 100):
        url = f"https://www.estantevirtual.com.br/busca?tipo-de-livro=usado&categoria=ciencias-exatas&page={page_index}"
        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            try:
                html = HTMLParser(response.text)
                data_layer = json.loads(
                    html.css("script")[1].text().strip().split("= ")[1]
                )[0]

                books = data_layer["ecommerce"]["impressions"]
                for book in books:
                    book_name = book["name"]
                    pp(book["name"])
                    with open("books.txt", "a") as f:
                        f.write(book_name + "\n")

            except Exception as e:
                print(f"Error: {e}")
        else:
            print(f"Error: {response.status_code}")


if __name__ == "__main__":
    scrape()
