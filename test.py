import requests
from bs4 import BeautifulSoup

# Пример запроса к elibrary.ru
url = "http://elibrary.ru/query_results.asp?where_fulltext=on&where_name=on&where_abstract=on&where_keywords=on&where_affiliation=&where_references=&type_article=on&type_disser=on&type_book=on&type_report=on&type_conf=on&type_patent=on&type_preprint=on&type_grant=on&type_dataset=on&search_freetext=&search_morph=on&search_fulltext=&search_open=&search_results=&titles_all=&authors_all=&rubrics_all=&queryboxid=&itemboxid=&begin_year=&end_year=&issues=all&orderby=rank&order=rev&changed=1&pagenum=2"
payload = {
   
    "ftext": "multiplayer game engine"
}

headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" ,'referer':'https://www.google.com/'}

response = requests.get(url, params=payload,headers=headers, timeout=30)

soup = BeautifulSoup(response.text, "html.parser")
print(response.text)
# Далее обрабатываем HTML-страницу с помощью BeautifulSoup
# Например, находим все статьи по определенному тегу

# Это лишь пример, для реального использования необходимо изучить структуру
# HTML-страниц на elibrary.ru
