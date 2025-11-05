import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import random
import logging
from urllib.parse import urljoin, quote_plus

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Конфигурация
CONFIG = {
    'topic': 'multiplayer game engine architecture',
    'num_articles': 20,
    'output_csv': 'elibrary_articles.csv',
    'delay_min': 5,
    'delay_max': 12,
    'timeout': 30,
    'base_url': 'http://elibrary.ru'
}

class ElibraryScraper:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.kw_model = None  # Убрали KeyBERT
        self.headers_pool = self._get_headers_pool()
        self.session.cookies.clear()

    def _get_headers_pool(self):
        return [
            {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" ,'referer':'https://www.google.com/'}

        ]

    def _random_delay(self):
        delay = random.uniform(self.config['delay_min'], self.config['delay_max'])
        logger.info(f"Задержка: {delay:.2f} сек")
        time.sleep(delay)

    def _get_headers(self):
        return random.choice(self.headers_pool)

    def search_articles(self):
        """Поиск статей на elibrary.ru"""
        articles = []
        page = 1
        topic_encoded = quote_plus(self.config['topic'])

        logger.info(f"Поиск по теме: {self.config['topic']}")

        self.session.cookies.clear()  # СБРАСЫВАЕМ СЕССИЮ
        while len(articles) < self.config['num_articles']:
            search_url = self.config['base_url']+"/query_results.asp?where_fulltext=on&where_name=on&where_abstract=on&where_keywords=on&where_affiliation=&where_references=&type_article=on&type_disser=on&type_book=on&type_report=on&type_conf=on&type_patent=on&type_preprint=on&type_grant=on&type_dataset=on&search_freetext=&search_morph=on&search_fulltext=&search_open=&search_results=&titles_all=&authors_all=&rubrics_all=&queryboxid=&itemboxid=&begin_year=&end_year=&issues=all&orderby=rank&order=rev&changed=1"
            params = {
                'ftext': self.config['topic'],
                'pagenum': page
            }

            logger.info(f"Запрос к странице {page}: {search_url}")
            try:
                headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" ,'referer':'https://www.google.com/'}

                response = self.session.get(
                    search_url,
                    params=params,
                    headers=self._get_headers(),
                    timeout=self.config['timeout']
                )
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                print(response.text)
                rows = soup.find_all('tr', id=re.compile(r'^a\d+$'))
                if not rows:
                    logger.warning("Статьи не найдены на этой странице. Остановка.")
                    break

                logger.info(f"Найдено {len(rows)} статей на странице {page}")

                for row in rows:
                    if len(articles) >= self.config['num_articles']:
                        break

                    article_data = self.parse_list_item(row)
                    if article_data:
                        detailed = self.fetch_article_details(article_data['url'])
                        article_data.update(detailed)
                        articles.append(article_data)
                        logger.info(f"Обработано: {article_data['title'][:60]}...")

                page += 1
                self._random_delay()

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка сети: {e}")
                self._random_delay()
                continue
            except Exception as e:
                logger.error(f"Ошибка парсинга: {e}")
                break

        return articles[:self.config['num_articles']]

    def parse_list_item(self, row):
        """Парсинг строки из списка результатов"""
        try:
            title_tag = row.find('a', href=re.compile(r'item\.asp\?id=\d+'))
            if not title_tag:
                return None

            title = title_tag.get_text(strip=True)
            url = urljoin(self.config['base_url'], title_tag['href'])

            # Авторы и журнал
            info_text = row.find('td', align='left').get_text(separator='|', strip=True)
            parts = [p.strip() for p in info_text.split('|') if p.strip()]

            authors = ''
            journal_year = ''
            if len(parts) > 1:
                authors = parts[1] if parts[1].count('.') >= 2 else ''
                journal_year = ' '.join(parts[2:]) if len(parts) > 2 else ''

            year_match = re.search(r'\b(19|20)\d{2}\b', journal_year)
            year = year_match.group(0) if year_match else 'N/A'

            return {
                'title': self.clean_text(title),
                'authors': self.clean_text(authors),
                'year': year,
                'journal_info': self.clean_text(journal_year),
                'url': url
            }
        except Exception as e:
            logger.error(f"Ошибка парсинга строки: {e}")
            return None

    def fetch_article_details(self, url):
        """Получение детальной информации со страницы статьи"""
        try:
            logger.info(f"Загрузка детальной страницы: {url}")
            response = self.session.get(url, headers=self._get_headers(), timeout=self.config['timeout'])
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            details = {
                'journal': '',
                'volume': '',
                'issue': '',
                'abstract': ''
            }

            # Журнал
            journal_tag = soup.find('a', href=re.compile(r'contents\.asp\?id=\d+'))
            if journal_tag:
                details['journal'] = journal_tag.get_text(strip=True)

            # Том, номер, год
            info_table = soup.find('table', width='580')
            if info_table:
                text = info_table.get_text(separator=' ', strip=True)
                vol_match = re.search(r'Том:\s*([^\s]+)', text)
                issue_match = re.search(r'Номер:\s*<[^>]*>\s*([^\s<]+)', text)
                details['volume'] = vol_match.group(1) if vol_match else ''
                details['issue'] = issue_match.group(1) if issue_match else ''

            # Аннотация
            abstract_div = soup.find('div', id='abstract1') or soup.find('div', id='abstract2')
            if abstract_div:
                details['abstract'] = self.clean_text(abstract_div.get_text(separator=' ', strip=True))

            self._random_delay()
            return details

        except Exception as e:
            logger.error(f"Ошибка загрузки деталей {url}: {e}")
            return {'journal': '', 'volume': '', 'issue': '', 'abstract': ''}

    def clean_text(self, text):
        """Очистка текста"""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def save_to_csv(self, articles):
        """Сохранение в CSV"""
        if not articles:
            logger.warning("Нет данных для сохранения")
            return

        filename = self.config['output_csv']
        logger.info(f"Сохранение {len(articles)} статей в {filename}")

        fieldnames = [
            'title', 'authors', 'year', 'journal', 'volume', 'issue',
            'journal_info', 'abstract', 'url'
        ]

        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for art in articles:
                    writer.writerow(art)
            logger.info(f"Успешно сохранено в {filename}")
        except Exception as e:
            logger.error(f"Ошибка записи CSV: {e}")

    def run(self):
        logger.info("=" * 60)
        logger.info("ELIBRARY.RU SCRAPER — ЗАПУСК")
        logger.info("=" * 60)

        articles = self.search_articles()

        if articles:
            self.save_to_csv(articles)

            print("\n" + "=" * 60)
            print("РЕЗУЛЬТАТЫ")
            print("=" * 60)
            for i, art in enumerate(articles, 1):
                print(f"\n{i}. {art['title']}")
                print(f"   Авторы: {art['authors']}")
                print(f"   Год: {art['year']}")
                print(f"   Журнал: {art['journal']} | Т.{art['volume']} №{art['issue']}")
                print(f"   URL: {art['url']}")
                if art['abstract']:
                    print(f"   Аннотация: {art['abstract'][:200]}...")
        else:
            print("Статьи не найдены.")

        logger.info("Скрапинг завершён.")


if __name__ == "__main__":
    scraper = ElibraryScraper(CONFIG)
    scraper.run()