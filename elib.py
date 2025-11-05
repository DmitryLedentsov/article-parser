import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import random
import logging
from urllib.parse import urljoin, quote_plus
import os

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
    'num_articles': 200,
    'output_csv': 'elibrary_articles.csv',
    'delay_min': 40,
    'delay_max': 60,
    'timeout': 30,
    'base_url': 'http://elibrary.ru'
}

class ElibraryScraper:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.headers_pool = self._get_headers_pool()
        self.session.cookies.clear()
        self.csv_file = None
        self.csv_writer = None

    def _get_headers_pool(self):
        return [
            {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36", 'referer': 'https://www.google.com/'}
        ]

    def _random_delay(self):
        delay = random.uniform(self.config['delay_min'], self.config['delay_max'])
        logger.info(f"Задержка: {delay:.2f} сек")
        time.sleep(delay)

    def _get_headers(self):
        return random.choice(self.headers_pool)

    def _init_csv(self):
        """Инициализация CSV файла"""
        filename = self.config['output_csv']
        file_exists = os.path.exists(filename)
        
        self.csv_file = open(filename, 'a', newline='', encoding='utf-8')
        fieldnames = ['title', 'year', 'type', 'abstract', 'in_rinc', 'url']
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        
        # Пишем заголовок только если файл новый
        if not file_exists:
            self.csv_writer.writeheader()
            self.csv_file.flush()
        
        logger.info(f"CSV файл {'создан' if not file_exists else 'открыт для дозаписи'}: {filename}")

    def _write_article_to_csv(self, article):
        """Запись одной статьи в CSV"""
        if self.csv_writer:
            fieldnames = ['title', 'year', 'type', 'abstract', 'in_rinc', 'url']
            filtered_art = {k: article.get(k, '') for k in fieldnames}
            self.csv_writer.writerow(filtered_art)
            self.csv_file.flush()  # Сразу записываем на диск

    def _close_csv(self):
        """Закрытие CSV файла"""
        if self.csv_file:
            self.csv_file.close()
            logger.info("CSV файл закрыт")

    def search_articles(self):
        """Поиск статей на elibrary.ru"""
        articles_count = 0
        page = 1

        logger.info(f"Поиск по теме: {self.config['topic']}")

        self.session.cookies.clear()
        while articles_count < self.config['num_articles']:
            search_url = self.config['base_url']+"/query_results.asp?where_fulltext=on&where_name=on&where_abstract=on&where_keywords=on&where_affiliation=&where_references=&type_article=on&type_disser=on&type_book=on&type_report=on&type_conf=on&type_patent=on&type_preprint=on&type_grant=on&type_dataset=on&search_freetext=&search_morph=on&search_fulltext=&search_open=&search_results=&titles_all=&authors_all=&rubrics_all=&queryboxid=&itemboxid=&begin_year=&end_year=&issues=all&orderby=rank&order=rev&changed=1"
  
            params = {
                'ftext': self.config['topic'],
                'pagenum': page
            }

            logger.info(f"Запрос к странице {page}: {search_url}")
            try:
                response = self.session.get(
                    search_url,
                    params=params,
                    headers=self._get_headers(),
                    timeout=self.config['timeout']
                )
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                rows = soup.find_all('tr', id=re.compile(r'^a\d+$'))
                if not rows:
                    logger.warning("Статьи не найдены на этой странице. Остановка.")
                    break

                logger.info(f"Найдено {len(rows)} статей на странице {page}")
                self._random_delay()

                for row in rows:
                    if articles_count >= self.config['num_articles']:
                        break

                    article_data = self.parse_list_item(row)
                    if article_data:
                        detailed = self.fetch_article_details(article_data['url'])
                        article_data.update(detailed)
                        
                        # Сразу записываем в CSV
                        self._write_article_to_csv(article_data)
                        articles_count += 1
                        
                        logger.info(f"[{articles_count}/{self.config['num_articles']}] Обработано: {article_data['title'][:60]}...")
                    
                    self._random_delay()

                page += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка сети: {e}")
                self._random_delay()
                continue
            except Exception as e:
                logger.error(f"Ошибка парсинга: {e}")
                break

        return articles_count

    def parse_list_item(self, row):
        """Парсинг строки из списка результатов"""
        try:
            title_tag = row.find('a', href=re.compile(r'item\.asp\?id=\d+'))
            if not title_tag:
                return None

            title = title_tag.get_text(strip=True)
            url = urljoin(self.config['base_url'], title_tag['href'])

            # Извлечение года из информации о журнале
            year = 'N/A'
            td_content = row.find('td', align='left')
            if td_content:
                text = td_content.get_text()
                year_match = re.search(r'\b(19|20)\d{2}\b', text)
                if year_match:
                    year = year_match.group(0)

            return {
                'title': self.clean_text(title),
                'year': year,
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
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            details = {
                'type': '',
                'keywords': '',
                'abstract': '',
                'in_rinc': 'нет'
            }

            # Тип публикации
            type_td = soup.find('td', string=re.compile(r'Тип:', re.IGNORECASE))
            if not type_td:
                for td in soup.find_all('td'):
                    if 'Тип:' in td.get_text():
                        type_td = td
                        break
            
            if type_td:
                type_text = type_td.get_text(strip=True)
                type_match = re.search(r'Тип:\s*(.+?)\s*(?:Язык:|$)', type_text)
                if type_match:
                    details['type'] = self.clean_text(type_match.group(1))

            # Ключевые слова - улучшенный поиск

            # Аннотация
            abstract_div = soup.find('div', id='abstract1')
            if not abstract_div:
                abstract_div = soup.find('div', id='abstract2')
            if abstract_div:
                details['abstract'] = self.clean_text(abstract_div.get_text(separator=' ', strip=True))

            # Входит в РИНЦ
            for td in soup.find_all('td'):
                td_text = td.get_text(strip=True)
                if 'Входит в РИНЦ:' in td_text:
                    if re.search(r'Входит в РИНЦ:\s*да', td_text, re.IGNORECASE):
                        details['in_rinc'] = 'да'
                    elif 'на рассмотрении' in td_text.lower():
                        details['in_rinc'] = 'на рассмотрении'
                    elif re.search(r'Входит в РИНЦ:\s*нет', td_text, re.IGNORECASE):
                        details['in_rinc'] = 'нет'
                    break

            return details

        except Exception as e:
            logger.error(f"Ошибка загрузки деталей {url}: {e}")
            return {'type': '', 'keywords': '', 'abstract': '', 'in_rinc': 'нет'}

    def clean_text(self, text):
        """Очистка текста"""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def run(self):
        logger.info("=" * 60)
        logger.info("ELIBRARY.RU SCRAPER — ЗАПУСК")
        logger.info("=" * 60)

        try:
            # Инициализируем CSV файл
            self._init_csv()
            
            # Запускаем скрапинг с последовательной записью
            articles_count = self.search_articles()

            print("\n" + "=" * 60)
            print("СКРАПИНГ ЗАВЕРШЁН")
            print("=" * 60)
            print(f"Обработано статей: {articles_count}")
            print(f"Результаты сохранены в: {self.config['output_csv']}")
            
        except KeyboardInterrupt:
            logger.warning("\nПрервано пользователем")
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
        finally:
            # Всегда закрываем файл
            self._close_csv()
            logger.info("Скрапинг завершён.")


if __name__ == "__main__":
    scraper = ElibraryScraper(CONFIG)
    scraper.run()