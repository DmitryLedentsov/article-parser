import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from urllib.parse import urljoin, quote_plus
import PyPDF2
from io import BytesIO
from keybert import KeyBERT
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
CONFIG = {
    'topic': 'multiplayer game engine architecture',
    'num_articles': 20,  # Общее количество статей для сбора
    'start': 0,  # Начальная позиция поиска
    'filter_keywords': ['multiplayer', 'game engine', 'architecture', 'network'],
    'num_keywords_to_extract': 10,  # Количество ключевых слов для извлечения
    'output_csv': 'scholar_articles.csv',
    'delay': 5  # Задержка между запросами в секундах
}

class ScholarScraper:
    def __init__(self, config):
        self.config = config
        self.base_url = 'https://scholar.google.com'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.kw_model = KeyBERT()
        
    def search_articles(self):
        """Поиск статей на Google Scholar"""
        articles = []
        start = self.config['start']
        num_articles = self.config['num_articles']
        topic = self.config['topic']
        
        logger.info(f"Начинаем поиск статей по теме: {topic}")
        
        while len(articles) < num_articles:
            search_url = f"{self.base_url}/scholar?start={start}&q={quote_plus(topic)}&hl=en&as_sdt=0,5"
            logger.info(f"Запрос к: {search_url}")
            
            try:
                response = requests.get(search_url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                results = soup.find_all('div', class_='gs_r gs_or gs_scl')
                
                if not results:
                    logger.warning("Статьи не найдены на этой странице")
                    break
                
                logger.info(f"Найдено {len(results)} результатов на странице")
                
                for result in results:
                    if len(articles) >= num_articles:
                        break
                    
                    article_data = self.parse_article(result)
                    if article_data:
                        articles.append(article_data)
                        logger.info(f"Обработана статья {len(articles)}/{num_articles}: {article_data['title']}")
                
                start += 10
                time.sleep(self.config['delay'])
                
            except Exception as e:
                logger.error(f"Ошибка при запросе: {e}")
                break
        
        return articles
    
    def parse_article(self, result):
        """Парсинг данных статьи"""
        try:
            # Заголовок
            title_tag = result.find('h3', class_='gs_rt')
            if not title_tag:
                return None
            
            title_link = title_tag.find('a')
            title = title_link.get_text() if title_link else title_tag.get_text()
            title = self.clean_text(title)
            url = title_link['href'] if title_link and 'href' in title_link.attrs else ''
            
            # Год публикации
            meta_tag = result.find('div', class_='gs_a')
            year = self.extract_year(meta_tag.get_text() if meta_tag else '')
            
            # Проверка фильтров
            if not self.check_filters(title):
                logger.info(f"Статья отфильтрована: {title}")
                return None
            
            # Извлечение текста и ключевых слов
            text_content = self.fetch_article_content(url)
            keywords = self.extract_keywords(text_content) if text_content else []
            
            return {
                'title': title,
                'year': year,
                'url': url,
                'keywords': ', '.join(keywords)
            }
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге статьи: {e}")
            return None
    
    def fetch_article_content(self, url):
        """Получение текста статьи (HTML или PDF)"""
        if not url:
            return ""
        
        try:
            logger.info(f"Загрузка содержимого: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            content_type = response.headers.get('Content-Type', '').lower()
            
            if 'application/pdf' in content_type or url.endswith('.pdf'):
                return self.extract_pdf_text(response.content)
            else:
                return self.extract_html_text(response.content)
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке содержимого {url}: {e}")
            return ""
    
    def extract_html_text(self, html_content):
        """Извлечение текста из HTML"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Удаляем скрипты и стили
            for script in soup(['script', 'style', 'nav', 'header', 'footer']):
                script.decompose()
            
            # Извлекаем текст
            text = soup.get_text(separator=' ', strip=True)
            text = ' '.join(text.split())  # Убираем лишние пробелы
            
            # Ограничиваем длину для анализа
            return text[:10000]
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении HTML текста: {e}")
            return ""
    
    def extract_pdf_text(self, pdf_content):
        """Извлечение текста из PDF"""
        try:
            pdf_file = BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            # Читаем первые несколько страниц
            for page_num in range(min(5, len(pdf_reader.pages))):
                page = pdf_reader.pages[page_num]
                text += page.extract_text() + " "
            
            return text[:10000]
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении PDF текста: {e}")
            return ""
    
    def extract_keywords(self, text):
        """Извлечение ключевых слов с помощью KeyBERT"""
        if not text or len(text) < 100:
            return []
        
        try:
            keywords = self.kw_model.extract_keywords(
                text,
                keyphrase_ngram_range=(1, 3),
                stop_words='english',
                top_n=self.config['num_keywords_to_extract']
            )
            
            return [kw[0] for kw in keywords]
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении ключевых слов: {e}")
            return []
    
    def check_filters(self, title):
        """Проверка статьи по ключевым словам фильтра"""
        if not self.config.get('filter_keywords'):
            return True
        
        title_lower = title.lower()
        return any(keyword.lower() in title_lower for keyword in self.config['filter_keywords'])
    
    def extract_year(self, text):
        """Извлечение года из текста"""
        match = re.search(r'\b(19|20)\d{2}\b', text)
        return match.group(0) if match else 'N/A'
    
    def clean_text(self, text):
        """Очистка текста от лишних символов"""
        text = re.sub(r'\[PDF\]|\[HTML\]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def save_to_csv(self, articles):
        """Сохранение данных в CSV"""
        if not articles:
            logger.warning("Нет статей для сохранения")
            return
        
        csv_file = self.config['output_csv']
        logger.info(f"Сохранение {len(articles)} статей в {csv_file}")
        
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['title', 'year', 'url', 'keywords']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                writer.writeheader()
                for article in articles:
                    writer.writerow(article)
            
            logger.info(f"Данные успешно сохранены в {csv_file}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении в CSV: {e}")
    
    def run(self):
        """Запуск скрапера"""
        logger.info("=" * 50)
        logger.info("Запуск Google Scholar Scraper")
        logger.info("=" * 50)
        
        articles = self.search_articles()
        
        logger.info(f"Всего собрано статей: {len(articles)}")
        
        if articles:
            self.save_to_csv(articles)
            
            # Вывод результатов
            print("\n" + "=" * 50)
            print("РЕЗУЛЬТАТЫ")
            print("=" * 50)
            for i, article in enumerate(articles, 1):
                print(f"\n{i}. {article['title']}")
                print(f"   Год: {article['year']}")
                print(f"   URL: {article['url']}")
                print(f"   Ключевые слова: {article['keywords']}")
        
        logger.info("Скрапинг завершен")


if __name__ == "__main__":
    scraper = ScholarScraper(CONFIG)
    scraper.run()