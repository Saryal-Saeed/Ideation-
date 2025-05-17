import requests
from bs4 import BeautifulSoup
import json
import time
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# TechCrunch category URLs
CATEGORIES = {
    "AI": "https://techcrunch.com/category/artificial-intelligence/",
    "Venture": "https://techcrunch.com/category/venture/",
    "Apps": "https://techcrunch.com/category/apps/"
}

# User-Agents list for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

# Retry session for robust requests
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))
session.mount('http://', HTTPAdapter(max_retries=retries))

# Date range: past 30 days
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

def extract_date(article):
    """Extracts and formats the article date."""
    date_tag = article.select_one("time")
    if date_tag and date_tag.has_attr("datetime"):
        try:
            date = datetime.fromisoformat(date_tag["datetime"]).replace(tzinfo=None)
            return date
        except ValueError:
            print(f"Invalid date format: {date_tag['datetime']}")
    return None

def get_article_details(url):
    """Fetches article details (title & content) from a given URL."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    response = session.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch article: {url}")
        return None, None

    soup = BeautifulSoup(response.text, "html.parser")
    title_tag = soup.select_one(".wp-block-post-title")
    title = title_tag.get_text(strip=True) if title_tag else "No Title Found"
    
    paragraphs = soup.select(".wp-block-post-content-is-layout-constrained p")
    content = "\n".join([p.get_text(strip=True) for p in paragraphs])
    
    return title, content

def scrape_articles(category, page_url):
    """Scrapes articles from a given category page."""
    articles = []
    while page_url:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        response = session.get(page_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch {page_url}")
            break
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        for article in soup.select(".loop-card--default"):
            date = extract_date(article)
            if not date:
                print("Skipping article due to missing date.")
                continue
            if date < start_date:
                print("Reached older articles. Stopping scrape.")
                return articles, None
            
            title_tag = article.select_one(".loop-card--default .loop-card__title-link")
            if title_tag:
                link = title_tag["href"]
                title, content = get_article_details(link)
                articles.append({
                    "category": category,
                    "title": title,
                    "url": link,
                    "content": content,
                    "date": date.strftime("%Y-%m-%d")
                })
        
        next_page_tag = soup.select_one(".wp-block-query-pagination-next")
        page_url = urljoin(page_url, next_page_tag["href"]) if next_page_tag else None
        print(f"Next page: {page_url}")
        time.sleep(random.uniform(2, 5))
    
    return articles, None

def scrape_all_categories():
    """Scrapes all defined categories and saves the data."""
    all_articles = []
    for category, url in CATEGORIES.items():
        print(f"Scraping {category} articles...")
        articles, _ = scrape_articles(category, url)
        all_articles.extend(articles)
    
    with open("techcrunch_articles_data.json", "w") as file:
        json.dump(all_articles, file, indent=4)
    
    print(f"Scraped {len(all_articles)} articles from the last month.")

scrape_all_categories()