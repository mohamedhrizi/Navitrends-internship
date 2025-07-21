import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from urllib.parse import urlparse, urljoin
from googlesearch import search
import concurrent.futures
import threading

class UKURLCollector:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
        self.collected_urls = set()
        self.lock = threading.Lock()
        
        # Annuaires B2B étendus
        self.directories = [
            'https://www.yell.com',
            'https://www.freeindex.co.uk',
            'https://www.thomsonlocal.com',
            'https://www.hotfrog.co.uk',
            'https://www.scoot.co.uk',
            'https://www.cylex-uk.co.uk',
            'https://uk.kompass.com',
            'https://www.touch-local.com',
            'https://www.find-open.co.uk',
            'https://www.opendi.co.uk',
            'https://www.gov.uk/government/organisations',
            'https://www.bbc.co.uk/news',
            'https://www.britishcouncil.org'
        ]

    def is_uk_domain(self, url):
        """Vérifie si l'URL est un domaine UK"""
        try:
            domain = urlparse(url).netloc.lower()
            return '.uk' in domain or '.co.uk' in domain
        except:
            return False

    def google_search(self, query, num_results=50):
        """Recherche Google simple"""
        try:
            print(f"🔍 Google: '{query}'")
            results = search(query, num_results=num_results, sleep_interval=2)
            
            uk_urls = []
            for url in results:
                if self.is_uk_domain(url):
                    uk_urls.append(url)
                    
            return uk_urls
            
        except Exception as e:
            print(f"❌ Erreur Google: {e}")
            # Fallback: chercher directement avec requests
            return self.fallback_search(query)

    def scrape_directory(self, url):
        """Scrape un annuaire"""
        try:
            print(f"📂 Scraping {url}")
            headers = {'User-Agent': random.choice(self.user_agents)}
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links = set()
            for a in soup.find_all('a', href=True):
                href = a['href']
                
                if href.startswith('/'):
                    full_url = urljoin(url, href)
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                
                if self.is_uk_domain(full_url):
                    # Nettoyer l'URL
                    clean_url = full_url.split('?')[0].split('#')[0]
                    links.add(clean_url)
                    
            return links
            
        except Exception as e:
            print(f"❌ Erreur {url}: {e}")
            return set()

    def collect_from_google(self):
        """Collecte depuis Google avec threading"""
        queries = [
            'site:.co.uk "digital agency"',
            'site:.uk "web design"', 
            'site:.co.uk "e-commerce"',
            'site:.uk "cloud services"',
            'site:.co.uk "marketing agency"',
            'site:.uk "restaurant"',
            'site:.co.uk "hotel"',
            'site:.uk "legal services"',
            'site:.co.uk "accounting"',
            'site:.uk "construction"'
        ]
        
        def search_worker(query):
            urls = self.google_search(query, 50)
            with self.lock:
                self.collected_urls.update(urls)
            return len(urls)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(search_worker, query) for query in queries]
            for future in concurrent.futures.as_completed(futures):
                count = future.result()
                print(f"→ {count} URLs | Total: {len(self.collected_urls)}")

    def collect_from_directories(self):
        """Collecte depuis les annuaires avec threading"""
        def directory_worker(directory):
            urls = self.scrape_directory(directory)
            with self.lock:
                self.collected_urls.update(urls)
            return len(urls)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(directory_worker, directory) for directory in self.directories]
            for future in concurrent.futures.as_completed(futures):
                count = future.result()
                print(f"→ {count} URLs | Total: {len(self.collected_urls)}")

    def save_results(self, filename="uk_sites.csv"):
        """Sauvegarde en CSV"""
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['url', 'domain'])
            
            for url in sorted(self.collected_urls):
                domain = urlparse(url).netloc
                writer.writerow([url, domain])
                
        print(f"✅ {len(self.collected_urls)} URLs sauvées dans {filename}")

    def run(self, target=1000):
        """Lance la collecte"""
        print(f"🚀 Collecte URLs UK (objectif: {target})")
        
        # Uniquement scraping d'annuaires (Google est bloqué)
        print("\n📍 SCRAPING DIRECT des annuaires")
        self.collect_from_directories()
        
        print(f"\n🏁 Collecte terminée: {len(self.collected_urls)} URLs")
        self.save_results()

# Lancement
if __name__ == "__main__":
    collector = UKURLCollector()
    collector.run(1000)