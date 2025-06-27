import requests
from bs4 import BeautifulSoup
import time
import random
from flask import Flask, jsonify, request
from flask_cors import CORS
import re
from urllib.parse import urljoin, quote
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

class DatabaseManager:
    def __init__(self):
        # Use environment variables for database connection
        self.db_url = os.environ.get('DATABASE_URL') or os.environ.get('SUPABASE_DB_URL')
        if not self.db_url:
            raise ValueError("DATABASE_URL or SUPABASE_DB_URL environment variable is required")
    
    def get_connection(self):
        return psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)
    
    def init_database(self):
        """Initialize database tables"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Create categories table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS categories (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        href VARCHAR(500) NOT NULL UNIQUE,
                        last_scraped TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                # Create products table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS products (
                        id SERIAL PRIMARY KEY,
                        category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE,
                        title TEXT NOT NULL,
                        affiliate_link TEXT NOT NULL,
                        summary TEXT,
                        rank INTEGER,
                        last_updated TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                conn.commit()
    
    def insert_categories(self, categories_list):
        """Insert categories from your manual list"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for category in categories_list:
                    cur.execute("""
                        INSERT INTO categories (name, href)
                        VALUES (%s, %s)
                        ON CONFLICT (href) DO UPDATE SET
                        name = EXCLUDED.name
                        RETURNING id
                    """, (category['name'], category['href']))
                conn.commit()
    
    def get_all_categories(self):
        """Get all categories from database"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM categories ORDER BY name")
                return cur.fetchall()
    
    def get_category_by_id(self, category_id):
        """Get a specific category"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM categories WHERE id = %s", (category_id,))
                return cur.fetchone()
    
    def update_category_scraped(self, category_id):
        """Update last_scraped timestamp"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE categories 
                    SET last_scraped = NOW() 
                    WHERE id = %s
                """, (category_id,))
                conn.commit()
    
    def insert_products(self, category_id, products):
        """Insert products for a category (replace existing)"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Delete existing products for this category
                cur.execute("DELETE FROM products WHERE category_id = %s", (category_id,))
                
                # Insert new products
                for rank, product in enumerate(products, 1):
                    cur.execute("""
                        INSERT INTO products (category_id, title, affiliate_link, summary, rank)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (category_id, product['title'], product['url'], product['summary'], rank))
                
                conn.commit()
    
    def get_products_by_category(self, category_id):
        """Get all products for a category"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM products 
                    WHERE category_id = %s 
                    ORDER BY rank
                """, (category_id,))
                return cur.fetchall()
    
    def get_categories_needing_update(self, hours_old=24):
        """Get categories that haven't been scraped recently"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM categories 
                    WHERE last_scraped IS NULL 
                    OR last_scraped < NOW() - INTERVAL '%s hours'
                    ORDER BY last_scraped ASC NULLS FIRST
                """, (hours_old,))
                return cur.fetchall()

class AmazonScraper:
    def __init__(self, db_manager):
        self.base_url = "https://www.amazon.com"
        self.session = requests.Session()
        self.db = db_manager
        
        # Headers to avoid being blocked
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
        }
        self.session.headers.update(self.headers)

    def get_page(self, url, retries=3):
        """Get page content with retry logic"""
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(2, 5))  # Longer delays for safety
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt == retries - 1:
                    raise
                time.sleep(random.uniform(3, 7))

    def convert_to_affiliate_link(self, product_url, affiliate_tag="your-tag-20"):
        """Convert regular Amazon URL to affiliate link"""
        # Extract ASIN from URL
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', product_url)
        if asin_match:
            asin = asin_match.group(1)
            return f"https://www.amazon.com/dp/{asin}?tag={affiliate_tag}"
        return product_url  # Return original if can't convert

    def extract_product_info(self, product_element, rank):
        """Extract product information from a product element"""
        try:
            # Find product link
            link_elem = product_element.find('a', href=True)
            if not link_elem:
                return None
            
            product_url = urljoin(self.base_url, link_elem['href'])
            affiliate_url = self.convert_to_affiliate_link(product_url)
            
            # Find product title
            title_selectors = [
                'h2', 'span[class*="title"]', '.s-title-instructions-style',
                'a[class*="title"]', '.a-text-normal'
            ]
            
            title = "No title available"
            for selector in title_selectors:
                title_elem = product_element.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if title and len(title) > 3:
                        break
            
            # Extract summary/description
            summary = "No description available"
            desc_selectors = [
                '.a-size-small', '.a-color-secondary', 
                'span[class*="review"]', 'span[class*="rating"]',
                '.a-text-subtle'
            ]
            
            for selector in desc_selectors:
                desc_elem = product_element.select_one(selector)
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True)
                    if desc_text and len(desc_text) > 10 and 'stars' not in desc_text.lower():
                        summary = desc_text[:200] + "..." if len(desc_text) > 200 else desc_text
                        break
            
            return {
                'title': title,
                'url': affiliate_url,
                'summary': summary,
                'rank': rank
            }
            
        except Exception as e:
            print(f"Error extracting product info: {str(e)}")
            return None

    def scrape_category_products(self, category_href, limit=10):
        """Scrape products for a specific category"""
        try:
            full_url = urljoin(self.base_url, category_href)
            print(f"Scraping products from: {full_url}")
            
            response = self.get_page(full_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            products = []
            
            # Multiple selectors for different page layouts
            product_selectors = [
                'li[id^="zg-ordered-list"]',  # Bestseller list items
                '.zg-item-immersion',
                'div[data-component-type="s-search-result"]',
                '.s-result-item',
                'li[class*="zg-item"]'
            ]
            
            product_elements = []
            for selector in product_selectors:
                elements = soup.select(selector)
                if elements:
                    product_elements = elements[:limit]
                    print(f"Found {len(elements)} products using selector: {selector}")
                    break
            
            if not product_elements:
                print("No products found with standard selectors, trying fallback...")
                product_elements = soup.find_all(['div', 'li'], class_=re.compile(r'.*item.*|.*product.*', re.I))[:limit]
            
            for i, element in enumerate(product_elements):
                product_info = self.extract_product_info(element, i + 1)
                if product_info and product_info['title'] != "No title available":
                    products.append(product_info)
                    print(f"  {i + 1}. {product_info['title'][:50]}...")
                
                if len(products) >= limit:
                    break
            
            print(f"Successfully scraped {len(products)} products")
            return products
            
        except Exception as e:
            print(f"Error scraping category {category_href}: {str(e)}")
            return []

    def scrape_and_store_category(self, category_id):
        """Scrape products for a category and store in database"""
        category = self.db.get_category_by_id(category_id)
        if not category:
            return False
        
        print(f"Scraping category: {category['name']}")
        products = self.scrape_category_products(category['href'])
        
        if products:
            self.db.insert_products(category_id, products)
            self.db.update_category_scraped(category_id)
            print(f"Stored {len(products)} products for {category['name']}")
            return True
        
        return False

# Initialize database and scraper
try:
    db_manager = DatabaseManager()
    db_manager.init_database()
    scraper = AmazonScraper(db_manager)
    print("Database and scraper initialized successfully")
except Exception as e:
    print(f"Failed to initialize: {e}")
    db_manager = None
    scraper = None

# API Routes
@app.route('/api/categories', methods=['GET'])
def get_all_categories():
    """Get all categories from database"""
    try:
        categories = db_manager.get_all_categories()
        return jsonify({
            'success': True,
            'data': [dict(cat) for cat in categories],
            'count': len(categories)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/categories/<int:category_id>/products', methods=['GET'])
def get_category_products(category_id):
    """Get products for a specific category"""
    try:
        category = db_manager.get_category_by_id(category_id)
        if not category:
            return jsonify({'success': False, 'error': 'Category not found'}), 404
        
        products = db_manager.get_products_by_category(category_id)
        
        return jsonify({
            'success': True,
            'category': dict(category),
            'products': [dict(prod) for prod in products],
            'count': len(products)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/scrape/<int:category_id>', methods=['POST'])
def scrape_category(category_id):
    """Manually trigger scraping for a specific category"""
    try:
        success = scraper.scrape_and_store_category(category_id)
        if success:
            return jsonify({'success': True, 'message': 'Category scraped successfully'})
        else:
            return jsonify({'success': False, 'error': 'Failed to scrape category'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/scrape/batch', methods=['POST'])
def scrape_batch():
    """Scrape multiple categories that need updating"""
    try:
        limit = request.json.get('limit', 5) if request.json else 5
        categories = db_manager.get_categories_needing_update()[:limit]
        
        results = []
        for category in categories:
            try:
                success = scraper.scrape_and_store_category(category['id'])
                results.append({
                    'category_id': category['id'],
                    'name': category['name'],
                    'success': success
                })
                # Longer delay between categories
                time.sleep(random.uniform(10, 15))
            except Exception as e:
                results.append({
                    'category_id': category['id'],
                    'name': category['name'],
                    'success': False,
                    'error': str(e)
                })
        
        return jsonify({
            'success': True,
            'results': results,
            'processed': len(results)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/setup/categories', methods=['POST'])
def setup_categories():
    """Setup initial categories from your manual list"""
    try:
        # You'll need to pass your 90 categories here
        categories_data = request.json.get('categories', [])
        
        if not categories_data:
            return jsonify({
                'success': False, 
                'error': 'No categories provided. Send array of {name, href} objects'
            }), 400
        
        db_manager.insert_categories(categories_data)
        
        return jsonify({
            'success': True,
            'message': f'Inserted {len(categories_data)} categories',
            'count': len(categories_data)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        'success': True,
        'message': 'Amazon Scraper with Database is running',
        'database_connected': db_manager is not None
    })

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'message': 'Amazon Bestsellers Scraper API with Database',
        'endpoints': {
            'GET /api/categories': 'Get all categories from database',
            'GET /api/categories/{id}/products': 'Get products for a category',
            'POST /api/scrape/{id}': 'Scrape a specific category',
            'POST /api/scrape/batch': 'Scrape multiple categories needing update',
            'POST /api/setup/categories': 'Setup initial categories'
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)