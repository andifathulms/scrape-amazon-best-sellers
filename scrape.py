import json
import time
import requests

from constants import SCRAPEOWL_API_KEY, SCRAPEOWL_API_URL, SECTION_SELECTOR, PRODUCT_SELECTOR, BASE_URL
from db import save_category_to_db, setup_categories_database, get_category_by_id, check_products_exist_by_category, delete_products_by_category
from utils import extract_url_from_anchor, process_products

def fetch_page(url, selectors, max_retries=3):
    """
    Fetches the page content from the given URL using ScrapeOwl API.
    Retries up to `max_retries` times if a server-side error occurs.

    :param url: The URL of the page to scrape.
    :param selectors: A list of CSS selectors to scrape.
    :param max_retries: Number of retry attempts if an error occurs.
    :return: JSON response from ScrapeOwl.
    """
    # Ensure selectors are provided as a list
    if not isinstance(selectors, list):
        raise ValueError("Selectors must be provided as a list.")

    # Build the elements list from the selectors
    elements = [{"type": "css", "selector": selector, "html": True} for selector in selectors]

    object_of_data = {
        "api_key": SCRAPEOWL_API_KEY,
        "url": url,
        "elements": elements,
        "json_response": True
    }

    data = json.dumps(object_of_data)
    headers = {"Content-Type": "application/json"}

    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.post(SCRAPEOWL_API_URL, data, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Attempt {attempt+1} failed with status code {response.status_code}")
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error on attempt {attempt+1}: {e}")
            attempt += 1
            if attempt < max_retries:
                print(f"Retrying... ({attempt+1}/{max_retries})")
                time.sleep(2)
            else:
                raise Exception(f"Failed to fetch page after {max_retries} attempts")

def scrape_category(url, depth, conn):
    """
    Scrape the category recursively up to 3 levels deep.
    Stores the category and URL in the database.
    """
    if depth > 3:
        return

    print(f"Scraping category: {url} (Depth: {depth})")
    response = fetch_page(url, selectors=[SECTION_SELECTOR])
    data = response['data']
    print(data)
    results = data[0]['results']

    for result in results:
        category_name = result['text']
        category_url = extract_url_from_anchor(result['html'])

        if category_url:
            print(f"Found category: {category_name} -> {category_url}")

            # Save to the database
            save_category_to_db(conn, category_name, category_url)

            # Recursive call to go deeper into subcategories
            scrape_category(category_url, depth + 1, conn)

def scrape_categories_initial():
    """
    Main function to scrape categories and save them into the database.
    This is a one-time process.
    """
    # Set up the database connection
    conn = setup_categories_database()

    # Start scraping from the base URL
    try:
        url = BASE_URL + "gp/bestsellers/"
        scrape_category(url, depth=1, conn=conn)
    except Exception as e:
        print(f"Error fetching the page: {e}")
    finally:
        conn.close()

def fetch_products_by_category(category_id):
    """
    Fetches products from a category by its ID, deletes old products if any, scrapes product data, and processes it.

    :param category_id: The ID of the category to fetch products from.
    """
    # Check if products already exist for this category
    if check_products_exist_by_category(category_id):
        confirm = input(f"Products for category ID {category_id} already exist. This will delete all previous products. Do you want to continue? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Operation aborted.")
            return
        else:
            # Delete existing products for the category
            delete_products_by_category(category_id)

    # Get the category information by ID (name and URL)
    result = get_category_by_id(category_id)
    
    if result is None:
        print(f"Category with ID {category_id} not found.")
        return

    category_name, url = result
    print(f"Category Name: {category_name}, URL: {url}")

    # Fetch product data from the category URL using the provided product selector
    response = fetch_page(url, [PRODUCT_SELECTOR])
    if 'data' not in response or len(response['data']) == 0:
        print("No product data found for this category.")
        return

    data = response['data']
    results = data[0]['results']

    # Process the products using the provided function
    process_products(results, category_id)
