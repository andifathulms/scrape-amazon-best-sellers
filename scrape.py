import json
import time
import requests

from constants import SCRAPEOWL_API_KEY, SCRAPEOWL_API_URL, SECTION_SELECTOR, PRODUCT_SELECTOR, BASE_URL
from db import save_category_to_db, setup_categories_database, get_category_by_id, check_products_exist_by_category, delete_products_by_category
from utils import extract_url_from_anchor, process_products

def fetch_page(url, selectors, max_retries=3, render_js=False):
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
        "json_response": True,
        "render_js": render_js
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

def scrape_category(url, depth, conn, parent_id=None, depth1_progress=None, depth2_progress=None, depth3_progress=None):
    """
    Scrape the category recursively up to 3 levels deep.
    Stores the category and URL in the database, along with the parent ID.
    Prints detailed progress at each depth.

    :param url: The URL of the category to scrape.
    :param depth: Current depth in the recursion (1 for root).
    :param conn: Database connection object.
    :param parent_id: ID of the parent category.
    :param depth1_progress: Progress tracker for depth-1 categories.
    :param depth2_progress: Progress tracker for depth-2 categories.
    :param depth3_progress: Progress tracker for depth-3 categories.
    :return: Updated progress counts for depth-1, depth-2, and depth-3.
    """
    if depth > 3:
        return depth1_progress, depth2_progress, depth3_progress

    print(f"Scraping category: {url} (Depth: {depth})")
    response = fetch_page(url, selectors=[SECTION_SELECTOR], render_js=True)

    if 'data' not in response or len(response['data']) == 0:
        print(f"No category data found at depth {depth}.")
        return depth1_progress, depth2_progress, depth3_progress

    results = response['data'][0]['results']

    # Initialize progress counters if not already set
    if depth1_progress is None:
        depth1_progress = {"current": 0, "total": 0}
    if depth2_progress is None:
        depth2_progress = {"current": 0, "total": 0}
    if depth3_progress is None:
        depth3_progress = {"current": 0, "total": 0}

    # Determine which progress counter to update based on depth
    if depth == 1:
        depth1_progress["total"] += len(results)
    elif depth == 2:
        depth2_progress["total"] += len(results)
    elif depth == 3:
        depth3_progress["total"] += len(results)

    for result in results:
        category_name = result['text']
        category_url = extract_url_from_anchor(result['html'])

        if category_url:
            if depth == 1:
                depth1_progress["current"] += 1
            elif depth == 2:
                depth2_progress["current"] += 1
            elif depth == 3:
                depth3_progress["current"] += 1

            # Save the category and retrieve its ID
            current_category_id = save_category_to_db(conn, category_name, category_url, parent_id)

            # Show progress after saving each category
            print(f"Progress: Depth-{depth} -> {depth1_progress['current']}/{depth1_progress['total']} depth-1 categories, "
                  f"{depth2_progress['current']}/{depth2_progress['total']} depth-2 categories, "
                  f"{depth3_progress['current']}/{depth3_progress['total']} depth-3 categories.")
            print(f"Found category: {category_name} -> {category_url}")

            if current_category_id:
                # Recursive call to go deeper into subcategories with the current category as parent
                depth1_progress, depth2_progress, depth3_progress = scrape_category(
                    category_url, depth + 1, conn, current_category_id,
                    depth1_progress, depth2_progress, depth3_progress)

    # Completion message when a depth level is finished
    if depth == 1 and depth1_progress["current"] == depth1_progress["total"]:
        print(f"Finished all depth-1 categories. Total: {depth1_progress['total']}")
    elif depth == 2 and depth2_progress["current"] == depth2_progress["total"]:
        print(f"Finished depth-2 categories for depth-1 category. Total: {depth2_progress['total']}")
    elif depth == 3 and depth3_progress["current"] == depth3_progress["total"]:
        print(f"Finished depth-3 categories for depth-2 category. Total: {depth3_progress['total']}")

    return depth1_progress, depth2_progress, depth3_progress

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
        depth1_progress, depth2_progress, depth3_progress = scrape_category(url, depth=1, conn=conn)
        print(f"Scraping completed: Depth-1: {depth1_progress['current']}/{depth1_progress['total']}, "
              f"Depth-2: {depth2_progress['current']}/{depth2_progress['total']}, "
              f"Depth-3: {depth3_progress['current']}/{depth3_progress['total']}")
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
