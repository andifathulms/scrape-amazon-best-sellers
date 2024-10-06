import re

from constants import BASE_URL
from db import save_product_to_db


def process_products(results, category_id):
    """
    Processes the list of results and extracts product information.
    Handles cases where data is missing (like title, rating, or price).
    Saves the products along with the category ID.

    :param results: List of results scraped from the page.
    :param category_id: The category ID to associate the products with.
    """
    title = None
    rating = None
    price = None

    for result in results:
        text = result['text'].strip()  # Clean the text

        # Check if the result is likely a title (it doesn't contain stars or $)
        if text and not 'stars' in text and not '$' in text:
            # If we encounter a new title, save the previous product (if it exists)
            if title is not None:
                save_product_to_db(title, rating, price, category_id)

            # Start a new product
            title = text
            rating = None
            price = None

        # Check if the result is likely a rating (it contains 'stars')
        elif 'stars' in text:
            rating = text

        # Check if the result is likely a price (it contains '$')
        elif '$' in text:
            price = text

    # Save the last product after the loop ends
    if title is not None:
        save_product_to_db(title, rating, price, category_id)

def extract_url_from_anchor(html_string):
    """
    Extracts URL from anchor tag in HTML string.
    """
    match = re.search(r'href="([^"]+)"', html_string)
    if match:
        href_value = match.group(1)
        return BASE_URL + href_value if not href_value.startswith("http") else href_value

    return None