from scrape import scrape_category, fetch_products_by_category, scrape_categories_initial
from db import (query_categories, query_products, query_products_by_category,
                check_products_exist_by_category, display_category_tree)

def main():
    """
    Main function to provide the user with a menu to:
    1. Scrape categories and URLs (one-time process)
    2. Query all categories
    3. Display Category Tree
    4. Scrape products by category
    5. Query all products
    6. Query products by category
    """
    conn = None

    while True:
        # Display the options to the user
        print("\n--- Menu ---")
        print("1. Scrape categories and URLs (one-time process)")
        print("2. Query all categories")
        print("3. Display category tree")
        print("4. Scrape products by category")
        print("5. Query all products")
        print("6. Query products by category")
        print("7. Exit")

        choice = input("Enter your choice (1-7): ")

        if choice == '1':
            proceed = input("This will scrape categories and URLs. Do you want to proceed? (yes/no): ").strip().lower()
            if proceed == 'yes':
                # Perform the category scraping (one-time process)
                scrape_categories_initial()
                print("Scraping complete!")
            else:
                print("Operation aborted.")

        elif choice == "2":
            # Option 2: Query all categories
            query_categories()

        elif choice == "3":
            display_category_tree()

        elif choice == '4':
            try:
                category_id = int(input("Enter the category ID to scrape products from: "))

                # Proceed with scraping if user confirms or if no products exist
                fetch_products_by_category(category_id)
            
            except ValueError:
                print("Invalid category ID.")

        elif choice == "5":
            # Option 4: Query all products
            query_products()

        elif choice == "6":
            # Option 5: Query products by category
            try:
                category_id = int(input("Enter the category ID to query products: "))
                query_products_by_category(category_id)
            except ValueError:
                print("Invalid input. Please enter a valid category ID.")

        elif choice == "7":
            # Exit the program
            print("Exiting the program.")
            break

        else:
            print("Invalid choice. Please choose a valid option (1-6).")

    # Close the connection if it was opened
    if conn:
        conn.close()


if __name__ == '__main__':
    main()
