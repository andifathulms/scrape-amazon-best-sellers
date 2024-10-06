import sqlite3

# Centralized function for database connection
def connect_to_db(db_name):
    """
    Establish a connection to the given SQLite database.
    
    :param db_name: The name of the database (e.g., 'categories.db', 'products.db').
    :return: SQLite connection object.
    """
    return sqlite3.connect(db_name)

# Centralized function for table creation
def create_table(conn, create_table_sql):
    """
    Create a table using the provided SQL statement.
    
    :param conn: Connection object.
    :param create_table_sql: SQL statement to create a table.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

# Set up the categories table
def setup_categories_table(conn):
    create_table_sql = '''
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_name TEXT,
            url TEXT UNIQUE,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES categories (id)
        );
    '''
    create_table(conn, create_table_sql)

# Set up the products table
def setup_products_table(conn):
    """
    Creates the products table if it does not exist.
    The table includes the category ID foreign key.
    """
    create_table_sql = '''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            rating TEXT,
            price TEXT,
            category_id INTEGER
        )
    '''
    create_table(conn, create_table_sql)

# Centralized query function
def query_database(conn, query):
    """
    Executes a given SQL query and returns the results.
    
    :param conn: Connection object.
    :param query: SQL query to execute.
    :return: List of rows fetched from the query.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []

# Query categories
def query_categories():
    conn = connect_to_db('categories.db')

    # Query to get the category information along with its parent category
    query = '''
        SELECT c.id, c.category_name, c.url, p.category_name AS parent_name
        FROM categories c
        LEFT JOIN categories p ON c.parent_id = p.id
    '''

    results = query_database(conn, query)

    # Loop through the results and print the details
    for row in results:
        category_id = row[0]
        category_name = row[1]
        url = row[2]
        parent_name = row[3] if row[3] else "No Parent"  # Handle cases where there's no parent
        
        print(f"ID: {category_id}, Category: {category_name}, URL: {url}, Parent: {parent_name}")
    
    conn.close()

# Query products
def query_products():
    """
    Queries the products table and returns the product information along with the category ID.
    """
    conn = connect_to_db('products.db')
    
    # Select product details along with category_id
    query = "SELECT id, title, rating, price, category_id FROM products"
    
    # Execute the query and fetch results
    results = query_database(conn, query)
    
    # Display the products with category_id included
    for row in results:
        print(f"ID: {row[0]}, Title: {row[1]}, Rating: {row[2]}, Price: {row[3]}, Category ID: {row[4]}")
    
    # Close the connection
    conn.close()

def query_products_by_category(category_id):
    """
    Query and display all products that belong to the given category ID.
    
    :param category_id: The ID of the category to filter products by.
    """
    conn = connect_to_db('products.db')
    
    try:
        # Prepare the SQL query to select products by category_id
        query = "SELECT * FROM products WHERE category_id = ?"
        cursor = conn.cursor()
        cursor.execute(query, (category_id,))
        
        # Fetch the results
        results = cursor.fetchall()

        # Check if any results were found
        if not results:
            print(f"No products found for category ID: {category_id}")
            return
        
        # Print the results
        for row in results:
            print(f"ID: {row[0]}, Title: {row[1]}, Rating: {row[2]}, Price: {row[3]}, Category ID: {row[4]}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")

    finally:
        # Close the database connection
        conn.close()

def check_products_exist_by_category(category_id):
    """
    Checks if products exist for a given category ID.
    Returns False if the products table doesn't exist yet.

    :param category_id: The category ID to check.
    :return: True if products exist, False otherwise.
    """
    conn = connect_to_db('products.db')
    cursor = conn.cursor()

    try:
        # Check if products exist for the given category_id
        query = "SELECT COUNT(*) FROM products WHERE category_id = ?"
        cursor.execute(query, (category_id,))
        count = cursor.fetchone()[0]
    except sqlite3.OperationalError as e:
        # This error occurs if the 'products' table doesn't exist
        if 'no such table: products' in str(e):
            print("No products table found. Initializing the database.")
            setup_products_database()
            conn.close()
            return False
        else:
            raise e  # If it's a different error, re-raise it
    finally:
        conn.close()

    return count > 0  # Returns True if products exist, otherwise False

def delete_products_by_category(category_id):
    """
    Deletes all products associated with a given category ID.
    :param category_id: The category ID whose products will be deleted.
    """
    conn = connect_to_db('products.db')
    cursor = conn.cursor()

    query = "DELETE FROM products WHERE category_id = ?"
    cursor.execute(query, (category_id,))
    conn.commit()

    conn.close()

    print(f"All products for category ID {category_id} have been deleted.")

# Get category by ID
def get_category_by_id(category_id):
    conn = connect_to_db('categories.db')
    cursor = conn.cursor()
    cursor.execute('SELECT category_name, url FROM categories WHERE id = ?', (category_id,))
    row = cursor.fetchone()
    conn.close()
    return row if row else None

# Save category to the database
def save_category_to_db(conn, category_name, url, parent_id=None):
    """
    Save category name, URL, and parent ID into the database.
    Returns the ID of the inserted category.
    """
    try:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO categories (category_name, url, parent_id) VALUES (?, ?, ?)', 
                       (category_name, url, parent_id))
        conn.commit()
        return cursor.lastrowid  # Return the ID of the newly inserted category
    except sqlite3.IntegrityError:
        print(f"Category '{category_name}' already exists in the database.")
        return None  # Return None if the category already exists

# Save product to the database
def save_product_to_db(title, rating, price, category_id):
    """
    Saves a product to the database along with its category ID.

    :param title: Product title.
    :param rating: Product rating (can be None if missing).
    :param price: Product price (can be None if missing).
    :param category_id: The category ID that this product belongs to.
    """
    conn = connect_to_db('products.db')
    setup_products_table(conn)  # Ensure the products table exists before saving
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO products (title, rating, price, category_id) 
        VALUES (?, ?, ?, ?)
    ''', (title, rating, price, category_id))
    
    conn.commit()
    conn.close()

def setup_categories_database():
    conn = connect_to_db('categories.db')
    setup_categories_table(conn)
    return conn

def setup_products_database():
    conn = connect_to_db('products.db')
    setup_products_table(conn)
    conn.close()

def print_categories_hierarchy(conn, parent_id=None, level=0):
    """
    Prints the categories in a hierarchical format, with children indented.
    
    :param conn: Database connection object.
    :param parent_id: ID of the parent category (None for top-level categories).
    :param level: The depth level (used for indentation).
    """
    cursor = conn.cursor()
    
    # Query to get categories with the specified parent_id (NULL for top-level categories)
    query = '''
        SELECT id, category_name 
        FROM categories
        WHERE parent_id IS ? 
    '''
    cursor.execute(query, (parent_id,))
    categories = cursor.fetchall()

    # Loop through categories and print them
    for category in categories:
        category_id, category_name = category
        indent = ' ' * (level * 4)  # Indentation for pretty print
        print(f"{indent}- {category_name} (ID: {category_id})")
        
        # Recursively print child categories
        print_categories_hierarchy(conn, parent_id=category_id, level=level + 1)

# Example of how to use this function in your main code
def display_category_tree():
    conn = connect_to_db('categories.db')
    print("Category Hierarchy:")
    print_categories_hierarchy(conn)
    conn.close()

if __name__ == '__main__':
    query_categories()
    query_products()
