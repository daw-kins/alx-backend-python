import mysql.connector
from mysql.connector import Error

def paginate_users(page_size, offset):
    """Fetch a specific page of users from the user_data table."""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  # Replace with your MySQL username
            password="qwertyuiop",  # Replace with your actual MySQL password
            database="ALX_prodev"
        )
        if connection.is_connected():
            cursor = connection.cursor()
            query = "SELECT user_id, name, email, age FROM user_data LIMIT %s OFFSET %s"
            cursor.execute(query, (page_size, offset))
            rows = cursor.fetchall()
            return rows
    except Error as e:
        print(f"Error fetching page at offset {offset}: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def lazy_paginate(page_size):
    """Generator function to lazily load pages from the user_data table."""
    offset = 0
    while True:
        # Fetch the next page
        page = paginate_users(page_size, offset)
        if not page:  # Break if no more rows to fetch
            break
        yield page  # Yield the page (list of rows)
        offset += page_size  # Increment offset for the next page

if __name__ == "__main__":
    # Example usage: Paginate users with a page size of 2
    page_size = 2
    print(f"Paginating users with page size {page_size}:")
    for page in lazy_paginate(page_size):
        print(f"Page: {page}")