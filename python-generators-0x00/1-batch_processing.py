import mysql.connector
import mysql.connector.errorcode

def create_connection():
    """Creates connection to the database"""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="qwertyuiop",
        database="ALX_prodev"
    )

def stream_users_in_batches(batch_size):
    """Generator that yields users in batch"""
    connection = create_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM user_data")

    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield batch

    cursor.close()
    connection.close()



def batch_processing(batch_size):
    """Processing each batch to print users who are over age 25"""
    for batch in stream_users_in_batches(batch_size):
        filtered_batch = [user for user in batch if user[3] > 25]
        if filtered_batch:
            print(filtered_batch, "\n")