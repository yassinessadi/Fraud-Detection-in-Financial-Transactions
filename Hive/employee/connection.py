from pyhive import hive 
import requests as req

# Get data from the API
transactions = req.get("http://127.0.0.1:5000/api/customers/")

if transactions.status_code == 200:
    data = transactions.json()[0]

    # Establish a connection
    connection = hive.connect(host='localhost', port=10000, database='testdb')
    cursor = connection.cursor()

    # Create the table
    table_creation_query = """
    CREATE TABLE IF NOT EXISTS testdb.users (
        account_history ARRAY<STRING>,
        behavioral_patterns STRUCT<avg_transaction_value: DOUBLE>,
        customer_id STRING,
        demographics STRUCT<age: INT, location: STRING>
    )
    """
    cursor.execute(table_creation_query)

    # Insert data into the table
    insert_query = """
    INSERT INTO testdb.users
    VALUES (?, ?, ?, ?)
    """

    # cursor.execute(insert_query, (
    #     data['account_history'],
    #     {'avg_transaction_value': data['behavioral_patterns']['avg_transaction_value']},
    #     data['customer_id'],
    #     {'age': data['demographics']['age'], 'location': data['demographics']['location']}
    # ))

    # Commit the transaction
    connection.commit()

    # Fetch and print the inserted data
    cursor.execute('SELECT * FROM testdb.employee')
    print(cursor.fetchone())
    # print(cursor.fetchall())

    # Close the cursor and connection
    cursor.close()
    connection.close()
else:
    print("Failed to fetch data from the API.")
