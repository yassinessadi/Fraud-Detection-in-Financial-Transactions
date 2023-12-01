from pyhive import hive 
import requests as req

###################################################
#    create tables and insert into customers     #
##################################################
def customersTable(cursor):
    table_creation_query = """
        CREATE TABLE IF NOT EXISTS testdb.producers_gogo (
            account_history STRING,
            avg_transaction_value DOUBLE,
            customer_id STRING,
            age INT,
            location STRING
        )
        """
    cursor.execute(table_creation_query)

###################
# connect to hive #
##################
def connectHive():
    connection = hive.connect(host='localhost', database='testdb')
    cursor = connection.cursor()
    return connection,cursor

########################################
#          close connection            #
########################################

def closeConnection(cursor,connection):
    cursor.close()
    connection.close()

################
# save changes #
################
def savesChanges(connection):
    connection.commit()

def get_all_customers(cursor):
    """
    get all the customers
    """
    cursor.execute('SELECT * FROM testdb.customers')
    return cursor.fetchall()

def inseting_query(cursor,query):
    cursor.execute(query)

# insert into customers
def insert_into_customers(url_base):
    customers = req.get(url_base)
    if customers.status_code == 200:
        result = customers.json()
        counter = 0
        while True:
            data = customers.json()[counter]
            account_history_string = ",".join(data["account_history"])
            # Insert data into the table
            insert_query = '''
                INSERT INTO testdb.producers_gogo
                VALUES ('{a}',
                '{b}',
                '{c}',
                {d},
                '{e}')
                '''.format(
                    a = account_history_string,
                    b = data['behavioral_patterns']['avg_transaction_value'],
                    c = data['customer_id'],
                    d = data['demographics']['age'],
                    e = data['demographics']['location']
                )
            

            # connection
            connection,cursor = connectHive()

            # create customer tables
            customersTable(cursor)


            # insert into customers
            inseting_query(cursor=cursor,query=insert_query)


            # close connection
            closeConnection(cursor=cursor,connection=connection)
            # save changes
            savesChanges(connection)

            counter += 1
            if counter == len(result) :
                break
    else:
        print("Failed to fetch data from the API.")



#########################
# insert into customers #
#########################
insert_into_customers("http://127.0.0.1:5000/api/customers/")
