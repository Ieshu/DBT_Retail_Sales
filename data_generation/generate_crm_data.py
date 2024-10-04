import psycopg2
import numpy as np
from faker import Faker

# PostgreSQL connection details
POSTGRES_HOST = 'localhost'
POSTGRES_DB = 'stock_warehouse'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'ies123@IES123'
POSTGRES_PORT = '5432'

# Connect to PostgreSQL database
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        print("Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Ensure that the required tables exist
def create_tables_if_not_exists():
    conn = connect_to_postgres()
    if conn is None:
        print("Failed to connect to PostgreSQL. Exiting.")
        return

    cursor = conn.cursor()
    try:
        # Create Customers Table in 'retail' schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS retail.customers (
                customer_id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100) UNIQUE,
                phone VARCHAR(50),  -- Increased to 50 characters
                address TEXT
            );
        """)
        print("Table 'retail.customers' created or verified.")

        # Create Accounts Table in 'retail' schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS retail.accounts (
                account_id SERIAL PRIMARY KEY,
                customer_id INT REFERENCES retail.customers(customer_id),
                account_type VARCHAR(50),
                balance NUMERIC(25, 2)
            );
        """)
        print("Table 'retail.accounts' created or verified.")

        # Create Transactions Table in 'retail' schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS retail.transactions (
                transaction_id SERIAL PRIMARY KEY,
                account_id INT REFERENCES retail.accounts(account_id),
                transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                amount NUMERIC(25, 2),
                transaction_type VARCHAR(50)
            );
        """)
        print("Table 'retail.transactions' created or verified.")

        conn.commit()
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# Fetch existing emails from the database
def fetch_existing_emails():
    conn = connect_to_postgres()
    if conn is None:
        print("Failed to connect to PostgreSQL. Exiting.")
        return set()

    cursor = conn.cursor()
    try:
        cursor.execute("SELECT email FROM retail.customers;")
        existing_emails = set(email[0] for email in cursor.fetchall())
        return existing_emails
    except Exception as e:
        print(f"Error fetching existing emails: {e}")
        return set()
    finally:
        cursor.close()
        conn.close()

# Generate random data using numpy and faker
def generate_random_data(num_records=1000):
    fake = Faker()
    
    customers = []
    accounts = []
    transactions = []
    generated_emails = fetch_existing_emails()  # Fetch existing emails from the database

    # Generate customer data
    for _ in range(num_records):
        customer_id = np.random.randint(1000, 9999)
        name = fake.name()
        email = fake.email()[:100]

        # Ensure email is unique both in generated and existing emails
        while email in generated_emails:
            email = fake.email()[:100]  # Regenerate email until unique
        generated_emails.add(email)

        phone = fake.phone_number()[:50]
        address = fake.address()
        customers.append((customer_id, name, email, phone, address))

        # Generate account data
        account_id = np.random.randint(10000, 99999)
        account_type = np.random.choice(['Checking', 'Savings', 'Credit'])
        balance = np.random.uniform(100.0, 10000.0)
        accounts.append((account_id, customer_id, account_type, balance))

        # Generate transaction data (between 1-10 transactions per account)
        num_transactions = np.random.randint(1, 11)
        for _ in range(num_transactions):
            transaction_id = np.random.randint(100000, 999999)
            transaction_date = fake.date_time_this_year()
            amount = np.random.uniform(-500.0, 500.0)  # Positive for credit, negative for debit
            transaction_type = np.random.choice(['Debit', 'Credit'])
            transactions.append((transaction_id, account_id, transaction_date, amount, transaction_type))
    
    return customers, accounts, transactions

# Function to retrieve and display a maximum of 10 rows from a table
def display_table_rows(table_name):
    # Establish the connection to PostgreSQL
    conn = connect_to_postgres()
    if conn is None:
        print("Failed to connect to PostgreSQL. Exiting.")
        return

    cursor = conn.cursor()
    try:
        # Execute the SELECT query to fetch a maximum of 10 rows from the specified table
        cursor.execute(f"SELECT * FROM retail.{table_name} LIMIT 10;")
        rows = cursor.fetchall()
        
        # Print the number of rows and the actual data
        print(f"Displaying up to 10 rows from {table_name}:")
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Error retrieving data from {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

# Insert generated data into the CRM database
# Insert generated data into the CRM database and verify if data is loaded
def insert_data_to_postgres(customers, accounts, transactions):
    conn = connect_to_postgres()
    if conn is None:
        print("Failed to connect to PostgreSQL. Exiting.")
        return

    cursor = conn.cursor()
    try:
        # Insert customer data into 'retail.customers'
        cursor.executemany("""
            INSERT INTO retail.customers (customer_id, name, email, phone, address)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """, customers)

        # Insert account data into 'retail.accounts'
        cursor.executemany("""
            INSERT INTO retail.accounts (account_id, customer_id, account_type, balance)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (account_id) DO NOTHING;
        """, accounts)

        # Insert transaction data into 'retail.transactions'
        cursor.executemany("""
            INSERT INTO retail.transactions (transaction_id, account_id, transaction_date, amount, transaction_type)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """, transactions)

        conn.commit()
        print(f"Inserted {len(customers)} customers, {len(accounts)} accounts, and {len(transactions)} transactions.")

        # Verify data insertion by counting rows in each table
        cursor.execute("SELECT COUNT(*) FROM retail.customers;")
        customer_count = cursor.fetchone()[0]
        print(f"Total customers loaded: {customer_count}")

        cursor.execute("SELECT COUNT(*) FROM retail.accounts;")
        account_count = cursor.fetchone()[0]
        print(f"Total accounts loaded: {account_count}")

        cursor.execute("SELECT COUNT(*) FROM retail.transactions;")
        transaction_count = cursor.fetchone()[0]
        print(f"Total transactions loaded: {transaction_count}")

        # Display up to 10 rows from each table to verify the insertion
        display_table_rows('customers')
        display_table_rows('accounts')
        display_table_rows('transactions')

    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # Step 1: Create tables if they don't exist
    create_tables_if_not_exists()

    # Step 2: Generate random data
    customers, accounts, transactions = generate_random_data(num_records=1000)

    # Step 3: Insert data into PostgreSQL
    insert_data_to_postgres(customers, accounts, transactions)
