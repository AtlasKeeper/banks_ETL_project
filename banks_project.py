import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3
import numpy as np 
import datetime 



def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the code execution to a log file. '''
    with open('code_log.txt', 'a') as log_file:
        time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(f"{time_stamp} : {message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required information from the website and save it to a data frame. The function returns the data frame for further processing. '''
    log_progress("Initiating data extraction process.")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', table_attribs)
    
    data = []
    headers = [header.text.strip() for header in table.find_all('th')]
    
    for row in table.find_all('tr')[1:11]:  # Top 10 banks
        columns = row.find_all('td')
        if columns:
            row_data = [column.text.strip() for column in columns]
            data.append(row_data)
    
    df = pd.DataFrame(data, columns=headers)
    df = df[['Bank name', 'Market cap(US$ billion)']]  # Keep relevant columns
    df.columns = ['Name', 'MC_USD_Billion']  # Rename columns for consistency
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(r'\n', '').astype(float)
    
    log_progress("Data extraction complete. Initiating Transformation process.")
    return df

# Declare known values
log_progress("Preliminaries complete. Initiating ETL process")

# Define URLs and paths
data_url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

# Extract data
df_extracted = extract(data_url, {'class': 'wikitable'})
print(df_extracted)

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, each containing the transformed version of Market Cap column to respective currencies '''
    log_progress("Initiating data transformation process.")
    
    # Read the exchange rate CSV file
    exchange_rates = pd.read_csv(csv_path)
    
    # Print the column names to inspect
    print("Exchange rate CSV columns:", exchange_rates.columns)
    
    # Convert the exchange rates to a dictionary
    exchange_rate_dict = exchange_rates.set_index('Currency')['Rate'].to_dict()
    
    # Add new columns with transformed values
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    
    log_progress("Data transformation complete. Initiating Loading process.")
    return df

# Define the path to the exchange rate CSV file
exchange_rate_csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

# Transform the data
df_transformed = transform(df_extracted, exchange_rate_csv_path)
print(df_transformed)

# Print the value of df['MC_EUR_Billion'][4] for the quiz
print(df_transformed['MC_EUR_Billion'][4])

def load_to_csv(df, csv_path):
    ''' This function saves the transformed dataframe to a CSV file at the specified path. '''
    log_progress("Initiating CSV loading process.")
    
    df.to_csv(csv_path, index=False)
    
    log_progress(f"Data saved to CSV file: {csv_path}")

# Define the output CSV path
output_csv_path = "./Largest_banks_data.csv"

# Call the function to save the dataframe to CSV
load_to_csv(df_transformed, output_csv_path)    

def load_to_db(df, db_name, table_name):
    ''' This function loads the transformed dataframe into an SQLite database table. '''
    log_progress("Initiating database loading process.")
    
    # Establish connection to SQLite database
    conn = sqlite3.connect(db_name)
    
    # Write DataFrame to SQLite Table
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    log_progress(f"Data loaded to SQLite database. Database file: {db_name}, Table name: {table_name}")

# Define database name and table name
db_name = "Banks.db"
table_name = "Largest_banks"

# Call the function to load dataframe into SQLite database
load_to_db(df_transformed, db_name, table_name)

def run_queries(query, conn):
    ''' This function executes the given SQL query and prints the output. '''
    log_progress(f"Executing SQL query: {query}")
    
    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)
    
    # Fetch and print the query results
    results = cursor.fetchall()
    for row in results:
        print(row)
    
    # Close the cursor
    cursor.close()

# Establish connection to SQLite database
conn = sqlite3.connect("Banks.db")

try:
    # Query 1: Print the contents of the entire table
    query1 = "SELECT * FROM Largest_banks"
    run_queries(query1, conn)
    
    # Query 2: Print the average market capitalization of all the banks in Billion USD
    query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_queries(query2, conn)
    
    # Query 3: Print only the names of the top 5 banks
    query3 = "SELECT Name FROM Largest_banks LIMIT 5"
    run_queries(query3, conn)
    
    log_progress("All queries executed successfully.")

finally:
    # Close connection to SQLite database
    conn.close()