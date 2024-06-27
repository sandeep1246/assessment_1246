# Instructions to Run the Script


## Dependencies
- Python (version 3.x recommended)
- pandas library
- pyodbc library
- ODBC driver for PostgreSQL
- PostgreSQL database with appropriate permissions

## Setup
1. Clone or download the repository containing the script.

2. Install dependencies using pip:
   ```bash
   pip install pandas pyodbc
   
3. **Download and Install PostgreSQL ODBC Driver:**
   - Download the appropriate PostgreSQL ODBC driver for your operating system from [PostgreSQL Downloads](https://www.postgresql.org/ftp/odbc/).
   - Install the ODBC driver following the installation instructions provided.
   
4. **Configure ODBC Data Source:**
   - Open ODBC Data Source Administrator:
     - On Windows: Search for "ODBC Data Sources" or run `odbcad32.exe`.
     - On macOS/Linux: Edit the `odbc.ini` file located in `/etc/`.

   - Add a new System or User DSN (Data Source Name):
     - Choose the PostgreSQL ODBC driver.
     - Configure the connection details (Database, Server, Port, Username, Password).
   

##To run the script from the command line:

1. **Navigate to Script Directory:**
   Open a terminal or command prompt and navigate to the directory where `interaction.py` and `data.csv` are located.

2. **Run the Script:**
   Execute the script using Python:
   ```bash
   python interaction.py 

### Explanation

- **Prerequisites:** List the software and libraries required to run the script (`Python`, `pandas`, `pyodbc`).

- **Setup:** Instructions to clone the repository and install dependencies (`pandas`, `pyodbc`) using `pip`.

- **Configuration:** Details on configuring the PostgreSQL connection string (`conn_str`) in `script.py`.

- **Running the Script:** Steps to execute `script.py`, including navigating to the correct directory and running the script using `python`.

- **Notes:** Additional information and considerations for running the script, such as data format (`data.csv`) and SQL queries.

### Usage

- Create a file named `README.md` in the root directory of your project.
- Copy the provided Markdown content into `README.md`.
- Update placeholders (`your_database`, `your_username`, `your_password`, `your_server_address`, `data.csv` path) with your actual details.

-------------------------------------Airflow Task----------------------------


## 

1. ** curl 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml' -o 'docker-compose.yaml'
2. ** docker compose up # For running in background
3. **mkdir -p ./dags ./logs ./plugins


## Components
- **DAG Name**: read_and_transform_dag
- **Description**: A simple PythonOperator DAG
- **Schedule Interval**: Daily
- **Owner**: airflow
- **Start Date**: 2024-06-27

# Setup
1. Ensure Apache Airflow is installed and properly configured.
2. Place the DAG file in your Airflow DAGs directory.
3. Define the `read_data` function with the required data reading and transformation logic.
4. Update the file paths in the `read_data` function to match your environment.


-------------------------------Data Storage and Retrieval-------------------


 # Query 1: Total number of interactions per day

 SELECT DATE(timestamp) AS interaction_date, COUNT(*) AS total_interactions
    FROM public.interactions
    GROUP BY interaction_date
    ORDER BY interaction_date;

# Query 2: Top 5 users by the number of interactions

SELECT user_id, COUNT(*) AS total_interactions
    FROM public.interactions
    GROUP BY user_id
    ORDER BY total_interactions DESC
    LIMIT 5;

# Query 3: Most interacted products based on the number of interactions
SELECT product_id, COUNT(*) AS total_interactions
    FROM user_interactions
    GROUP BY product_id
    ORDER BY total_interactions DESC;

# Optimization 
We can create incremental column with index or parttiton the table by Products.

