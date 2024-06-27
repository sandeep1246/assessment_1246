import pandas as pd
import pyodbc


def read_data():
    try:
        conn_str = (
            "DRIVER={PostgreSQL Unicode};"
            "DATABASE=postgres;"
            "UID=postgres;"
            "PWD=aman@123;"
            "SERVER=localhost;"
            "PORT=5432;"
        )

        # Establish a connection to the PostgreSQL database
        conn = pyodbc.connect(conn_str)

        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        file_name = "C:\\Users\\aman7\\Documents\\assesment1\\data.csv"
        # Read CSV file into DataFrame
        df = pd.read_csv(file_name)
        # Convert 'timestamp' column to datetime objects
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Fill NaN values with empty string ('')
        df_filled = df.fillna('')
        # Drop rows with NaN values
        df_clean = df_filled.dropna()

        print(df_clean.dtypes)

        truncate_query = 'TRUNCATE TABLE interactions;'
        cursor.execute(truncate_query)

        # Create table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS interactions (
            interaction_id INTEGER,
            user_id INTEGER,
            product_id INTEGER,
            action TEXT,
            timestamp TEXT
        )
        '''

        cursor.execute(create_table_query)
        # Insert data into the table
        for row in df_clean.itertuples(index=False):
            cursor.execute('''
            INSERT INTO interactions (interaction_id, user_id, product_id, action, timestamp)
            VALUES (?, ?, ?, ?, ?)
            ''', (row.interaction_id, row.user_id, row.product_id, row.action, row.timestamp))

        print(df_clean)
        # calling transform function
        transform_data(df_clean,cursor,conn)

        conn.commit()
        cursor.close()
        conn.close()

    except  Exception as e:
        print(e)


def transform_data(df_clean,cursor,conn):

    # Step 2: Group the data by `user_id` and `product_id` and calculate the interaction count
    interaction_counts = df_clean.groupby(['user_id', 'product_id']).size().reset_index(name='interaction_count')

    # Step 3: Merge the interaction count back to the original DataFrame
    df_transform = pd.merge(df_clean, interaction_counts, on=['user_id', 'product_id'])

    # Step 4: Display the user_id, product_id, and interaction_count columns
    result_df = df_transform[['user_id', 'product_id', 'interaction_count']].drop_duplicates()

    # Define the ALTER TABLE query to delete the column
    alter_table_query = '''
            ALTER TABLE interactions
            DROP COLUMN interaction_count;
            '''

    # Execute the ALTER TABLE query
    cursor.execute(alter_table_query)

    # ALTER TABLE command to add interaction_count column
    alter_table_query = '''
            ALTER TABLE interactions
            ADD COLUMN interaction_count INTEGER;
            '''
    # Execute the ALTER TABLE command
    cursor.execute(alter_table_query)

    # Populate interaction_count column using UPDATE query
    update_query = '''
           UPDATE interactions AS i
           SET interaction_count = sub.interaction_count
           FROM (
               SELECT user_id, product_id, COUNT(*) AS interaction_count
               FROM interactions
               GROUP BY user_id, product_id
           ) AS sub
           WHERE i.user_id = sub.user_id AND i.product_id = sub.product_id
           
           '''
    # Execute the update command
    cursor.execute(update_query)
    conn.commit()
    cursor.close()
    conn.close()
    print(result_df)

if __name__ == "__main__":
    read_data()







