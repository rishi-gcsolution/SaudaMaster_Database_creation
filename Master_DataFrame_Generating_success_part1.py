# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 11:37:11 2023

@author: asus
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 10:37:33 2023
@author: asus
"""

import pandas as pd
import psycopg2

# Make a database connection
conn = psycopg2.connect(
    host="localhost",
    database="HDFC_FrontOffice_DB",
    user="postgres",
    password="Rishi@29"
)
cur = conn.cursor()

your_table_name = 'saudamasterdbtable'

# Construct your SQL query
sql_query = f"SELECT * FROM {your_table_name}"

# Use pandas to execute the query and fetch the data as a DataFrame
df = pd.read_sql(sql_query, conn)

# Close the cursor and the connection
cur.close()
conn.close()

# Now, 'df' contains the data fetched from the database as a DataFrame

# Initialize a DataFrame with dictionary keys to receive input
keys = ['client_id', 'client_Name', 'Last_order_date', 'Total_No_of_orders', 'Total_Brokerage_Amount_sum']
my_dict = {key: None for key in keys}
master_df = pd.DataFrame(columns=my_dict.keys())

def get_client_data(client_id_value, column_name):
    df_retrieved = df[df[column_name] == client_id_value]
    df_retrieved = df_retrieved.copy()
    df_retrieved['client_name'] = df_retrieved['client_name'].str.rstrip()
    Total_No_of_orders = df_retrieved['order_no'].count()
    Last_order_date = df_retrieved['trade_date'].max()
    df_retrieved['brokerage'] = pd.to_numeric(df_retrieved['brokerage'], errors='coerce')
    Total_Brokerage_Amount_sum = df_retrieved['brokerage'].sum()

    final_output_in_dictform = {
        'client_id': client_id_value,
        'client_Name': df_retrieved['client_name'].values[0],
        'Last_order_date': Last_order_date,
        'Total_No_of_orders': Total_No_of_orders,
        'Total_Brokerage_Amount_sum': Total_Brokerage_Amount_sum
    }

    return final_output_in_dictform

# Using client_id from the rows to pass into function get_client_data and updating and appending the above initialized dataframe
column_name = 'client_code'

for index, row in df.iterrows():
    ref_list=[]
    client_id = row[4]  # Assuming the client_id as client_code (will clean it later) is in the 4th column of the rows
    data_to_append = get_client_data(client_id, column_name)
    
    if master_df.empty:
       master_df = master_df.append(data_to_append, ignore_index=True) 
       ref_list.append(data_to_append['client_id'])
        
    else:
        is_present = data_to_append['client_id'] in master_df['client_id'].values
        if not is_present:
            master_df = master_df.append(data_to_append, ignore_index=True)    
        #    pass
            
        
    '''    
        result = master_df.loc[master_df[client_id] == data_to_append['client_id']]

        
    master_df.loc[master_df[client_id]]
    

    # Check if any values in data_to_append are different from the corresponding values in the DataFrame
    should_append = not master_df.empty and any(
        master_df[key].iloc[-1] != value for key, value in data_to_append.items()
    )

    if should_append:
        # Append the data from the dictionary to the DataFrame
        master_df = master_df.append(data_to_append, ignore_index=True)

# Print or use the master_df DataFrame as needed

for index, row in df.iterrows():
    pass
'''