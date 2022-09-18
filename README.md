I found a need to have a process monitor a specific directory for CSV files, and import the data into a specified table in a multi-tenant database. The format for the csv files should always include the Account Name, and the name of the table to import data into, so in the existing code it would look something like "AccountName_Client.csv".

config.py file ignored due to personal information present, but current variables present (with definitions) are :
connstr (The database connection string)
getAccountName (The T-SQL query used for accountName matching with the account in the filename of the csv)
insertAccount (The T-SQL query used for inserting a new record into the Account table, when the result for getAccountName = 0)
insertClient (The T-SQL query used for inserting the records into the Client table)
maxAccountID (The T-SQL for getting the maximum AccountID field in the Account table; used for determining the new AccountID to be inserted in coordination with the insertAccount var)
maxClientID (The T-SQL for getting the maximum ClientID field in the Client table; used for determining the new ClientID to be inserted in coordination with the insertClient var)
path (The directory path that the process will scan)
