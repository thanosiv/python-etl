import pandas as pd
import pyodbc
import glob
import os
import config
import time

def update():
    while True:
        for fname in glob.glob(config.path + '*.csv'):
            data = pd.read_csv(fname)
            df = pd.DataFrame(data)

            # Date related functions
            date_cols = df[[col for col in df.columns if "date" in col]]
            
            for x in date_cols:
                df[x].fillna(0,inplace=True)
            
            # Fill all blank fields with an empty length string
            df.fillna('', inplace=True)
            
            # Connect to SQL Server
            conn = pyodbc.connect(config.connstr)
            cursor = conn.cursor()
            
            # Pull out the Account Name from the file
            # query the database to get the AccountID for the account (if present)
            string = os.path.basename(fname)
            string = string.split('_')[0]
            query = config.getAccountName + "\'" + string + "\'"
            acctID = cursor.execute(query).fetchval()
            
            # if there is no account record, create a new one
            # and set the acctID variable to the new created Account.AccountID
            if acctID == None:
                maxAcctid = cursor.execute(config.maxAccountID).fetchval()
                maxAcctid = maxAcctid+1
                acctID = maxAcctid
                cursor.execute(config.insertAccount,maxAcctid,string)
            
            # Get max ClientID from Client table to autoincrement ClientID
            # add 1 to maxid to get starting value
            maxid = cursor.execute(config.maxClientID).fetchval()
            maxid = maxid+1
            
            # for each record in the CSV file, execute an INSERT statement into the Client table
            for row in df.itertuples():
                
                # Needs to be changed to do this for any date field
                ctBirthdate = row.ClientBirthdate
                
                if ctBirthdate == 0:
                    ctBirthdate = None
                # End of the portion to be changed for any date field
                    
                cursor.execute(config.insertClient,
                            acctID, 
                            maxid,
                            row.ClientFirstName,
                            row.ClientLastName,
                            row.ClientMiddleInit,
                            row.ClientSex,
                            ctBirthdate,
                            row.AddressID
                            )
                # Add 1 to maxid var to get next value to insert for ClientID
                maxid = maxid+1
            conn.commit()
            os.remove(fname)
            
            time.sleep(config.timeval)

update()