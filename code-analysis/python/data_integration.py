# pip install pygsheets
# pip install psycopg2
import pygsheets
import os, sys
import psycopg2
import pandas as pd
import json

# Google Sheets
def write_to_gsheet(service_file_path, spreadsheet_id, sheet_name, data_df):
    
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        sh.add_worksheet(sheet_name)
    except:
        pass
    wks_write = sh.worksheet_by_title(sheet_name)
    wks_write.clear('A1',None,'*')
    wks_write.set_dataframe(data_df, (1,1), encoding='utf-8', fit=False, copy_index=False)
    wks_write.frozen_rows = 1
    wks_write.frozen_cols = 1

def append_to_gsheet(service_file_path, spreadsheet_id, sheet_name, data_df):
    """
    this function takes data_df and writes it under spreadsheet_id
    and sheet_name using your credentials under service_file_path
    """
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        sh.add_worksheet(sheet_name)
    except:
        pass
    wks_write = sh.worksheet_by_title(sheet_name)
    wks_write.set_dataframe(data_df, (len(data_df) + 12,1), encoding='utf-8', fit=True, copy_index=False)
    
def get_gsheet_data(service_file_path, spreadsheet_id, sheet_name, file_name):
    creds_file = service_file_path
    gc = pygsheets.authorize(service_file=creds_file)
    list_filename = file_name # nama file
    sheet_key = spreadsheet_id
    sheet_title = sheet_name
    for idx in range(len(list_filename)):
     
        sh = gc.open_by_key(sheet_key[idx])
        wks = sh.worksheet("title",sheet_title[idx])
        df = wks.get_as_df()
        
        # if os.path.exist("app/data"+list_filename[idx]+".csv"):
        # os.remove("app/data"+list_filename[idx]+".csv")
        # wks.export(pygsheets.ExportType.CSV,path="app/data", filename=list_filename[idx])
    return df

# Postgresql
def get_postgres_data(table, schema) :
    engine = psycopg2.connect(
                database="xxxxxx",
                user="xxxxxx",
                password="xxxxxxxx",
                host="xxxxxx",
                port="xxxxxx",
                options=f"-c search_path=dbo,{schema}"
            )
    engine.autocommit = True
    query=f"SELECT * from {table}"
    cursor = engine.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cols = []
    for elt in cursor.description:
        cols.append(elt[0])
    df = pd.DataFrame(data=data, columns=cols)

    return df

def df_to_postgres(source_df, table_on_db, schema, primary_key = None) :
    
    engine = psycopg2.connect(
            database="xxxxx",
            user="xxxxx",
            password="xxxxx",
            host="xxxxx",
            port="xxxxx",
            options=f"-c search_path=dbo,{schema}"
        )

    if primary_key == None :
        # flow : truncate --> insert new data
        cursor = engine.cursor()
        tup_data = [tuple(x) for x in source_df.to_numpy()]
        values = '%s,'*len(source_df.columns)
        col = json.dumps(tuple(source_df.columns.tolist())).replace('[','(').replace(']',')')
        
        truncate_query = f'''
                    TRUNCATE TABLE {table_on_db} RESTART IDENTITY'''

        insert_query = f'''
                    INSERT INTO {table_on_db} {col}
                    VALUES ({values.rstrip(',')})
                '''

        cursor.execute(truncate_query)

        for i in tup_data :
            cursor.execute(insert_query, i)
        engine.commit()
        cursor.close()
        engine.close()

    else :
        # flow : update existing data

        primary_key = json.dumps(tuple(primary_key)).replace('[','(').replace(']',')')
        cursor = engine.cursor()
        tup_data = [tuple(x) for x in source_df.to_numpy()]
        values = '%s,'*len(source_df.columns)
        col = json.dumps(tuple(source_df.columns.tolist())).replace('[','(').replace(']',')')
        nth_col =  ["EXCLUDED."+i.strip() for i in json.dumps(tuple(source_df.columns.tolist())).replace('[','').replace(']','').split(',')]

        insert_query = f'''
        
                        INSERT INTO {table_on_db} {col}
                        VALUES ({values.rstrip(',')})
                        ON CONFLICT ({primary_key}) DO UPDATE SET
                        {col} = ({','.join(str(i) for i in nth_col)})

                        '''
        # cursor.execute(truncate_query)

        for i in tup_data :
            cursor.execute(insert_query, i)
        engine.commit()
        cursor.close()
        engine.close()       

