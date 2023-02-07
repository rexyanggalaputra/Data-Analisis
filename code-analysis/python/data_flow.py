import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


def data_checking(data) :
    print('Basic data information')
    print('-'*22)
    print('Jumlah data              :', len(data))
    print('Jumlah kolom             :', len(data.columns.tolist()))
    print('Jumlah duplicated data   :', data.duplicated().sum(), 'atau', round(data.duplicated().sum()/len(data)*100,1), '% dari keseluruhan data')
    
    print('\nKolom-kolom yang mengandung null values sebagai berikut :')
    print('-'*57)
    if len(data.columns[data.isnull().any()].tolist()) == 0 :
        print('Tidak ada kolom yang mengandung null values')
    else :
        for i in data.columns[data.isnull().any()].tolist() :
            print(i, ':', data[i].isnull().sum(), 'atau', round(data[i].isnull().sum()/len(data[i])*100,3),'%')

    print('\nUnique object columns')
    print('-'*21)

    list_kolom = []
    detect_nunique_object_type_column = 13
    for i in data.select_dtypes(include=object).columns.tolist() :
        if data[i].nunique() < detect_nunique_object_type_column :
            list_kolom.append(i)

    if len(list_kolom) == 0 :
        print('Semua kolom data mempunyai unique value lebih dari', detect_nunique_object_type_column)
    else :
        for i in list_kolom :
            print(i, ':', data[i].unique().tolist())

    print('\nColumns possibility boolean')
    print('-'*28)

    list_bool = []
    for i in data.select_dtypes(include=np.number).columns.tolist() :
        if data[i].nunique() == 2 :
            list_bool.append(i)
    
    if len(list_bool) == 0 :
        print('Tidak ada kolom yang terindikasi bertipe Boolean')
    else :
        for i in list_bool :
            print(i, ':', data[i].unique().tolist())

def data_cleansing(data) :
    
    data_pre = data.copy()

    try :
        def drop_dupicates(data) :
            if data.duplicated() == 0 :
                data = data.copy()
            else :
                data.drop_duplicates(inplace=True)
            return data

        nan_replacements = {"children:": 0, "agent": data['agent'].mode()}
        data = data.fillna(nan_replacements)
        data.drop(['company'], axis=1, inplace=True)

        # redundan data
        data['meal'] = data['meal'].replace('Undefined', 'No Meal')

        # new data with adults > 0, assumed in hotel bookings should be have 1 adults to order
        data = data[data['adults'] > 0]

        print('\nJumlah data sebelum processing :', len(data_pre))
        print('Jumlah data sesudah processing :', len(data), '\n')

        return data
    
    except Exception as e :
        return f'Error detected. What went wrong is : {e}'
        
    



    