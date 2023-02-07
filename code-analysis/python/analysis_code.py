import pandas as pd
from math import ceil
import warnings
import pygsheets
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """  
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + first_day.weekday()
        
    return int(ceil(adjusted_dom/7.0))

def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=2, diff = None):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2[key2].tolist()
    
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
    df_1['matches_{}'.format(diff)] = m
    
    m2 = df_1['matches_{}'.format(diff)].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches_{}'.format(diff)] = m2
    
    return df_1

    
def weekly_feed_brand_analysis(df, goal_days = 28, to = 'bw') :
    df['chickin_date'] = df['real_chickin_date']
    df['kandang'] = df['Kandang']
    df[f'{to}_{goal_days}'] = pd.to_numeric(df[f'{to}_{goal_days}'], errors='coerce')
    df[f'{to}_{goal_days}'] = df[f'{to}_{goal_days}'].astype(float)
    df['chickin_date'] = pd.to_datetime(df['chickin_date'])

    df['feed_terbanyak'] = df['feed_terbanyak'].replace('AAAA', None).replace('BBBB', None)
    df['feed_terbanyak'] = df['feed_terbanyak'].replace('CCCC', 'DDDD').replace('EEEE', 'FFFF').replace('GGGG', 'HHHH')

    df.dropna(subset=['feed_terbanyak', f'{to}_{goal_days}', 'chickin_date'], inplace=True)

    col1 = []
    for i in df['feed_terbanyak'].unique() :
      for j in range(1,29) :
        col1.append('{}_{}'.format(i, j))

    data = pd.concat([df[col1], df[['chickin_date', f'{to}_{goal_days}', 'feed_terbanyak']]], axis=1)


    for i in df['feed_terbanyak'].unique() :
      data[i] = np.sum(data.filter(like=i), axis=1)

    jenis = ['pppp', 'qqqq', 'rrrr']

    col2 = []
    for i in jenis :
      for j in df['feed_terbanyak'].unique() :
        for k in range(1,29) :
          col2.append('{}_{}_{}'.format(i, j, k))

    df = pd.concat([data, df[col2]], axis=1)
    df.drop(col1, axis=1, inplace=True)

    for i in df['feed_terbanyak'].unique() :
      df[i] = np.where(df[i] >= 2, 1,
                        np.where(df[i] == 1, 1, 0))
      
    for i in jenis :
      for j in df['feed_terbanyak'].unique() :
        df['{}_{}'.format(i,j)] = np.sum(df.filter(like='{}_{}'.format(i,j)), axis=1)
        df['{}_{}'.format(i,j)] = np.where(df['{}_{}'.format(i,j)] >= 2, 1,
                                          np.where(df['{}_{}'.format(i,j)] == 1, 1, 0))
        
    df.drop(col2, axis=1, inplace=True)
    df[f'body_weight_day_{goal_days}'] = df[f'{to}_{goal_days}']
    df[f'body_weight_day_{goal_days}'] = df[f'body_weight_day_{goal_days}'].astype(float)
    df.drop([f'{to}_{goal_days}'], axis=1, inplace=True)



    df['week'] = df['chickin_date'].dt.isocalendar().week.astype(int)
    df['month'] = df['chickin_date'].apply(lambda x: x.strftime('%b') )
    df['year'] = df['chickin_date'].dt.isocalendar().year.astype(int)
    
    
    df['week month'] = df['chickin_date'].apply(week_of_month)

    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    years = ['21', '22']
    weeks = ['1', '2', '3', '4', '5']

    dict_all = {}
    goal_days = goal_days
    to = to
    for i in years :
         for j in months :
             for k in weeks :
                  # Filtering Date
                  globals()[f'df_1_mo_{j}_{i}_week_{k}'] = df[(df['year'] == int(f'20{i}')) & (df['month'] == f'{j}'.capitalize()) & (df['week month'] == int(f'{k}'))]

                  # Generate Variable
                  dict_all[f'{to}_data_1_mo_{j}_{i}_week_{k}'] = round(globals()[f'df_1_mo_{j}_{i}_week_{k}'].drop(['week', 'year', 'week month'],axis=1).corr().tail(1).transpose().iloc[:-1 , :],3).rename(columns = {f'body_weight_day_{goal_days}' : f'{j} {i} : Week {k}'.capitalize()})


    df_names = []

    for i in dict_all.keys():
        temp_df = dict_all[i]
        df_names.append(temp_df)

    mortality_data_1_mo = pd.concat(df_names, axis=1)
    mortality_data_1_mo = mortality_data_1_mo.iloc[:,50:].fillna('').reset_index().rename(columns={'index' : 'Brand x Hatchery'})
    mortality_data_1_mo['Brand x Hatchery'] = mortality_data_1_mo['Brand x Hatchery'].apply(replacement)
    mortality_data_1_mo.columns = mortality_data_1_mo.columns.str.split(' : ', expand = True)   
    
    
    return mortality_data_1_mo
# test 123
def weekly_doc_brand_analysis(df, goal_days = 28, to = 'bw', combination = 'hatchery') :
    df['chickin_date'] = df['real_chickin_date']
    df['kandang'] = df['Kandang']
    df['chickin_date'] = pd.to_datetime(df['chickin_date'])
    
    if combination == 'hatchery' : 

        df = df[['kandang', 'chickin_date', 'DOC', 'Hatchery', f'{to}_{goal_days}']]
        df[f'{to}_{goal_days}'] = pd.to_numeric(df[f'{to}_{goal_days}'], errors='coerce')
        df[f'{to}_{goal_days}'] = df[f'{to}_{goal_days}'].astype(float)
        
        if to == 'mortality' :
            df[f'mortality_{goal_days}'] = round(df[f'mortality_{goal_days}']*100,4)
            df = df[df[f'mortality_{goal_days}'] <= 100]
            
        else :
            df = df

        df['DOC'] = df['DOC'].replace('AAAA', 'BBBB').replace('cccc','CCCC').replace('DDDD', 'BBBB').replace('EEEE', 'CCCC').replace('FFFF', None).replace('gggg', 'GGGG').replace('hhhh', 'HHHH')
        df['Hatchery'] = df['Hatchery'].replace('serang', 'Serang').replace('Sukabumi-Tangerang', 'Sukabumi').replace('Bogor & Sukabumi', 'Bogor').replace('Tangeran', 'Tangerang')
        df = df.dropna(subset = ['DOC', 'Hatchery', 'chickin_date'])
        
        df[''] = df['DOC'] + '__' + df['Hatchery']
        
        one_hot_var = ['DOC', '']
        
        for i in one_hot_var :
            onehots = pd.get_dummies(df[i], prefix=i)
            df = df.join(onehots)
        
        df = df.iloc[:, ::-1]

    
        df['week'] = df['chickin_date'].dt.isocalendar().week.astype(int)
        df['month'] = df['chickin_date'].apply(lambda x: x.strftime('%b') )
        df['year'] = df['chickin_date'].dt.isocalendar().year.astype(int)
    
    
        df['week month'] = df['chickin_date'].apply(week_of_month)

        months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        years = ['21', '22']
        weeks = ['1', '2', '3', '4', '5']

        dict_all = {}
        goal_days = goal_days
        for i in years :
            for j in months :
                for k in weeks :
                    # Filtering Date
                    globals()[f'df_1_mo_{j}_{i}_week_{k}'] = df[(df['year'] == int(f'20{i}')) & (df['month'] == f'{j}'.capitalize()) & (df['week month'] == int(f'{k}'))]

                    # Generate Variable
                    dict_all[f'mortality_data_1_mo_{j}_{i}_week_{k}'] = round(globals()[f'df_1_mo_{j}_{i}_week_{k}'].drop(['week', 'year', 'week month'],axis=1).corr().tail(1).transpose().iloc[:-1 , :],3).rename(columns = {f'{to}_{goal_days}' : f'{j} {i} : Week {k}'.capitalize()})


        df_names = []

        for i in dict_all.keys():
            temp_df = dict_all[i]
            df_names.append(temp_df)

        mortality_data_1_mo = pd.concat(df_names, axis=1)
        mortality_data_1_mo = mortality_data_1_mo.iloc[:,50:].fillna('').reset_index().rename(columns={'index' : 'Brand x Hatchery'})
        mortality_data_1_mo['Brand x Hatchery'] = mortality_data_1_mo['Brand x Hatchery'].apply(replacement)
        mortality_data_1_mo.columns = mortality_data_1_mo.columns.str.split(' : ', expand = True)

    elif combination == 'price' :

        df = df[['kandang', 'chickin_date', 'harga_per_doc' ,'DOC', 'bw_28']]
        df['DOC'] = df['DOC'].replace('AAAA', 'BBBB').replace('cccc','CCCC').replace('DDDD', 'DDDDD').replace('BBBBB', 'BBBB').replace('bb', None).replace('ffff', 'FFFF').replace('gggg', 'GGGG')

        df = df.dropna(subset = ['DOC', 'chickin_date'])
        df['harga_per_doc'] = df['harga_per_doc'].fillna(df['harga_per_doc'].median())


        conditions = [
                    (df['harga_per_doc'] < 2000),
                    ((df['harga_per_doc'] >= 2000) & (df['harga_per_doc'] < 4000)),
                    ((df['harga_per_doc'] >= 4000) & (df['harga_per_doc'] < 6000)),
                    ((df['harga_per_doc'] >= 6000) & (df['harga_per_doc'] < 8000)),
                    ((df['harga_per_doc'] >= 8000) & (df['harga_per_doc'] < 10000)),
                    ((df['harga_per_doc'] >= 10000) & (df['harga_per_doc'] <= 12000)),
                    (df['harga_per_doc'] > 2000)
                    ]

        values = [' < 2000', '2000 - 4000', '4000 - 6000', '6000 - 8000', '8000 - 10000', '10000 - 12000', '> 12000']
        df['harga_per_doc_cat'] = np.select(conditions, values)

        df['']  = df['DOC'] + '_' + df['harga_per_doc_cat']

        one_hot_var = ['']

        for i in one_hot_var :
            onehots = pd.get_dummies(df[i], prefix=i)
            df = df.join(onehots)

        df[f'bw_28.'] = df['bw_28']
        df['bw_28.'] = df['bw_28.'].astype(float)
        df.drop(['bw_28', 'harga_per_doc'], axis=1, inplace=True)
                
        df['week'] = df['chickin_date'].dt.isocalendar().week.astype(int)
        df['month'] = df['chickin_date'].apply(lambda x: x.strftime('%b') )
        df['year'] = df['chickin_date'].dt.isocalendar().year.astype(int)
            
            
        df['week month'] = df['chickin_date'].apply(week_of_month)

        months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        years = ['21', '22']
        weeks = ['1', '2', '3', '4', '5']

        dict_all = {}

        for i in years :
            for j in months :
                for k in weeks :
                            # Filtering Date
                            globals()[f'df_1_mo_{j}_{i}_week_{k}'] = df[(df['year'] == int(f'20{i}')) & (df['month'] == f'{j}'.capitalize()) & (df['week month'] == int(f'{k}'))]

                            # Generate Variable
                            dict_all[f'mortality_data_1_mo_{j}_{i}_week_{k}'] = round(globals()[f'df_1_mo_{j}_{i}_week_{k}'].drop(['week', 'year', 'week month'],axis=1).corr().tail(1).transpose().iloc[:-1 , :],3).rename(columns = {'bw_28.' : f'{j} {i} : Week {k}'.capitalize()})
                                                                                

        df_names = []

        for i in dict_all.keys():
                    temp_df = dict_all[i]
                    df_names.append(temp_df)

        mortality_data_1_mo = pd.concat(df_names, axis=1)
        mortality_data_1_mo = mortality_data_1_mo.iloc[:,50:].fillna('').reset_index().rename(columns={'index' : 'Brand x Hatchery'})
        mortality_data_1_mo['Brand x Hatchery'] = mortality_data_1_mo['Brand x Hatchery'].apply(replacement)
        mortality_data_1_mo.columns = mortality_data_1_mo.columns.str.split(' : ', expand = True)
        mortality_data_1_mo = mortality_data_1_mo.iloc[::-1, ::]

    return mortality_data_1_mo.iloc[::-1, ::]


def recent_brand_analysis(df, kind = 'doc', kind_only = False, combination = 'hatchery', inplace = True) :
    if kind == 'doc' :
        df['DOC'] = df['DOC'].replace('AAAAA', 'AAAA').replace('bbbb','BBBB').replace('ccccc', 'AAAA').replace('CCCCC', 'DDDD').replace('aa', None).replace('ffff', 'FFFF').replace('gggg', 'GGGG')
        df['Hatchery'] = df['Hatchery'].replace('serang', 'Serang').replace('Sukabumi-Tangerang', 'Sukabumi').replace('Bogor & Sukabumi', 'Bogor').replace('Tangeran', 'Tangerang')
        if kind_only == False : 
            if inplace == False :

                df['chickin_date'] = pd.to_datetime(df['real_chickin_date'])
                df['kandang'] = df['Kandang']
                # df['chickin_date'] = pd.to_datetime(df['chickin_date'])
                df = df[['chickin_date', 'kandang', 'DOC', f'{combination}'.capitalize(), 'mortality_8', 'bw_0' ,'bw_8']]
                for i in ['bw_0', 'bw_8']:
                    df[i] = pd.to_numeric(df[i], errors='coerce')
                    df[i] = df[i].astype(float)
                df['mortality_8'] = round(df['mortality_8']*100,4)
                df = df[df['mortality_8'] <= 100]

                df['laju_pertumbuhan'] = round(df['bw_8']/df['bw_0'],1)

                df = df.dropna(subset = ['DOC', f'{combination}'.capitalize()])

                df.drop(['DOC'], axis=1, inplace=True)

                df = df.sort_values('chickin_date', ascending=False).groupby(f'{combination}'.capitalize()).head(7).reset_index(drop=True).sort_values(f'{combination}'.capitalize())

                for i in df[f'{combination}'.capitalize()].unique().tolist() :
                    locals()[f'df_doc_hat_{i}'] = df[df[f'{combination}'.capitalize()] == i].sort_values('chickin_date', ascending=False)

                for i in df[f'{combination}'.capitalize()].unique().tolist() :
                    for index, row in locals()[f'df_doc_hat_{i}'].iterrows():
                        locals()[f'df_{index}'] = locals()[f'df_doc_hat_{i}'][locals()[f'df_doc_hat_{i}'].index == index]

                for i in df[f'{combination}'.capitalize()].unique().tolist() :
                        locals()[f'df_doc_hat_{i}'] = locals()[f'df_doc_hat_{i}'].set_index(f'{combination}'.capitalize())
                        locals()[f'df_doc_hat_{i}'] = pd.concat([  locals()[f'df_doc_hat_{i}'].iloc[0:1],
                                                            locals()[f'df_doc_hat_{i}'].iloc[1:2],
                                                            locals()[f'df_doc_hat_{i}'].iloc[2:3],
                                                            locals()[f'df_doc_hat_{i}'].iloc[3:4],
                                                            locals()[f'df_doc_hat_{i}'].iloc[4:5],
                                                            locals()[f'df_doc_hat_{i}'].iloc[5:6],
                                                            locals()[f'df_doc_hat_{i}'].iloc[6:7]], axis=1)
                        

                dflist = []
                dflist.extend(value for name, value in locals().items() if name.startswith('df_doc_hat_'))

                r = round(pd.concat(dflist, axis=0),2).fillna(' ')
            
            else : 

                df['chickin_date'] = pd.to_datetime(df['real_chickin_date'])
                df['kandang'] = df['Kandang']
                # df['chickin_date'] = pd.to_datetime(df['chickin_date'])
                df = df[['chickin_date', 'kandang', 'DOC', f'{combination}'.capitalize(), 'mortality_8', 'bw_0' ,'bw_8']]
                for i in ['bw_0', 'bw_8']:
                    df[i] = pd.to_numeric(df[i], errors='coerce')
                    df[i] = df[i].astype(float)
                df['mortality_8'] = round(df['mortality_8']*100,4)
                df = df[df['mortality_8'] <= 100]

                df['laju_pertumbuhan'] = round(df['bw_8']/df['bw_0'],1)

                df = df.dropna(subset = ['DOC', f'{combination}'.capitalize()])

                df[f'DOC x {combination}'.capitalize()] = df['DOC'] + '_' + df[f'{combination}'.capitalize()]
                df.drop(['DOC', f'{combination}'.capitalize()], axis=1, inplace=True)

                df = df.sort_values('chickin_date', ascending=False).groupby(f'DOC x {combination}'.capitalize()).head(7).reset_index(drop=True).sort_values(f'DOC x {combination}'.capitalize())

                for i in df[f'DOC x {combination}'.capitalize()].unique().tolist() :
                    locals()[f'df_doc_hat_{i}'] = df[df[f'DOC x {combination}'.capitalize()] == i].sort_values('chickin_date', ascending=False)

                for i in df[f'DOC x {combination}'.capitalize()].unique().tolist() :
                    for index, row in locals()[f'df_doc_hat_{i}'].iterrows():
                        locals()[f'df_{index}'] = locals()[f'df_doc_hat_{i}'][locals()[f'df_doc_hat_{i}'].index == index]

                for i in df[f'DOC x {combination}'.capitalize()].unique().tolist() :
                        locals()[f'df_doc_hat_{i}'] = locals()[f'df_doc_hat_{i}'].set_index(f'DOC x {combination}'.capitalize())
                        locals()[f'df_doc_hat_{i}'] = pd.concat([  locals()[f'df_doc_hat_{i}'].iloc[0:1],
                                                            locals()[f'df_doc_hat_{i}'].iloc[1:2],
                                                            locals()[f'df_doc_hat_{i}'].iloc[2:3],
                                                            locals()[f'df_doc_hat_{i}'].iloc[3:4],
                                                            locals()[f'df_doc_hat_{i}'].iloc[4:5],
                                                            locals()[f'df_doc_hat_{i}'].iloc[5:6],
                                                            locals()[f'df_doc_hat_{i}'].iloc[6:7]], axis=1)
                        

                dflist = []
                dflist.extend(value for name, value in locals().items() if name.startswith('df_doc_hat_'))

                r = round(pd.concat(dflist, axis=0),2).fillna(' ')

        else : 
            df['chickin_date'] = pd.to_datetime(df['real_chickin_date'])
            df['kandang'] = df['Kandang']
            # df['chickin_date'] = pd.to_datetime(df['chickin_date'])
            df = df[['chickin_date', 'kandang', 'DOC', 'Hatchery', 'mortality_8', 'bw_0' ,'bw_8']]
            for i in ['bw_0', 'bw_8']:
                df[i] = pd.to_numeric(df[i], errors='coerce')
                df[i] = df[i].astype(float)
            df['mortality_8'] = round(df['mortality_8']*100,4)
            df = df[df['mortality_8'] <= 100]

            df['laju_pertumbuhan'] = round(df['bw_8']/df['bw_0'],1)

            df['DOC'] = df['DOC'].replace('AAAAA', 'AAAA').replace('bbbb','BBBB').replace('cCcC', 'CCCC').replace('bBbB', 'BBBB').replace('bB', None).replace('ffff', 'FFFF').replace('gggg', 'GGGG')
            df['Hatchery'] = df['Hatchery'].replace('serang', 'Serang').replace('Sukabumi-Tangerang', 'Sukabumi').replace('Bogor & Sukabumi', 'Bogor').replace('Tangeran', 'Tangerang')
            df = df.dropna(subset = ['DOC', 'Hatchery'])

            df = df.sort_values('chickin_date', ascending=False).groupby('DOC').head(7).reset_index(drop=True).sort_values('DOC')

            for i in df['DOC'].unique().tolist() :
                locals()[f'df_doc_hat_{i}'] = df[df['DOC'] == i].sort_values('chickin_date', ascending=False)

            for i in df['DOC'].unique().tolist() :
                for index, row in locals()[f'df_doc_hat_{i}'].iterrows():
                    locals()[f'df_{index}'] = locals()[f'df_doc_hat_{i}'][locals()[f'df_doc_hat_{i}'].index == index]

            for i in df['DOC'].unique().tolist() :
                    locals()[f'df_doc_hat_{i}'] = locals()[f'df_doc_hat_{i}'].set_index('DOC')
                    locals()[f'df_doc_hat_{i}'] = pd.concat([  locals()[f'df_doc_hat_{i}'].iloc[0:1],
                                                            locals()[f'df_doc_hat_{i}'].iloc[1:2],
                                                            locals()[f'df_doc_hat_{i}'].iloc[2:3],
                                                            locals()[f'df_doc_hat_{i}'].iloc[3:4],
                                                            locals()[f'df_doc_hat_{i}'].iloc[4:5],
                                                            locals()[f'df_doc_hat_{i}'].iloc[5:6],
                                                            locals()[f'df_doc_hat_{i}'].iloc[6:7]], axis=1)
                        

            dflist = []
            dflist.extend(value for name, value in locals().items() if name.startswith('df_doc_hat_'))

            r = round(pd.concat(dflist, axis=0),2).fillna(' ')   
    
    elif kind == 'feed' :
        if kind_only == False :
           if inplace == True :
 
               df['chickin_date'] = df['real_chickin_date']
               df['kandang'] = df['Kandang']
               df['chickin_date'] = pd.to_datetime(df['chickin_date'])
               df = df[['chickin_date', 'kandang', f'{combination}'.capitalize(), 'prestarter_terbanyak','mortality_8', 'bw_0' ,'bw_8']]
               for i in ['bw_0', 'bw_8']:
                   df[i] = pd.to_numeric(df[i], errors='coerce')
                   df[i] = df[i].astype(float)
               df['laju_pertumbuhan'] = round(df['bw_8']/df['bw_0'],1)
               for i in ['prestarter_terbanyak'] :
                   df[i] = df[i].replace('AAAA', None).replace('BBBB', None)
                   df[i] = df[i].replace('CCCCc', 'CCCC').replace('DDDdD', 'DDDD').replace('fFFF', 'FFFF')
                   df[i] = df[i].replace('gGgGG', 'GGGG').replace('HhHH', 'HHHH').replace('IiII', 'IIII').replace('JJjJ', 'JJJJ').replace('KKKkk', 'KKKK').replace('lLLLL', 'LLLL')
                   df[i] = df[i].replace('MMMm', 'MMMM').replace('NNNn', 'NNNN')
               df[f'feedX{combination}'] = df['prestarter_terbanyak'] + '_' + df[f'{combination}'.capitalize()]
               df.dropna(subset=[f'feedX{combination}'], inplace=True)
 
               df = df.sort_values('chickin_date', ascending=False).groupby(f'feedX{combination}').head(7).reset_index(drop=True).sort_values(f'feedX{combination}')
 
               for i in df[f'feedX{combination}'].unique().tolist() :
                   locals()[f'df_prestarter_{i}'] = df[df[f'feedX{combination}'] == i].sort_values('chickin_date', ascending=False)
 
               for i in df[f'feedX{combination}'].unique().tolist() :
                   for index, row in locals()[f'df_prestarter_{i}'].iterrows():
                       locals()[f'df_{index}'] = locals()[f'df_prestarter_{i}'][locals()[f'df_prestarter_{i}'].index == index]
 
               for i in df[f'feedX{combination}'].unique().tolist() :
                       locals()[f'df_prestarter_{i}'] = locals()[f'df_prestarter_{i}'].set_index(f'feedX{combination}')
                       locals()[f'df_prestarter_{i}'] = pd.concat([ 
                                                           locals()[f'df_prestarter_{i}'].iloc[0:1],
                                                           locals()[f'df_prestarter_{i}'].iloc[1:2],
                                                           locals()[f'df_prestarter_{i}'].iloc[2:3],
                                                           locals()[f'df_prestarter_{i}'].iloc[3:4],
                                                           locals()[f'df_prestarter_{i}'].iloc[4:5],
                                                           locals()[f'df_prestarter_{i}'].iloc[5:6],
                                                           locals()[f'df_prestarter_{i}'].iloc[6:7]], axis=1)
                      
 
               dflist = []
               dflist.extend(value for name, value in locals().items() if name.startswith('df_prestarter_'))
 
               r = round(pd.concat(dflist, axis=0),2).fillna(' ')
          
           else:
 
               df['chickin_date'] = df['real_chickin_date']
               df['kandang'] = df['Kandang']
               df['chickin_date'] = pd.to_datetime(df['chickin_date'])
               df = df[['chickin_date', 'kandang', f'{combination}'.capitalize(), 'prestarter_terbanyak','mortality_8', 'bw_0' ,'bw_8']]
               for i in ['bw_0', 'bw_8']:
                   df[i] = pd.to_numeric(df[i], errors='coerce')
                   df[i] = df[i].astype(float)
               df['laju_pertumbuhan'] = round(df['bw_8']/df['bw_0'],1)
               for i in ['prestarter_terbanyak'] :
                   df[i] = df[i].replace('AAAA', None).replace('BBBB', None)
                   df[i] = df[i].replace('CCCCc', 'CCCC').replace('DDDdD', 'DDDD').replace('fFFF', 'FFFF')
                   df[i] = df[i].replace('gGgGG', 'GGGG').replace('HhHH', 'HHHH').replace('IiII', 'IIII').replace('JJjJ', 'JJJJ').replace('KKKkk', 'KKKK').replace('lLLLL', 'LLLL')
                   df[i] = df[i].replace('MMMm', 'MMMM').replace('NNNn', 'NNNN')
 
               df.dropna(subset=[f'{combination}'.capitalize()], inplace=True)
 
               df = df.sort_values('chickin_date', ascending=False).groupby(f'{combination}'.capitalize()).head(7).reset_index(drop=True).sort_values(f'{combination}'.capitalize())
 
               for i in df[f'{combination}'.capitalize()].unique().tolist() :
                   locals()[f'df_prestarter_{i}'] = df[df[f'{combination}'.capitalize()] == i].sort_values('chickin_date', ascending=False)
 
               for i in df[f'{combination}'.capitalize()].unique().tolist() :
                   for index, row in locals()[f'df_prestarter_{i}'].iterrows():
                       locals()[f'df_{index}'] = locals()[f'df_prestarter_{i}'][locals()[f'df_prestarter_{i}'].index == index]
 
               for i in df[f'{combination}'.capitalize()].unique().tolist() :
                       locals()[f'df_prestarter_{i}'] = locals()[f'df_prestarter_{i}'].set_index(f'{combination}'.capitalize())
                       locals()[f'df_prestarter_{i}'] = pd.concat([ 
                                                           locals()[f'df_prestarter_{i}'].iloc[0:1],
                                                           locals()[f'df_prestarter_{i}'].iloc[1:2],
                                                           locals()[f'df_prestarter_{i}'].iloc[2:3],
                                                           locals()[f'df_prestarter_{i}'].iloc[3:4],
                                                           locals()[f'df_prestarter_{i}'].iloc[4:5],
                                                           locals()[f'df_prestarter_{i}'].iloc[5:6],
                                                           locals()[f'df_prestarter_{i}'].iloc[6:7]], axis=1)
                      
 
               dflist = []
               dflist.extend(value for name, value in locals().items() if name.startswith('df_prestarter_'))
 
               r = round(pd.concat(dflist, axis=0),2).fillna(' ')
        else :
            df['chickin_date'] = df['real_chickin_date']
            df['kandang'] = df['Kandang']
            df['chickin_date'] = pd.to_datetime(df['chickin_date'])
            df = df[['chickin_date', 'kandang', f'{combination}'.capitalize(), 'prestarter_terbanyak','mortality_8', 'bw_0' ,'bw_8']]
            for i in ['bw_0', 'bw_8']:
               df[i] = pd.to_numeric(df[i], errors='coerce')
               df[i] = df[i].astype(float)
            df['laju_pertumbuhan'] = round(df['bw_8']/df['bw_0'],1)
            for i in ['prestarter_terbanyak'] :
                df[i] = df[i].replace('AAAA', None).replace('BBBB', None)
                df[i] = df[i].replace('CCCCc', 'CCCC').replace('DDDdD', 'DDDD').replace('fFFF', 'FFFF')
                df[i] = df[i].replace('gGgGG', 'GGGG').replace('HhHH', 'HHHH').replace('IiII', 'IIII').replace('JJjJ', 'JJJJ').replace('KKKkk', 'KKKK').replace('lLLLL', 'LLLL')
                df[i] = df[i].replace('MMMm', 'MMMM').replace('NNNn', 'NNNN')
 
            df.dropna(subset=['prestarter_terbanyak'], inplace=True)
 
            df = df.sort_values('chickin_date', ascending=False).groupby('prestarter_terbanyak').head(7).reset_index(drop=True).sort_values('prestarter_terbanyak')
 
            for i in df['prestarter_terbanyak'].unique().tolist() :
               locals()[f'df_prestarter_{i}'] = df[df['prestarter_terbanyak'] == i].sort_values('chickin_date', ascending=False)
 
            for i in df['prestarter_terbanyak'].unique().tolist() :
               for index, row in locals()[f'df_prestarter_{i}'].iterrows():
                   locals()[f'df_{index}'] = locals()[f'df_prestarter_{i}'][locals()[f'df_prestarter_{i}'].index == index]
 
            for i in df['prestarter_terbanyak'].unique().tolist() :
                   locals()[f'df_prestarter_{i}'] = locals()[f'df_prestarter_{i}'].set_index('prestarter_terbanyak')
                   locals()[f'df_prestarter_{i}'] = pd.concat([ 
                                                       locals()[f'df_prestarter_{i}'].iloc[0:1],
                                                       locals()[f'df_prestarter_{i}'].iloc[1:2],
                                                       locals()[f'df_prestarter_{i}'].iloc[2:3],
                                                       locals()[f'df_prestarter_{i}'].iloc[3:4],
                                                       locals()[f'df_prestarter_{i}'].iloc[4:5],
                                                       locals()[f'df_prestarter_{i}'].iloc[5:6],
                                                       locals()[f'df_prestarter_{i}'].iloc[6:7]], axis=1)
                  
 
            dflist = []
            dflist.extend(value for name, value in locals().items() if name.startswith('df_prestarter_'))
 
            r = round(pd.concat(dflist, axis=0),2).fillna(' ')    
    
    else :
        r = 'Masukkan jenis analisis yang benar'
        
    return r


