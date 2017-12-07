import pandas as pd

def clean_acs(df):
    #Use descriptive names in first row
    df.columns = df.loc[0]
    df = df.drop(0)
    df = df.drop(['Id', 'Id2'], axis=1)
    #Extract census block and tract
    df[['BLOCKCE10', 'TRACTCE10']] = df['Geography'].str.extract(
        'Block Group (\d), Census Tract (\d+\.?\d*)')
    df = df.drop(['Geography'], axis=1)
    #Drop first two columns since they only contain totals
    df = df.drop(df.columns[[0,1]], axis=1)
    #Drop margin of errors
    df = df.drop(df.columns[df.columns.str.startswith('Margin')], axis=1)
    #Convert to numbers
    df['BLOCKCE10'] = df['BLOCKCE10'].astype('int')
    df['TRACTCE10'] = df['TRACTCE10'].astype('float')
    #Multiply tract by 100 to be consistent with other data
    df['TRACTCE10'] = df['TRACTCE10'] * 100
    df['TRACTCE10'] = df['TRACTCE10'].astype('int')
    return df

#Add all file names here
acs_data = ['acs_income.csv','acs_occupancy.csv','acs_year_built.csv','acs_year_moved.csv']
#Read every csv
acs_data = map(pd.read_csv, acs_data)
#Clean every dataset
acs_data = map(clean_acs, acs_data)
#Merge datasets together
acs_data_combined = reduce(lambda x,y:x.merge(y, how='outer', on=['BLOCKCE10','TRACTCE10']), acs_data)

#Write combined data
acs_data_combined.to_csv('acs_combined.csv')