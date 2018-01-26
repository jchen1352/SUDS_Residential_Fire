#This script creates 3 files (pittdata_blocks.csv, plidata_blocks.csv, fire_incident_blocks.csv)
#that clean the data and convert spatial data to census tracts and blocks

import pandas as pd
import numpy as np

#read parcel data (matches parcels to census tract and block group
parcel_blocks = pd.read_csv('parcels.csv', encoding='utf-8')
#keep only parcel, tract, and block group
parcel_blocks = parcel_blocks[['PIN', 'TRACTCE10', 'BLOCKCE10']]
#get first digit of block, convert to int
parcel_blocks['BLOCKCE10'] = parcel_blocks['BLOCKCE10'].astype(str).str[0].astype(float)
#ignore bad parcels
parcel_blocks = parcel_blocks[parcel_blocks['PIN'] != ' ']
parcel_blocks = parcel_blocks[parcel_blocks['PIN'] != 'COMMON GROUND']
parcel_blocks = parcel_blocks[~parcel_blocks['PIN'].str.match('.*County')]

#Process pittdata
pittdata = pd.read_csv('pittdata.csv',dtype={'PROPERTYADDRESS':'str','PROPERTYHOUSENUM':'str',
                                             'STATEDESC':'str','NEIGHDESC':'str',
                                             'DEEDPAGE':'str','MABT':'str',
                                             'TAXFULLADDRESS4':'str', 'CHANGENOTICEADDRESS4':'str',
                                             'STYLE':'str','ALT_ID':'str'})

#include only residential data
pittdata = pittdata[pittdata['STATEDESC'] == 'RESIDENTIAL']
#ignore data without house number
pittdata = pittdata[pittdata['PROPERTYHOUSENUM'] != '0']

#matches parcels to street/number addresses
address_parcels = pittdata[['PARID','PROPERTYADDRESS','PROPERTYHOUSENUM']]

#picked out necessary columns
pittdata = pittdata[['PARID','PROPERTYHOUSENUM','PROPERTYADDRESS','MUNIDESC','SCHOOLDESC','NEIGHCODE',
                     'TAXDESC','OWNERDESC','USEDESC','LOTAREA','SALEPRICE','FAIRMARKETBUILDING','FAIRMARKETLAND']]

#convert pittdata to census block level
pittdata = pd.merge(pittdata, parcel_blocks, how='left', left_on=['PARID'], right_on=['PIN'])
#drop extra columns
pittdata = pittdata.drop(['PARID','PIN', 'PROPERTYHOUSENUM','PROPERTYADDRESS'], axis=1)

#group by blocks
grouped = pittdata.groupby(['TRACTCE10','BLOCKCE10'])
#change the '-DESC' columns to the most common in each group (block)
#change the other columns to the mean
max_count = lambda x:x.value_counts().index[0]
pittdata_blocks = grouped.agg({
    'MUNIDESC':max_count,'SCHOOLDESC':max_count,'NEIGHCODE':max_count,
    'TAXDESC':max_count,'OWNERDESC':max_count,'USEDESC':max_count,'LOTAREA':np.mean,
    'SALEPRICE':np.mean,'FAIRMARKETBUILDING':np.mean,'FAIRMARKETLAND':np.mean
})
#reset index to columns
pittdata_blocks = pittdata_blocks.reset_index(level=[0,1])

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
    df['BLOCKCE10'] = df['BLOCKCE10'].astype('float')
    df['TRACTCE10'] = df['TRACTCE10'].astype('float')
    #Multiply tract by 100 to be consistent with other data
    df['TRACTCE10'] = df['TRACTCE10'] * 100
    return df

#Add all acs file names here
acs_data = ['acs_income.csv','acs_occupancy.csv','acs_year_built.csv','acs_year_moved.csv']
#Read every csv
acs_data = map(pd.read_csv, acs_data)
#Clean every dataset
acs_data = map(clean_acs, acs_data)
#Merge datasets together
acs_data_combined = reduce(lambda x,y:x.merge(y, how='outer', on=['BLOCKCE10','TRACTCE10']), acs_data)

#merge pittdata with acs
pittacs = pd.merge(pittdata_blocks, acs_data_combined, how='inner', on=['BLOCKCE10','TRACTCE10'])

#write cleaned pittdata at block level
pittacs.to_csv('pittdata_blocks.csv')

#pli: Permits, Licenses, and Inspections
plidata = pd.read_csv('pli.csv',encoding='utf-8',dtype={'STREET_NUM':'str', 'STREET_NAME':'str'})
plidata = plidata[['PARCEL','INSPECTION_DATE','INSPECTION_RESULT','VIOLATION']]

plidata['INSPECTION_DATE'] = pd.to_datetime(plidata['INSPECTION_DATE'])

#get only residential data
plidata = pd.merge(plidata, address_parcels[['PARID']], how='inner', left_on=['PARCEL'], right_on=['PARID'])
#get census block from parcel
plidata = pd.merge(plidata, parcel_blocks, how='left', left_on=['PARCEL'], right_on=['PIN'])
#drop extra columns
plidata = plidata.drop(['PARCEL','PARID','PIN'], axis=1)
#write cleaned plidata at census block level
#NOTE: Reason for not merging plidata with pittdata: plidata is temporal while pittdata is constant
plidata.to_csv('plidata_blocks.csv')

#loading fire incidents csvs
#IMPORTANT: Fire_Incidents_Pre14.csv has a bad byte at position 131, delete it to run code
fire_pre14 = pd.read_csv('Fire_Incidents_Pre14.csv', encoding ='utf-8', dtype={'street': 'str', 'number': 'str'})
del_columns = ['CALL_NO','inci_id','arv_dttm','alarms','..AGENCY','PRIMARY_UNIT','XCOORD',
               'YCOORD','CALL_CREATED_DATE','MAP_PAGE','full.code','descript','response_time']
fire_pre14 = fire_pre14.drop(del_columns, axis=1)
fire_pre14 = fire_pre14.drop(fire_pre14.columns[0], axis=1)

#reading the fire_historicalfile
fire_historical = pd.read_csv('Fire_Incidents_Historical.csv',encoding = 'utf-8',dtype={'street':'str','number':'str'})
del_columns = ['CALL_NO','inci_id','arv_dttm','alarms','pbf_narcan','meds_glucose','meds_epi',
               'meds_nitro','pbf_albut','cpr','car_arr','aed','none','pbf_lift_ass','Med_Assist',
               'Lift_Ref','Card_CPR','AGENCY','PRIMARY_UNIT','XCOORD','YCOORD','LOCATION',
               'CALL_CREATED_DATE','MAP_PAGE','CURR_DGROUP','REP_DIST','full.code','descript','response_time']
fire_historical = fire_historical.drop(del_columns, axis=1)
fire_historical = fire_historical.drop(fire_historical.columns[0], axis=1)

fire_historical = fire_historical.append(fire_pre14, ignore_index=True)
del fire_pre14

#correcting problems with the street column
fire_historical['street'] = fire_historical['street'].replace(to_replace=', PGH', value='', regex=True)
fire_historical['street'] = fire_historical['street'].replace(to_replace=', P', value='', regex=True)
fire_historical['street'] = fire_historical['street'].replace(to_replace=',', value='', regex=True)
fire_historical['street'] = fire_historical['street'].replace(to_replace='#.*', value='', regex=True)
fire_historical['street'] = fire_historical['street'].str.strip()
fire_historical['number'] = fire_historical['number'].str.strip()
fire_historical['st_type'] = fire_historical['st_type'].str.strip()
fire_historical['st_prefix'] = fire_historical['st_prefix'].str.strip()
fire_historical['street'] = fire_historical['st_prefix'] + ' ' + \
                            fire_historical['street'] + ' ' + fire_historical['st_type']
fire_historical['street'] = fire_historical['street'].str.strip()
fire_historical['st_type'] = fire_historical['st_type'].str.replace('AV$', 'AVE')
fire_historical['street'] = fire_historical['street'].str.strip()

#Split street column when there are 2 streets
street_split = fire_historical['street'].str.split('/')
fire_historical['street'] = street_split.map(lambda x:x[0])
fire_historical['street2'] = street_split.map(lambda x:x[1] if len(x) > 1 else np.nan)

#dropping unneeded street columns
del_columns = ['st_prefix','st_type','st_suffix']
fire_historical = fire_historical.drop(del_columns, axis=1)

#dropping rows with no number or street
fire_historical = fire_historical[fire_historical['number'] != '']
fire_historical = fire_historical[fire_historical['street'] != '']

#making the fire column with all type 100s as fires
fire_historical['fire'] = fire_historical['inci_type'].astype(str).map(lambda x:1 if x[0]=='1' else 0)

#converting to date time
fire_historical = fire_historical.dropna(subset=['alm_dttm'])
fire_historical['alm_dttm'] = pd.to_datetime(fire_historical['alm_dttm'])

fire_historical = fire_historical.drop_duplicates()

#first convert from addresses to parcels
fire_historical = pd.merge(fire_historical, address_parcels, how='left',
                           left_on=['street','number'], right_on=['PROPERTYADDRESS','PROPERTYHOUSENUM'])
#then convert from parcels to census blocks
fire_historical = pd.merge(fire_historical, parcel_blocks, how='left', left_on=['PARID'], right_on=['PIN'])
#drop extra columns
fire_historical = fire_historical.drop(['number','street','street2','PARID','PROPERTYADDRESS',
                                        'PROPERTYHOUSENUM','PIN'], axis=1)
#drop data without block or tract (this drops non-residential data)
fire_historical = fire_historical.dropna(subset=['TRACTCE10','BLOCKCE10'])

fire_historical = fire_historical.drop_duplicates();

#write cleaned fire incident data at census block level
fire_historical.to_csv('fire_incident_blocks.csv')