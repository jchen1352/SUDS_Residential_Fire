import pandas as pd
import numpy as np
import datetime as dt

#Date to split data between test and train
date_split = dt.date(2016, 7, 1)

#pli: Permits, Licenses, and Inspections
plidata = pd.read_csv('pli.csv',encoding='utf-8',dtype={'STREET_NUM':'str', 'STREET_NAME':'str'})
plidata = plidata[['PARCEL','INSPECTION_DATE','INSPECTION_RESULT','VIOLATION']]

#One-hot encoding inspection result and violations
inspection_results = pd.get_dummies(plidata['INSPECTION_RESULT'])
violations = plidata['VIOLATION'].str.get_dummies(sep=' :: ')
plidata = pd.concat([plidata, inspection_results, violations], axis=1)
plidata = plidata.drop(['INSPECTION_RESULT','VIOLATION'], axis=1)
del inspection_results
del violations

#Separate by date
plidata['INSPECTION_DATE'] = pd.to_datetime(plidata['INSPECTION_DATE'])
plidata_old = plidata[plidata['INSPECTION_DATE'] < date_split]
plidata_new = plidata[plidata['INSPECTION_DATE'] >= date_split]
del plidata

#Aggregate data from same parcel
plidata_old = plidata_old.groupby('PARCEL').aggregate(np.sum)
plidata_new = plidata_new.groupby('PARCEL').aggregate(np.sum)

#Process pittdata
pittdata = pd.read_csv('pittdata.csv',dtype={'PROPERTYADDRESS':'str','PROPERTYHOUSENUM':'str',
                                             'STATEDESC':'str','NEIGHDESC':'str',
                                             'DEEDPAGE':'str','MABT':'str',
                                             'TAXFULLADDRESS4':'str', 'CHANGENOTICEADDRESS4':'str',
                                             'STYLE':'str','ALT_ID':'str'})

#include only residential data
pittdata = pittdata[pittdata['STATEDESC']=='RESIDENTIAL']
pittdata = pittdata[pittdata['PROPERTYHOUSENUM'] != '0']

#drop columns with less than 15% data
pittdata = pittdata.dropna(thresh=int(pittdata.shape[0]*.15), axis=1)

#drop columns with only one value
#drop columns that end in 'DESC' because they are the same as another column
#drop several other unnecessary columns
del_columns = ['PROPERTYOWNER','AGENT','TAXFULLADDRESS1','TAXFULLADDRESS2','TAXFULLADDRESS3',
               'TAXFULLADDRESS4','CHANGENOTICEADDRESS1','CHANGENOTICEADDRESS2',
               'CHANGENOTICEADDRESS3','CHANGENOTICEADDRESS4']
pitt_temp = pittdata
for col in pittdata.columns:
    if col in del_columns or col.endswith('DESC') or len(pittdata[col].unique()) == 1:
        pitt_temp = pitt_temp.drop(col, axis=1)
pittdata = pitt_temp
del pitt_temp

#merging pli with city of pitt
plipca_old = pd.merge(pittdata, plidata_old, how = 'left', left_on =['PARID'], right_index=True)
plipca_new = pd.merge(pittdata, plidata_new, how = 'left', left_on =['PARID'], right_index=True)
del pittdata
del plidata_old
del plidata_new

#drop duplicate addresses, don't really care about lost data because aggregating at census block level
plipca_old = plipca_old.drop_duplicates(subset=['PROPERTYADDRESS','PROPERTYHOUSENUM'])
plipca_new = plipca_new.drop_duplicates(subset=['PROPERTYADDRESS','PROPERTYHOUSENUM'])

#loading fire incidents csvs
#IMPORTANT: Fire_Incidents_Pre14.csv has a bad byte at position 131, delete it to run code
fire_pre14 = pd.read_csv('Fire_Incidents_Pre14.csv', encoding ='utf-8', dtype={'street': 'str', 'number': 'str'})
del_columns = ['CALL_NO','inci_id','arv_dttm','alarms','..AGENCY','PRIMARY_UNIT','XCOORD',
               'YCOORD','CALL_CREATED_DATE','MAP_PAGE','full.code']
fire_pre14 = fire_pre14.drop(del_columns, axis=1)
fire_pre14 = fire_pre14.drop(fire_pre14.columns[0], axis=1)

#reading the fire_historicalfile
fire_historical = pd.read_csv('Fire_Incidents_Historical.csv',encoding = 'utf-8',dtype={'street':'str','number':'str'})
del_columns = ['CALL_NO','inci_id','arv_dttm','alarms','pbf_narcan','meds_glucose','meds_epi',
               'meds_nitro','pbf_albut','cpr','car_arr','aed','none','pbf_lift_ass','Med_Assist',
               'Lift_Ref','Card_CPR','AGENCY','PRIMARY_UNIT','XCOORD','YCOORD','LOCATION',
               'CALL_CREATED_DATE','MAP_PAGE','CURR_DGROUP','REP_DIST','full.code']
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

# making the fire column with all type 100s as fires
fire_historical['fire'] = fire_historical['inci_type'].astype(str).map(lambda x:1 if x[0]=='1' else 0)

#converting to date time
fire_historical = fire_historical.dropna(subset=['alm_dttm'])
fire_historical['alm_dttm'] = pd.to_datetime(fire_historical['alm_dttm'])

fire_historical = fire_historical.drop_duplicates()

#split fire_historical by date
fire_historical_old = fire_historical[fire_historical['alm_dttm'] < date_split]
fire_historical_new = fire_historical[fire_historical['alm_dttm'] >= date_split]
del fire_historical

#combining addresses so number and street have no duplicates
#experiment with fire column being any fire or count fires
fire_historical_old['fire_count'] = fire_historical_old.groupby(['number','street'])['fire'].transform(np.sum)
fire_historical_new['fire_count'] = fire_historical_new.groupby(['number','street'])['fire'].transform(np.sum)

#not sure if other columns are necessary
fire_historical_old = fire_historical_old[['number','street','fire_count']]
fire_historical_new = fire_historical_new[['number','street','fire_count']]
fire_historical_old = fire_historical_old.drop_duplicates(subset=['number','street'])
fire_historical_new = fire_historical_new.drop_duplicates(subset=['number','street'])

#merge with plipca data
pcafire_old = pd.merge(plipca_old, fire_historical_old, how='left',
                       left_on=['PROPERTYADDRESS','PROPERTYHOUSENUM'], right_on=['street','number'])
pcafire_new = pd.merge(plipca_new, fire_historical_new, how='left',
                       left_on=['PROPERTYADDRESS','PROPERTYHOUSENUM'], right_on=['street','number'])
del plipca_old
del plipca_new
pcafire_old = pcafire_old.drop(['street','number'], axis=1)
pcafire_new = pcafire_new.drop(['street','number'], axis=1)

#read parcel data
parceldata = pd.read_csv('parcels.csv', encoding='utf-8')
#keep only parcel, tract, and block group
parceldata = parceldata[['PIN','TRACTCE10','BLOCKCE10']]
parceldata['BLOCKCE10'] = parceldata['BLOCKCE10'].astype(str).str[0]
#ignore bad parcels
parceldata = parceldata[parceldata['PIN'] != ' ']
parceldata = parceldata[parceldata['PIN'] != 'COMMON GROUND']
parceldata = parceldata[~parceldata['PIN'].str.match('.*County')]

#merge with pcafire
#TODO: use other columns of pcafire from pittdata instead of dropping
del_columns = ['PROPERTYUNIT','PROPERTYZIP','MUNICODE','SCHOOLCODE','NEIGHCODE','TAXCODE',
               'OWNERCODE','USECODE','LOTAREA','HOMESTEADFLAG','SALEDATE','SALEPRICE',
               'SALECODE','DEEDBOOK','DEEDPAGE','COUNTYBUILDING','COUNTYLAND','COUNTYTOTAL',
               'LOCALBUILDING','LOCALLAND','LOCALTOTAL','FAIRMARKETBUILDING','FAIRMARKETLAND',
               'FAIRMARKETTOTAL','STYLE','STORIES','YEARBLT','EXTERIORFINISH','ROOF','BASEMENT',
               'GRADE','CONDITION','TOTALROOMS','BEDROOMS','FULLBATHS','HALFBATHS','HEATINGCOOLING',
               'FIREPLACES','ATTACHEDGARAGES','FINISHEDLIVINGAREA','CARDNUMBER']
pcafire_old = pcafire_old.drop(del_columns, axis=1)
pcafire_new = pcafire_new.drop(del_columns, axis=1)

#merge parcel data with fires
parcel_fires_old = pd.merge(parceldata, pcafire_old, how='left', left_on='PIN', right_on='PARID')
parcel_fires_new = pd.merge(parceldata, pcafire_new, how='left', left_on='PIN', right_on='PARID')
blocks = parcel_fires_old.groupby(['TRACTCE10','BLOCKCE10'])
parcel_fires_old = blocks.aggregate(np.sum)
blocks = parcel_fires_new.groupby(['TRACTCE10','BLOCKCE10'])
parcel_fires_new = blocks.aggregate(np.sum)
parcel_fires_old['fire_count'] = parcel_fires_old['fire_count'].fillna(0)
parcel_fires_new['fire_count'] = parcel_fires_new['fire_count'].fillna(0)

#write cleaned data
#contains plidata and fire count
#may include pittdata in the future
parcel_fires_old.to_csv('fires_old.csv')
parcel_fires_new.to_csv('fires_new.csv')