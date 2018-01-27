import pandas as pd
from xgboost import XGBClassifier
from sklearn.feature_selection import SelectFromModel
from sklearn import metrics
from sklearn.metrics import confusion_matrix

def get_dummies(df, columns):
    dummies = [pd.get_dummies(df[c]) for c in columns]
    df = pd.concat([df] + dummies, axis=1)
    df = df.drop(columns, axis=1)
    return df

#read pittdata
pittdata = pd.read_csv('pittdata_blocks.csv')
#1st column is unneeded indices
pittdata = pittdata.drop(pittdata.columns[0], axis=1)

#contains only block and tract
blocks = pittdata[['TRACTCE10','BLOCKCE10']]

#read incident data
incidents = pd.read_csv('fire_incident_blocks.csv')
incidents = incidents.drop(incidents.columns[0], axis=1)
incidents['alm_dttm'] = pd.to_datetime(incidents['alm_dttm'])

#keep non-fire incidents as features
nonfire_incidents = incidents[incidents['fire'] != 1]
nonfire_incidents = nonfire_incidents.drop('fire', axis=1)
#one-hot encode fire codes
nonfire_incidents = get_dummies(nonfire_incidents, ['inci_type'])

#read plidata
plidata = pd.read_csv('plidata_blocks.csv')
plidata = plidata.drop(plidata.columns[0], axis=1)
plidata['INSPECTION_DATE'] = pd.to_datetime(plidata['INSPECTION_DATE'])

#group by every certain period of time
#earliest data is 2009
#(also resampling is really dumb, idk how to do every 6 months starting january)
period = 'Q' #1 quarter=3 months
incident_groups = incidents.groupby(pd.Grouper(key='alm_dttm', freq=period))
nonfire_groups = nonfire_incidents.groupby(pd.Grouper(key='alm_dttm', freq=period))
plidata_groups = plidata.groupby(pd.Grouper(key='INSPECTION_DATE', freq=period))

#contains time period, block, tract, and whether there was a fire in that time and area
incidents_divided = incident_groups.apply(lambda df:
                                          pd.merge(df, blocks, how='right', on=['TRACTCE10','BLOCKCE10'])
                                          .fillna(0)
                                          .groupby(['TRACTCE10','BLOCKCE10'])[['fire']].max())
#incidents_divided = incident_groups.apply(lambda df:df.groupby(['TRACTCE10','BLOCKCE10'])['fire'].max())
#similar, but with every non-fire code
nonfire_divided = nonfire_groups.apply(lambda df:
                                       pd.merge(df, blocks, how='right', on=['TRACTCE10','BLOCKCE10'])
                                       .fillna(0)
                                       .groupby(['TRACTCE10','BLOCKCE10']).max())
nonfire_divided = nonfire_divided.drop('alm_dttm', axis=1)

#one hot encode plidata and divide into time periods
def f(df):
    df = pd.merge(df, blocks, how='right', on=['TRACTCE10','BLOCKCE10'])
    result_dummies = pd.get_dummies(df['INSPECTION_RESULT'])
    violation_dummies = df['VIOLATION'].str.get_dummies(sep=' :: ')
    df = pd.concat([df, result_dummies, violation_dummies], axis=1)
    df = df.drop(['INSPECTION_RESULT','VIOLATION'], axis=1)
    return df.groupby(['TRACTCE10','BLOCKCE10']).sum()
plidata_divided = plidata_groups.apply(f)

#join incidents and pli together
incidents_divided = incidents_divided.reset_index()
nonfire_divided = nonfire_divided.reset_index()
plidata_divided = plidata_divided.reset_index()
incidents_temp = pd.merge(incidents_divided, nonfire_divided, how='outer', on=['alm_dttm','TRACTCE10','BLOCKCE10'])
incidents_pli = pd.merge(incidents_temp, plidata_divided, how='outer',
                         left_on = ['alm_dttm','TRACTCE10','BLOCKCE10'],
                         right_on=['INSPECTION_DATE','TRACTCE10','BLOCKCE10'])
incidents_pli['alm_dttm'] = incidents_pli['alm_dttm'].fillna(incidents_pli['INSPECTION_DATE'])
incidents_pli = incidents_pli.drop('INSPECTION_DATE', axis=1)
incidents_pli = incidents_pli.fillna(0)

#drop columns with fewer than 20 values
s = incidents_pli.sum()
incidents_pli = incidents_pli.drop(s[s < 20].index, axis=1)

#sliding windows: add previous period data as features
inc_pli_offset = incidents_pli.copy()
inc_pli_offset['alm_dttm'] = inc_pli_offset['alm_dttm'] + pd.DateOffset(months=3)
inc_pli_offset['alm_dttm'] = inc_pli_offset['alm_dttm'].dt.to_period('M').dt.to_timestamp('M')
incidents_pli = pd.merge(incidents_pli, inc_pli_offset, how='inner',
               on=['alm_dttm','TRACTCE10','BLOCKCE10'], suffixes=('','_prev'))

#join with pittdata
combined = pd.merge(pittdata, incidents_pli, how='left', on=['TRACTCE10','BLOCKCE10'])
combined = combined.fillna(0)

#one-hot encode pittdata columns
columns = ['OWNERDESC','MUNIDESC','SCHOOLDESC','TAXDESC','USEDESC','NEIGHCODE']
combined = get_dummies(combined, columns)

#separate into training and testing: testing is last few months
date_sep = '2016-06-30'
traindata = combined[combined['alm_dttm'] <= date_sep]
traindata = traindata.drop(['alm_dttm','TRACTCE10','BLOCKCE10'], axis=1)
X_train = traindata.drop(['fire'], axis=1)
y_train = traindata['fire']

testdata = combined[combined['alm_dttm'] > date_sep]
testdata = testdata.drop(['alm_dttm','TRACTCE10','BLOCKCE10'], axis=1)
X_test = testdata.drop(['fire'], axis=1)
y_test = testdata['fire']

from sklearn.ensemble import AdaBoostClassifier

model = AdaBoostClassifier(n_estimators = 65, random_state=27)
model.fit(X_train, y_train)

from sklearn.metrics import cohen_kappa_score

def score(model, X, y):
    pred = model.predict(X)
    real = y
    cm = confusion_matrix(real, pred)
    print confusion_matrix(real, pred)

    kappa = cohen_kappa_score(real, pred)
    fpr, tpr, thresholds = metrics.roc_curve(y_test, pred, pos_label=1)
    roc_auc = metrics.auc(fpr, tpr)
    print 'Accuracy = ', float(cm[0][0] + cm[1][1]) / len(real)
    print 'kappa score = ', kappa
    print 'AUC Score = ', metrics.auc(fpr, tpr)
    print 'recall = ', tpr[1]
    print 'precision = ', float(cm[1][1]) / (cm[1][1] + cm[0][1])

score(model, X_test, y_test)