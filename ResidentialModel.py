import pandas as pd
from xgboost import XGBClassifier
from  sklearn.metrics import explained_variance_score, mean_absolute_error, mean_squared_error, r2_score

acsdata = pd.read_csv('acs_combined.csv')
acsdata = acsdata.drop(acsdata.columns[0], axis=1)

traindata = pd.read_csv('fires_old.csv')
traindata = traindata.merge(acsdata, how='left', on=['BLOCKCE10','TRACTCE10'])
traindata = traindata.drop(['TRACTCE10','BLOCKCE10'], axis=1)
traindata = traindata.fillna(0)
train_x = traindata.drop(['fire_count'], axis=1)
train_y = traindata['fire_count']

testdata = pd.read_csv('fires_new.csv')
testdata = testdata.merge(acsdata, how='left', on=['BLOCKCE10','TRACTCE10'])
testdata = testdata.drop(['TRACTCE10','BLOCKCE10'], axis=1)
testdata = testdata.fillna(0)
test_x = testdata.drop(['fire_count'], axis=1)
test_y = testdata['fire_count']

model = XGBClassifier(
    learning_rate=.2,
    n_estimators=1500,
    max_depth=5,
    min_child_weight=1,
    gamma=0,
    subsample=0.8,
    colsample_bytree=0.8,
    objective= 'reg:linear',
    nthread=4,
    seed=1)

#TODO: fix overfitting
model.fit(train_x.values, train_y.values)

pred = model.predict(test_x.values)
real = test_y.values

print explained_variance_score(real, pred)
print mean_absolute_error(real, pred)
print mean_squared_error(real, pred)
print r2_score(real, pred)