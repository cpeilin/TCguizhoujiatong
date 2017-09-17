import pandas as pd
import numpy as np
from sklearn.cross_validation import train_test_split
import xgboost as xgb
import operator
import matplotlib
matplotlib.use("Agg") #Needed to save figures
import matplotlib.pyplot as plt
import warnings

def AddBaseTimeFeature(df):
    df['time_interval_begin'] = pd.to_datetime(df['time_interval'].map(lambda x: x[1:20]))
    #df = df.drop(['date', 'time_interval'], axis=1)
    df['time_interval_month'] = df['time_interval_begin'].map(lambda x: x.strftime('%m'))
    df['time_interval_day'] = df['time_interval_begin'].map(lambda x: x.day)
    df['time_interval_begin_hour'] = df['time_interval_begin'].map(lambda x: x.strftime('%H'))
    df['time_interval_minutes'] = df['time_interval_begin'].map(lambda x: x.strftime('%M'))
    # Monday=1, Sunday=7
    df['time_interval_week'] = df['time_interval_begin'].map(lambda x: x.weekday() + 1)
    return df
##读数据并对时间进行拆分
link_info = pd.read_table('gy_contest_link_info.txt',sep=';')
link_info = link_info.sort_values('link_ID')
training_data = pd.read_table('quaterfinal_gy_cmp_training_traveltime.txt',sep=';')
print (training_data.shape)
training_data = pd.merge(training_data,link_info,on='link_ID')
testing_data = pd.read_table('semifinal_gy_cmp_testing_template_seg2.txt',sep=';',header=None)
testing_data.columns = ['link_ID', 'date', 'time_interval', 'travel_time']
testing_data = pd.merge(testing_data,link_info,on='link_ID')
print (testing_data.shape)
feature_date = pd.concat([training_data,testing_data],axis=0)
feature_date = feature_date.sort_values(['link_ID','time_interval'])
#print feature_date
feature_date.to_csv('semifinalfeature_data.csv',index=False)

feature_data = pd.read_csv('semifinalfeature_data.csv')
feature_data_date = AddBaseTimeFeature(feature_data)
#print feature_data_date
feature_data_date.to_csv('semifinalfeature_data.csv',index=False)
import math
import matplotlib.pyplot as plt
from scipy.stats import mode
# 中位数
def mode_function(df):
    counts = mode(df)
    return counts[0][0]
def maxmin_function(df):
    counts = max(df)-min(df)
    return counts

feature_data = pd.read_csv('semifinalfeature_data.csv')
print(len(feature_data))
week = pd.get_dummies(feature_data['time_interval_week'],prefix='week')
feature_data = pd.concat([feature_data,week],axis=1)
print (feature_data.head())

#产生训练集用4月5月做训练集
train = feature_data.loc[(feature_data.time_interval_month <= 5)&((feature_data.time_interval_begin_hour==8)|
                            (feature_data.time_interval_begin_hour==15)|(feature_data.time_interval_begin_hour==18)),: ]#&(feature_data['date']>'2017-01-01')
print(len(train))
##1
for i in [58,56,52,48,38,18,0]:
    tmp = feature_data.loc[(feature_data.time_interval_month <=5)&((feature_data.time_interval_begin_hour==7)|
                            (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                           (feature_data.time_interval_minutes >= i),:]
    tmp1 = tmp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean_%d' % (i), np.mean), ('median_%d' % (i), np.median),('min_%d' % (i),  maxmin_function),
                                ('mode_%d' % (i), mode_function), ('std_%d' % (i), np.std), ('max_%d' % (i), np.max),('sun_%d' % (i), np.sum)]).reset_index()
    tmp1['time_interval_begin_hour']=tmp1['time_interval_begin_hour']+1 
    train = pd.merge(train,tmp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
train['last1h']=0.5*train['median_0']+0.5*train['mode_0']
train['last1hsub']=0.5*train['median_0']-0.5*train['mode_0']    

del tmp
del tmp1
###2对应当天累积到目前为止前一小时的累积量时间衰减

tmp = feature_data.loc[(feature_data.time_interval_month == 5)&((feature_data.time_interval_begin_hour==7)|
                           (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                           (feature_data.time_interval_minutes >= 0),:]

tmp['weights'] = tmp['time_interval_minutes'].map(lambda x: math.exp(-(60-x)))
tmp['weights2'] = tmp['weights']*tmp['travel_time']
tmp2 = tmp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])['weights2'].agg([('exp', np.sum)]).reset_index()

tmp2['time_interval_begin_hour']=tmp2['time_interval_begin_hour']+1 
train = pd.merge(train,tmp2,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del tmp
del tmp2

###3每隔十分钟的路程
for i in [60,50,40,30,20,10]:
    temp = feature_data.loc[(feature_data.time_interval_month == 4)&((feature_data.time_interval_begin_hour==8)|
                         (feature_data.time_interval_begin_hour==15)|(feature_data.time_interval_begin_hour==18))&
                          ((feature_data.time_interval_minutes >=(i-10))&(feature_data.time_interval_minutes <=i)),:]  ###912gai==4  
    temp1 = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean1_%d' % (i), np.mean), ('median1_%d' % (i), np.median),('min1_%d' % (i), np.min),
                                ('mode1_%d' % (i), mode_function), ('std1_%d' % (i), np.std), ('max1_%d' % (i), np.max),('sun1_%d' % (i), np.sum)]).reset_index()    

    train = pd.merge(train,temp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp
del temp1
#4后一个小时每个十分钟
for i in [60,50,40,30,20,10]:
    temp = feature_data.loc[(feature_data.time_interval_month == 4)&((feature_data.time_interval_begin_hour==9)|
                         (feature_data.time_interval_begin_hour==16)|(feature_data.time_interval_begin_hour==19))&
                          ((feature_data.time_interval_minutes <=(i-10))&(feature_data.time_interval_minutes <=i)),:]   ###912gai==4 
    temp1 = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('meanb_%d' % (i), np.mean), ('medianb_%d' % (i), np.median),('minb_%d' % (i), np.min),
                                ('modeb_%d' % (i), mode_function), ('stdb_%d' % (i), np.std), ('maxb_%d' % (i), np.max),('sunb_%d' % (i), np.sum)]).reset_index()    
    temp1['time_interval_begin_hour']=temp1['time_interval_begin_hour']-1 
    train = pd.merge(train,temp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp
del temp1
    
    
    
#5month<=5,after 7:50 后按照周来group的数值特征
temp = feature_data.loc[(feature_data.time_interval_month<= 5)&((feature_data.time_interval_begin_hour==7)|
                         (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                         (feature_data.time_interval_minutes >=50),:]          
temp = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean2', np.mean), ('median2' , np.median),('min2' , np.min),
                                ('mode2', mode_function), ('std2' , np.std),('sun2', np.sum),('max2', np.max)]).reset_index()    
temp['time_interval_begin_hour']=temp['time_interval_begin_hour']+1 
train = pd.merge(train,temp,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp 

#6对应当天7:50后的最大值
temp = feature_data.loc[(feature_data.time_interval_month == 5)&((feature_data.time_interval_begin_hour==7)|
                         (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                         (feature_data.time_interval_minutes >=50),:] 
temp = temp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])[
            'travel_time'].agg([('max750', np.max),('mean750', np.mean), ('median750',np.median),('min750',maxmin_function),
                                ('mode750', mode_function), ('std750',np.std),('sun750',np.sum)]).reset_index()    
temp['time_interval_begin_hour']=temp['time_interval_begin_hour']+1 
train = pd.merge(train,temp,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del temp 

tmp = feature_data.loc[(feature_data.time_interval_month == 5)&((feature_data.time_interval_begin_hour==7)|
                     (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                      (feature_data.time_interval_minutes >=0),:]    
tmp['weights'] = tmp['time_interval_minutes'].map(lambda x: math.exp(-(60-x)))
tmp['weights2'] = tmp['weights']*tmp['travel_time']
tmp2 = tmp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])['weights2'].agg([('exp', np.sum)]).reset_index()

##上下游特征
intop=pd.read_csv('lzh_in_top.csv')
intop = pd.merge(intop,tmp2, left_on='in_top',right_on='link_ID',how='inner')
# intop['lengthsub']=intop['in_top_width']/intop['link_id_width']
intop= intop.fillna(0)
temp2= intop.groupby(['link_ID_x','time_interval_day','time_interval_begin_hour'])['exp'].agg([('meanin', np.mean)]).reset_index()    
temp2['time_interval_begin_hour']=temp2['time_interval_begin_hour']+1 
temp2['link_ID']=temp2['link_ID_x']
temp2=temp2.drop(['link_ID_x'],axis=1)
train = pd.merge(train,temp2,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del temp2
del tmp
del tmp2
del intop
train['subexp']=train['exp']-train['meanin']

#差值特征
train['subs1']=train['max750']-train['median2']
train['subs3']=train['mode750']-train['mode2']
train['subs4']=train['median750']-train['maxb_20']
train['subs6']=train['median1_60']-train['medianb_20']
train=train.fillna(method='ffill')
train=train.fillna(0)
train.to_csv('train.csv',index=False)
train_label = np.log1p(train.pop('travel_time'))
####################################################
#验证集：线下预测6月的值
test = feature_data.loc[(feature_data.time_interval_month== 6)&((feature_data.time_interval_begin_hour==8)|
                            (feature_data.time_interval_begin_hour==15)|(feature_data.time_interval_begin_hour==18)),: ]#|(feature_data.time_interval_begin_hour==15)|(feature_data.time_interval_begin_hour==18))
# test = feature_data.loc[(feature_data.time_interval_month== 6)&(
#                             (feature_data.time_interval_begin_hour==15)),: ]#


print(len(test))
# test = pd.read_csv('feature_test1_m456.csv')
##1累积到目前为止前一个小时每分钟按照周分组的历史
for i in [58,56,52,48,38,18,0]:
    tmp = feature_data.loc[(feature_data.time_interval_month <= 6)&((feature_data.time_interval_begin_hour==7)|
                           (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                           (feature_data.time_interval_minutes >= i),:]
   
    tmp1 = tmp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean_%d' % (i), np.mean), ('median_%d' % (i), np.median),('min_%d' % (i), np.min),
                                ('mode_%d' % (i), mode_function), ('std_%d' % (i), np.std), ('max_%d' % (i), np.max),('sun_%d' % (i), np.sum)]).reset_index()
    tmp1['time_interval_begin_hour']=tmp1['time_interval_begin_hour']+1 
    
    test = pd.merge(test,tmp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
test['last1h']=0.5*test['median_0']+0.5*test['mode_0']
test['last1hsub']=0.5*test['median_0']-0.5*test['mode_0']  
    
del tmp
del tmp1
###2对应当天累积到目前为止前一小时的累积量时间衰减
tmp = feature_data.loc[(feature_data.time_interval_month == 6)&((feature_data.time_interval_begin_hour==7)|
                           (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                           (feature_data.time_interval_minutes >= 0),:]

tmp['weights'] = tmp['time_interval_minutes'].map(lambda x: math.exp(-(60-x)))
tmp['weights2'] = tmp['weights']*tmp['travel_time']
tmp2 = tmp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])['weights2'].agg([('exp', np.sum)]).reset_index()
tmp2['time_interval_begin_hour']=tmp2['time_interval_begin_hour']+1 
test = pd.merge(test,tmp2,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del tmp
del tmp2

###3每隔十分钟的路程
for i in [60,50,40,30,20,10]:
    temp = feature_data.loc[(feature_data.time_interval_month <= 5)&((feature_data.time_interval_begin_hour==8)|
                         (feature_data.time_interval_begin_hour==15)|(feature_data.time_interval_begin_hour==18))&
                          ((feature_data.time_interval_minutes >=(i-10))&(feature_data.time_interval_minutes <=i)),:]    
    temp1 = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean1_%d' % (i), np.mean), ('median1_%d' % (i), np.median),('min1_%d' % (i), np.min),
                                ('mode1_%d' % (i), mode_function), ('std1_%d' % (i), np.std), ('max1_%d' % (i), np.max),('sun1_%d' % (i), np.sum)]).reset_index()    

    test = pd.merge(test,temp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp
del temp1
#4后一个小时每个十分钟
for i in [60,50,40,30,20,10]:
    temp = feature_data.loc[(feature_data.time_interval_month <= 5)&((feature_data.time_interval_begin_hour==9)|
                         (feature_data.time_interval_begin_hour==16)|(feature_data.time_interval_begin_hour==19))&
                          ((feature_data.time_interval_minutes <=(i-10))&(feature_data.time_interval_minutes <=i)),:]    
    temp1 = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('meanb_%d' % (i), np.mean), ('medianb_%d' % (i), np.median),('minb_%d' % (i), np.min),
                                ('modeb_%d' % (i), mode_function), ('stdb_%d' % (i), np.std), ('maxb_%d' % (i), np.max),('sunb_%d' % (i), np.sum)]).reset_index()    
    temp1['time_interval_begin_hour']=temp1['time_interval_begin_hour']-1 
    test = pd.merge(test,temp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp
del temp1



#5month==6,after 7:50 travel_time_medium
temp = feature_data.loc[(feature_data.time_interval_month <= 6)&((feature_data.time_interval_begin_hour==7)|
                         (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                         (feature_data.time_interval_minutes >=50),:]          
temp = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean2', np.mean), ('median2',np.median),('min2',np.min),
                                ('mode2', mode_function), ('std2',np.std),('sun2',np.sum),('max2', np.max)]).reset_index() ##按天数？   
temp['time_interval_begin_hour']=temp['time_interval_begin_hour']+1 
test = pd.merge(test,temp,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp

#6当天7:50后的最大值
temp = feature_data.loc[(feature_data.time_interval_month == 6)&((feature_data.time_interval_begin_hour==7)|
                         (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                         (feature_data.time_interval_minutes >=50),:] 
temp = temp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])[
            'travel_time'].agg([('max750', np.max),('mean750', np.mean), ('median750',np.median),('min750',maxmin_function),
                                ('mode750', mode_function), ('std750',np.std),('sun750',np.sum)]).reset_index()    
temp['time_interval_begin_hour']=temp['time_interval_begin_hour']+1 
test = pd.merge(test,temp,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del temp 
# test['ratio1']=np.log1p((test['max750']+0.1)/test['length'])
# test['ratio2']=np.log1p((test['median2']+0.1)/test['length'])
# test['ratio3']=np.log1p((test['median2']+0.1)/(test['length']*test['width']))
###上一条路上一个小时的时间衰减
tmp = feature_data.loc[(feature_data.time_interval_month == 6)&((feature_data.time_interval_begin_hour==7)|
                     (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                      (feature_data.time_interval_minutes >=0),:]    
tmp['weights'] = tmp['time_interval_minutes'].map(lambda x: math.exp(-(60-x)))
tmp['weights2'] = tmp['weights']*tmp['travel_time']
tmp2 = tmp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])['weights2'].agg([('exp', np.sum)]).reset_index()

intop=pd.read_csv('lzh_in_top.csv')
intop = pd.merge(intop,tmp2, left_on='in_top',right_on='link_ID',how='inner')
# intop['lengthsub']=intop['in_top_width']/intop['link_id_width']
intop= intop.fillna(0)
temp2= intop.groupby(['link_ID_x','time_interval_day','time_interval_begin_hour'])['exp'].agg([('meanin', np.mean)]).reset_index()    
temp2['time_interval_begin_hour']=temp2['time_interval_begin_hour']+1 
temp2['link_ID']=temp2['link_ID_x']
temp2=temp2.drop(['link_ID_x'],axis=1)
test = pd.merge(test,temp2,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del temp2
del tmp
del tmp2
del intop
test['subexp']=test['exp']-test['meanin']

test['subs1']=test['max750']-test['median2']
test['subs3']=test['mode750']-test['mode2']
test['subs4']=test['median750']-test['maxb_20']
test['subs6']=test['median1_60']-test['medianb_20']
test=test.fillna(method='ffill')
test=test.fillna(0)
test.to_csv('test.csv',index=False)
test_label = np.log1p(test.pop('travel_time'))##返回被弹出列的值，这是一种删除列的方法，不同的是会返回改列
train.drop(['time_interval_month','time_interval_begin','date', 'time_interval'],inplace=True,axis=1)
test.drop(['time_interval_month','time_interval_begin','date', 'time_interval'],inplace=True,axis=1)

###线下cv
def mape_object(y,d):
    g=1.0*np.sign(y-d)/y
    h=1.0/y
    return -g,h

# 评价函数
def mape(y,d):
    c=d.get_label()
    result=- np.sum(np.abs(y-c)/c)/len(c)
    return "mape",result

# 评价函数ln形式
def mape_ln(y,d):
    c=d.get_label()
    result=np.sum(np.abs(np.expm1(y)-np.abs(np.expm1(c)))/np.abs(np.expm1(c)))/len(c)
    return "mape",result



import xgboost as xgb

xlf = xgb.XGBRegressor(max_depth=6,
                       learning_rate=0.02,
                       n_estimators=215,
                       silent=True,
                       objective=mape_object,#'reg:linear',
                       gamma=0,
                       min_child_weight=5,
                       max_delta_step=0,
                       subsample=0.8,
                       colsample_bytree=0.8,
                       colsample_bylevel=1,
                       reg_alpha=1e0,
                       reg_lambda=0,
                       scale_pos_weight=1,
                       seed=1,
                       missing=None)


xlf.fit(train.values, train_label.values, eval_metric=mape_ln, verbose=True, eval_set=[(test.values, test_label.values)],early_stopping_rounds=50)
print (xlf.get_params()) 

#####直接用线下的模型线上预测7月份
import math
import matplotlib.pyplot as plt
from scipy.stats import mode
def AddBaseTimeFeature(df):
    df['time_interval_begin'] = pd.to_datetime(df['time_interval'].map(lambda x: x[1:20]))
    #df = df.drop(['date', 'time_interval'], axis=1)
    df['time_interval_month'] = df['time_interval_begin'].map(lambda x: x.strftime('%m'))
    df['time_interval_day'] = df['time_interval_begin'].map(lambda x: x.day)
    df['time_interval_begin_hour'] = df['time_interval_begin'].map(lambda x: x.strftime('%H'))
    df['time_interval_minutes'] = df['time_interval_begin'].map(lambda x: x.strftime('%M'))
    # Monday=1, Sunday=7
    df['time_interval_week'] = df['time_interval_begin'].map(lambda x: x.weekday() + 1)
    return df
def mode_function(df):
    counts = mode(df)
    return counts[0][0]

sub = feature_data.loc[(feature_data.time_interval_month == 7)&(feature_data.date>='2017-07-01')&((feature_data.time_interval_begin_hour==8)|
                            (feature_data.time_interval_begin_hour==15)|(feature_data.time_interval_begin_hour==18)),: ]
print(len(sub))
for i in [58,56,52,48,38,18,0]:
    tmp = feature_data.loc[(feature_data.time_interval_month <=7)&((feature_data.time_interval_begin_hour==7)|
                            (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                           (feature_data.time_interval_minutes >= i)&(feature_data.date>='2017-01-01'),:]
    tmp1 = tmp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean_%d' % (i), np.mean), ('median_%d' % (i), np.median),('min_%d' % (i),  np.min),
                                ('mode_%d' % (i), mode_function), ('std_%d' % (i), np.std), ('max_%d' % (i), np.max),('sun_%d' % (i), np.sum)]).reset_index()
    
    tmp1['time_interval_begin_hour']=tmp1['time_interval_begin_hour']+1 
    sub = pd.merge(sub,tmp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
sub['last1h']=0.5*sub['median_0']+0.5*sub['mode_0']
sub['last1hsub']=0.5*sub['median_0']-0.5*sub['mode_0']  
del tmp
del tmp1
###2对应当天累积到目前为止前一小时的累积量时间衰减

tmp = feature_data.loc[(feature_data.time_interval_month == 7)&(feature_data.date>='2017-07-01')&((feature_data.time_interval_begin_hour==7)|
                           (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                           (feature_data.time_interval_minutes >= 0),:]

tmp['weights'] = tmp['time_interval_minutes'].map(lambda x: math.exp(-(60-x)))
tmp['weights2'] = tmp['weights']*tmp['travel_time']
tmp2 = tmp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])['weights2'].agg([('exp', np.sum)]).reset_index()
tmp2['time_interval_begin_hour']=tmp2['time_interval_begin_hour']+1 
sub = pd.merge(sub,tmp2,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del tmp
del tmp2


###3每隔十分钟的路程    
    
    
for i in [60,50,40,30,20,10]:
    temp = feature_data.loc[(feature_data.time_interval_month <= 6)&((feature_data.time_interval_begin_hour==8)|
                             (feature_data.time_interval_begin_hour==15)|(feature_data.time_interval_begin_hour==18))&
                          ((feature_data.time_interval_minutes >=(i-10))&(feature_data.time_interval_minutes <=i)),:]    
    temp1 = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean1_%d' % (i), np.mean), ('median1_%d' % (i), np.median),('min1_%d' % (i), np.min),
                                ('mode1_%d' % (i), mode_function), ('std1_%d' % (i), np.std), ('max1_%d' % (i), np.max),('sun1_%d' % (i), np.sum)]).reset_index()    

    sub = pd.merge(sub,temp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp
del temp1

#############################################################
#4后一个小时每个十分钟
for i in [60,50,40,30,20,10]:
    temp = feature_data.loc[(feature_data.time_interval_month <= 6)&((feature_data.time_interval_begin_hour==9)|
                         (feature_data.time_interval_begin_hour==16)|(feature_data.time_interval_begin_hour==19))&
                          ((feature_data.time_interval_minutes <=(i-10))&(feature_data.time_interval_minutes <=i)),:]    
    temp1 = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('meanb_%d' % (i), np.mean), ('medianb_%d' % (i), np.median),('minb_%d' % (i), np.min),
                                ('modeb_%d' % (i), mode_function), ('stdb_%d' % (i), np.std), ('maxb_%d' % (i), np.max),('sunb_%d' % (i), np.sum)]).reset_index()    
    temp1['time_interval_begin_hour']=temp1['time_interval_begin_hour']-1 
    sub = pd.merge(sub,temp1,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp
del temp1

#5month==6,after 7:50 travel_time_medium
temp = feature_data.loc[(feature_data.time_interval_month <= 7)&((feature_data.time_interval_begin_hour==7)|
                         (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                         (feature_data.time_interval_minutes >=50),:] ##665版加上7月份筛选&(feature_data.date>='2017-01-01')         
temp = temp.groupby(['link_ID','time_interval_week','time_interval_begin_hour'])[
            'travel_time'].agg([('mean2', np.mean), ('median2',np.median),('min2',np.min),
                                ('mode2', mode_function), ('std2',np.std),('sun2',np.sum),('max2', np.max)]).reset_index() ##按天数？   
temp['time_interval_begin_hour']=temp['time_interval_begin_hour']+1 
sub = pd.merge(sub,temp,on=['link_ID', 'time_interval_week','time_interval_begin_hour'],how='left')
del temp

#6当天7:50后的最大值
temp = feature_data.loc[(feature_data.time_interval_month == 7)&((feature_data.time_interval_begin_hour==7)|
                         (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                         (feature_data.time_interval_minutes >=50),:] ##665版加上7月份筛选&(feature_data.date>='2017-07-01')
temp = temp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])[
            'travel_time'].agg([('max750', np.max),('mean750', np.mean), ('median750',np.median),('min750',maxmin_function),
                                ('mode750', mode_function), ('std750',np.std),('sun750',np.sum)]).reset_index()    
temp['time_interval_begin_hour']=temp['time_interval_begin_hour']+1 
sub = pd.merge(sub,temp,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del temp 
#####路段上下游衰减
tmp = feature_data.loc[(feature_data.time_interval_month == 7)&(feature_data.date>='2017-07-01')&((feature_data.time_interval_begin_hour==7)|
                     (feature_data.time_interval_begin_hour==14)|(feature_data.time_interval_begin_hour==17))&
                      (feature_data.time_interval_minutes >=0),:]    
tmp['weights'] = tmp['time_interval_minutes'].map(lambda x: math.exp(-(60-x)))
tmp['weights2'] = tmp['weights']*tmp['travel_time']
tmp2 = tmp.groupby(['link_ID','time_interval_day','time_interval_begin_hour'])['weights2'].agg([('exp', np.sum)]).reset_index()

intop=pd.read_csv('lzh_in_top.csv')
intop = pd.merge(intop,tmp2, left_on='in_top',right_on='link_ID',how='inner')
# intop['lengthsub']=intop['in_top_width']/intop['link_id_width']
intop= intop.fillna(0)
temp2= intop.groupby(['link_ID_x','time_interval_day','time_interval_begin_hour'])['exp'].agg([('meanin', np.mean)]).reset_index()    
temp2['time_interval_begin_hour']=temp2['time_interval_begin_hour']+1 
temp2['link_ID']=temp2['link_ID_x']
temp2=temp2.drop(['link_ID_x'],axis=1)
sub = pd.merge(sub,temp2,on=['link_ID', 'time_interval_day','time_interval_begin_hour'],how='left')
del temp2
del tmp
del tmp2
del intop
sub['subexp']=sub['exp']-sub['meanin']
sub['subs1']=sub['max750']-sub['median2']
sub['subs3']=sub['mode750']-sub['mode2']
sub['subs4']=sub['median750']-sub['maxb_20']
sub['subs6']=sub['median1_60']-sub['medianb_20']
#################################################################################

print(len(sub))
sub = sub.fillna(method='ffill')
sub=sub.fillna(0)
# sub=sub[sub.subs1<=40]
# sub=sub.loc[sub.subs1==0.993,:]

sub_label = np.log1p(sub.pop('travel_time'))
sub.to_csv('sub.csv',index=False)


######################################################################################################################
#生成最终结果
sub_index=sub[['link_ID','date','time_interval']]
print(len(sub))
sub.drop(['time_interval_month','time_interval_begin','date', 'time_interval'],inplace=True,axis=1)
print( "Training on: {}".format(sub.shape, sub_label.shape))
result = xlf.predict(sub.values)
sub_index['travel_time']=result
sub_index['travel_time'] = np.round(np.expm1(sub_index['travel_time']),2)
sub_index[['link_ID','date','time_interval','travel_time']].to_csv('sub0913semifinal.txt',sep=';',index=False,header=False)
print (sub_index[['link_ID','date','time_interval','travel_time']].shape)
print (sub_index[['link_ID','date','time_interval','travel_time']].isnull().sum())
#查看特征重要性
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
feature=test.columns
feature = pd.DataFrame({'feature':list(feature)})
feature_importances = pd.DataFrame({'feature_importances':list(xlf.feature_importances_)})
feature_result=pd.concat([feature,feature_importances],axis=1)
feature_result =feature_result.sort_values(['feature_importances'], ascending=False)
feature_result
#其他模型LGB效果不好
import lightgbm as lgb
#print(len(y_train))
traingb=train.drop(['link_ID'],axis=1)
testgb=test.drop(['link_ID'],axis=1)
print("Training on: {}".format(traingb.shape, train_label.shape))
dtrain=lgb.Dataset(traingb,label=train_label)
dtest=lgb.Dataset(testgb, label=test_label)
    
#     param = {'learning_rate' : 0.1, 'n_estimators': 1000, 'max_depth': 3, 
#         'min_child_weight': 5, 'gamma': 0, 'subsample': 1.0, 'colsample_bytree': 0.8,
#         'scale_pos_weight': 1, 'eta': 0.05, 'silent': 1, 'objective': 'binary:logistic'}
#     num_round = 283
#     param['nthread'] = 4
#param['eval_metric'] = "logloss"
RS = 2016
np.random.seed(RS)
vali= [(dtest, 'eval'), (dtrain, 'train')]
ROUNDS = 22
params = {
    'objective':'regression_l1',
    'metric': 'l1',
    'boosting': 'gbdt',
    'learning_rate': 0.03,
    'verbose': 0,
    'num_leaves': 63,
    'bagging_fraction': 0.95,
    'bagging_freq': 1,
    'bagging_seed': RS,
    'feature_fraction': 0.7,
    'feature_fraction_seed': RS,
    'max_bin': 100,
    'num_trees': ROUNDS,
    'min_sum_hessian_in_leaf': 100
    #'valid': valid_sets,
    #'is_unbalance': True
}

print("Started")

print("Training on: {}".format(train.shape, train_label.shape))
train_lgb = lgb.Dataset(traingb, train_label)#, categorical_feature=['brand','cate','sku_id','user_id'])
gbm = lgb.train(params, train_lgb, num_boost_round=ROUNDS,valid_sets=[dtest],early_stopping_rounds=10)
######################################################################################################################
#LGBMpredict
sub2=pd.read_csv('sub.csv')
sub_index2=sub2[['link_ID','date','time_interval']]
print(len(sub2))
sub2.drop(['time_interval_month','time_interval_begin','date', 'time_interval'],inplace=True,axis=1)
print( "Training on: {}".format(sub2.shape, sub2.shape))
subgb=sub2.drop(['link_ID'],axis=1)
result2 = gbm.predict(subgb,num_iteration=gbm.best_iteration)#,num_iteration = gbm.best_iteration   
sub_index2['travel_time']=result2
sub_index2['travel_time']=np.expm1(sub_index2['travel_time'])
sub_index2['travel_time'] = sub_index2['travel_time'].round(1)
sub_index2[['link_ID','date','time_interval','travel_time']].to_csv('sub170911gbm.txt',sep=';',index=False,header=False)
print (sub_index2[['link_ID','date','time_interval','travel_time']].shape)
print (sub_index2[['link_ID','date','time_interval','travel_time']].isnull().sum())