#%%
import pandas as pd
import numpy as np
import os
import xlrd
from itertools import combinations
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
from matplotlib.ticker import MaxNLocator

os.chdir(os.path.dirname(os.path.realpath(__file__)))

#%% 중국 미세먼지 데이터
# %%
def int_to_strdate(x): #날짜 + 시간 따로 기록된 것 합치기 (중국용)
    date = str(x[0])
    if x[1]<10:
        time='0'+str(x[1])
    else:
        time=str(x[1])
    return( date+time)
def change_date(x): #날짜 + 시간 +01~24단위에서 00~23단위 변환 (한국용)
    date = str(x[0])
    hour = str(x[1])
    Date = date+hour
    if hour=='00':
        Date = pd.to_datetime(Date,format='%Y%m%d%H')+datetime.timedelta(days=1)
    else:
        Date = pd.to_datetime(Date,format='%Y%m%d%H')
    return(Date)    
#%%
#%% 중국 지역명 변환
import unidecode
CHINA_ = pd.read_excel('./DATA/중국 미세먼지_대기질2015~2018_시간단위.xlsx',usecols="A:NF")
loc_in_CN = CHINA_.columns[3:]
from googletrans import Translator
trans = Translator()
tt=trans.translate(list(loc_in_CN),src='zh-CN',dest='en')
loc_in_EN = []
loc_in_EN2 = []
for transl in tt:
    loc_in_EN=loc_in_EN+[transl.text]
    loc_in_EN2=loc_in_EN2+[transl.extra_data['translation'][1][3]]

CN_EN_loc=pd.DataFrame({'EN':loc_in_EN,'CN':loc_in_CN,'EN2':loc_in_EN2})
CN_EN_loc['EN3']=CN_EN_loc.EN2.apply(lambda x: unidecode.unidecode(x))
CN_EN_loc.EN3[CN_EN_loc.EN3.duplicated(keep='first')]=CN_EN_loc.EN3[CN_EN_loc.EN3.duplicated(keep='first')].apply(lambda x : x+'2')

# CN_EN_loc.to_hdf('./DATA/CN_EN_loc.hdf',key='loc')
CN_EN_loc = pd.read_hdf('./DATA/CN_EN_loc.hdf',)

#%% 중국 시간별 csv 파일 저장
CHINA_.columns = ['date','hour','type']+CN_EN_loc.EN3.to_list()+['Date']
CHINA_['Date']=pd.to_datetime(CHINA_[['date','hour']].apply(int_to_strdate, axis=1),format='%Y%m%d%H')
CHINA_['type']=CHINA_.type.astype("category")

for loc in tqdm(CN_EN_loc.EN3):
    CN_temp = CHINA_[['Date','type',loc]].drop_duplicates(['Date','type'],keep='first').pivot(index='Date',columns='type',values=loc)
    CN_temp.columns=['CO','NO2','O3','PM10','PM2.5','SO2']
    CN_temp['loc']=loc
    CN_temp=CN_temp.reset_index()
    CN_temp.to_csv('./DATA/CN_air_EN3/'+loc+'.csv')

for loc in tqdm(CN_EN_loc.EN3):
    CN_temp = pd.read_csv('./DATA/CN_air_EN3/'+loc+'.csv',encoding='cp949',index_col='Date',parse_dates=True)
    CN_temp = CN_temp.iloc[:,1:]
    temp=CN_temp.resample('D').mean()
    print(sum(temp.index.duplicated()))
    temp.to_hdf('./DATA/CN_air.h5',key=loc,mode='a')
#%% 15개 도시 대기
#대기정보 지역 구분
KO_loc = ['강릉', '광주', '군산', '대구', '대전', '부산', '서울', '수원', '안동', '울산', '인천', '제주', '진주', '청주', '춘천']

for loc in tqdm(KO_loc):
    KO_temp = pd.read_excel('./DATA/15개 도시대기_2008~2018.xlsx',sheet_name=loc)
    KO_temp = KO_temp.iloc[:,0:10]
    KO_temp = KO_temp[KO_temp.측정일시.notna()]
    KO_temp['측정일시']=KO_temp['측정일시'].astype(str).str.replace('-','').str.replace(' ','')
    KO_temp['hour']=KO_temp.측정일시.apply(lambda x: x[8:10]).apply(lambda x: '00' if x=='24' else x)
    KO_temp['date']=KO_temp.측정일시.apply(lambda x: x[:8])
    KO_temp['Date']=KO_temp[['date','hour']].apply(change_date, axis=1)
    KO_temp['지역']=loc
    KO_temp['지역']=KO_temp['지역'].astype('category')
    #KO_temp['지역']=KO_temp['지역'].astype('category')
    KO_temp['측정소명']=KO_temp['측정소명'].astype('category')
    KO_temp['측정소코드']=KO_temp['측정소코드'].astype('category')
    KO_temp=KO_temp.drop_duplicates(['측정소명','측정소코드','Date'],keep='first')
    KO_temp=KO_temp[['Date','지역','측정소명','CO','NO2','SO2','O3','PM2.5','PM10']]
    KO_temp=KO_temp.reset_index()
    KO_temp.to_csv('./DATA/KO_air/'+loc+'.csv',encoding='cp949')

for loc in KO_loc:
    KO_temp = pd.read_csv('./DATA/KO_air/'+loc+'.csv',encoding='cp949',index_col='Date',parse_dates=True)
    KO_temp=KO_temp[['CO','NO2','SO2','O3','PM2.5','PM10']]
    temp=KO_temp.resample('D').mean()
    temp['loc'] = loc
    temp=temp[temp.index>='2015']
    print(loc)
    temp.to_hdf('./DustDATA/KO_air.h5',key=loc,mode='a')
#%% 15개 도시 기상

KO_loc = ['강릉', '광주', '군산', '대구', '대전', '부산', '서울', '수원', '안동', '울산', '인천', '제주', '진주', '청주', '춘천']

for loc in tqdm(KO_loc):
    KO_temp = pd.read_excel('./DATA/15개 도시기상자료_2008~2019.xlsx',sheet_name=loc,usecols="A:R")
    KO_temp.columns=['지점','Date','Avg_T','Min_T','Min_T_h','Max_T','Max_T_h','Rain','Max_WS','Max_WS_d','Max_WS_h','Avg_WS','Avg_Dew','Min_Hum','Min_Hum_h','Avg_Hum','Avg_Pr','Yel_Wind_']
    KO_temp['Yel_Wind'] = KO_temp['Yel_Wind_'].apply(lambda x: True if x=='O' else float('nan'))
    KO_temp['지역']=loc
    KO_temp['지역']=KO_temp['지역'].astype('category')
    KO_temp=KO_temp.dropna(subset=['Date'])
    KO_temp = KO_temp.drop_duplicates(['Date'])
    KO_temp = KO_temp.drop(['지점','Yel_Wind_'],axis=1)
    KO_temp.to_csv('./DATA/KO_weather/'+loc+'.csv',encoding='cp949')

for loc in KO_loc:
    KO_temp = pd.read_csv('./DATA/KO_weather/'+loc+'.csv',encoding='cp949',index_col='Date',parse_dates=True)
    KO_temp = KO_temp.iloc[:,1:]
    KO_temp.rename(columns = {'지역': 'loc'})
    temp = KO_temp[KO_temp.index>='2015']
    print(loc)
    temp.to_hdf('./DATA/KO_air.h5',key=loc,mode='a')

#%% 대기환경연구소_XRF
KO_loc = ['백령도', '수도권', '중부권', '호남권', '영남권', '제주도']

for loc in tqdm(KO_loc):
    KO_temp = pd.read_excel('./DATA/대기환경연구소_XRF_2012~2018_일평균.xlsx', sheet_name = loc,skiprows=1,header=1)
    KO_temp = KO_temp.drop_duplicates(['Date'])
    KO_temp['지역']=loc
    KO_temp.to_csv('./DATA/KO_xrf/'+loc+'.csv',encoding = 'cp949')

for loc in KO_loc:
    KO_temp = pd.read_csv('E:/DustDATA/KO_xrf/'+loc+'.csv',encoding='cp949',index_col='Date',parse_dates=True)
    KO_temp=KO_temp.iloc[:,1:]
    KO_temp.rename(columns={'지역':'loc'})
    temp = KO_temp[KO_temp.index>='2015']
    print(loc)
    print(sum(temp.index.duplicated()))
    temp.to_hdf('./DATA/KO_xrf.h5',key=loc,mode='a')


# %%


#%%
import xlrd
KOREA_ = xlrd.open_workbook('DATA/15개 도시대기_2008~2018.xlsx', on_demand=True)
KOREA_ = xlrd.open_workbook('./DATA/15개 도시기상자료_2008~2019.xlsx', on_demand=True)
print(KOREA_.sheet_names())
KO_temp = pd.read_excel('./DATA/15개 도시기상자료_2008~2019.xlsx',sheet_name='광주')
KO_temp.iloc[:,:18]
KO_temp = 

temp.['지역']=KO_loc
temp.['지역']=KO_temp['지역'].astype('category')
KO_temp['황사'][temp['황사 발생 여부'].notna()]

#기상정보 지역 구분
for loc in tqdm(KO_loc):
    KO_temp = pd.read_excel('./DATA/15개 도시기상자료_2008~2019.xlsx',sheet_name=loc)
    KO_temp = KO_temp.iloc[:,:18]
    KO_temp['지역']=loc
    KO_temp['지역']=KO_temp['지역'].astype('category')
    KO_temp['황사'] = KO_temp.iloc[:,17].apply(lambda x: True if x=='O' else x)
    KO_temp=KO_temp.dropna(subset=['일시'])
    KO_temp = KO_temp.drop_duplicates(['일시'])
    KO_temp.columns=['지점','Date','Avg_Temp','Min_Temp','Min_Temp_Hour','Max_Temp','Max_Temp_Hour','Rain','Wind_Speed','Wind_Direct','Wind_Hour','Avg_Wind','Avg_Dew','Min_Hum','Min_Hum_Hour','Avg_Hum','Avg_Pressure','Yellow_Wind_','지역','Yellow_Wind']
    KO_temp=KO_temp[['Date','지역','Avg_Temp','Min_Temp','Min_Temp_Hour','Max_Temp','Max_Temp_Hour','Rain','Wind_Speed','Wind_Direct','Wind_Hour','Avg_Wind','Avg_Dew','Min_Hum','Min_Hum_Hour','Avg_Hum','Avg_Pressure','Yellow_Wind']]
    KO_temp.to_csv('./DATA/KO_weather/'+loc+'.csv',encoding='utf-8-sig')



KO_loc = ['강릉', '광주', '군산', '대구', '대전', '부산', '서울', '수원', '안동', '울산', '인천', '제주', '진주', '청주', '춘천']
temp= pd.read_excel('E:/DustDATA/15개 도시대기_2008~2018.xlsx',sheet_name='강릉')
temp
temp=temp.iloc[:,0:10]