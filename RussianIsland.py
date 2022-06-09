import requests
import pandas as pd
#import matplotlib.pyplot as plt
#import matplotlib as matplotlib
import numpy as np
#import matplotlib.dates as mdates
import datetime as datetime
import json

# функция расчёта параметров накопителя
def eff_ess_day(ip,mrid,start_time,Pnom,kpd_last):
    p_nom=Pnom
    kpd_last_day=0

    #Функция выгрузки данных по API
    def json_to_dataframe(url,usr,psswrd):
        req=requests.get(url,auth=(usr, psswrd))
        val=[]
        time=[]
        for item in req.json():
            val.append(float(item['value']))
            time.append(pd.Timestamp(item['timeStamp']))
        df=pd.DataFrame({'time':time,'val':val})
        return(df)

    #загрузка предыдущего значения КПД
    if kpd_last==True:
        kpd_last_day = json_to_dataframe('http://'+ip+'/api/energyStoragingUnit/'+mrid+'/status/row?purposeKey=TM1D&start=P-3D', 'amigo', 'm&ms7_')
        kpd_last_day = kpd_last_day.val.mean()
        if kpd_last_day!=kpd_last_day: kpd_last_day=0

    #создание датафреймов
    df_p = json_to_dataframe('http://'+ip+'/api/energyStoragingUnit/'+mrid+'/p/row?purposeKey=TM1M&start='+start_time, 'amigo', 'm&ms7_')
    df_cap = json_to_dataframe('http://'+ip+'/api/energyStoragingUnit/'+mrid+'/e/row?purposeKey=TM1M&start='+start_time, 'amigo', 'm&ms7_')

    #условие чтоб были данные в датафреймах
    if len(df_p.val)>0 and len(df_cap.val)>0:
        #обрезка датафрейма
        df_p_1=df_p[(df_p.time >= (df_p.time[0]+pd.Timedelta('0 days 00:'+str(60-df_p.time[0].minute)+':00'))) & (df_p.time <= (df_p.time[len(df_p.time)-1]-pd.Timedelta('0 days 00:'+str(df_p.time[len(df_p.time)-1].minute+1)+':00')))]
        df_cap_1=df_cap[(df_cap.time >= (df_cap.time[0]+pd.Timedelta('0 days 00:'+str(60-df_cap.time[0].minute)+':00'))) & (df_cap.time <= (df_cap.time[len(df_cap.time)-1]-pd.Timedelta('0 days 00:'+str(df_cap.time[len(df_cap.time)-1].minute)+':00')))]  
        flag1=True

        #расчёт мощности заряда разряда
        df_p_1['charge']=df_p_1.val.apply(lambda x: -x if x < 0 else 0)
        df_p_1['discharge']=df_p_1.val.apply(lambda x: x if x > 0 else 0)

        #агрегация данных по часам
        df_p_1['hour']=df_p_1.time.apply(lambda x: x.hour)
        df_p_1['date']=df_p_1.time.apply(lambda x: x.date)
        df_cap_1['hour']=df_cap_1.time.apply(lambda x: x.hour)
        df_cap_1['date']=df_cap_1.time.apply(lambda x: x.date)
        df_p_2=df_p_1.groupby(['date','hour']).mean().reset_index()
        df_cap_2=df_cap_1.groupby(['date','hour']).first().reset_index()

        #удаление неполных значений
        for item in df_p_2.hour:
            if len(df_p_1[(df_p_1.hour == item)]) < 50:
                df_p_2.drop(df_p_2[(df_p_2.hour==item)].index)
        for item in df_cap_2.hour[0:-1]:
            if len(df_cap_1[(df_cap_1.hour == item)]) < 50:
                df_cap_2.drop(df_cap_2[(df_cap_2.hour==item)].index)

        #общий датафрейм
        df_p_2 = df_p_2.rename(columns={'val': 'p'})
        df_cap_2 = df_cap_2.rename(columns={'val': 'e'})
        df=df_cap_2.merge(df_p_2,how='left',on=['hour','date'])

        #расчёт КПД
        eff=[]
        for item in df.index[0:-1]:
            kpd=(1+(-df.charge[item]+df.discharge[item]+df.e[item+1]-df.e[item])/(df.charge[item]+df.e[item]))
            if kpd<1: eff.append(kpd)
            elif kpd!=kpd: eff.append(KPD_last_day)
            else: eff.append(1)
        df['eff']=pd.Series(eff)
        if len(df.eff[(df.p.abs() > p_nom/100)])>0:
            KPD = df.eff[(df.p.abs() > p_nom/100)].mean()*100
        else:
            KPD=df.eff.mean()*100

        #среднее значение мощности накопителя
        if df[(df.charge > p_nom/100)].mean().charge != df[(df.charge > p_nom/100)].mean().charge:
            p_charge_mean = 0
        else: p_charge_mean = df[(df.charge > p_nom/100)].mean().charge
        if df[(df.discharge > p_nom/100)].mean().discharge != df[(df.discharge > p_nom/100)].mean().discharge:
            p_discharge_mean = 0
        else: p_discharge_mean = df[(df.discharge > p_nom/100)].mean().discharge
        if df[(df.p.abs() > p_nom/100)].mean().abs().p != df[(df.p.abs() > p_nom/100)].mean().abs().p:
            p_abs_mean = 0
        else: p_abs_mean = df[(df.p.abs() > p_nom/100)].mean().abs().p

        print(df)
        if KPD!=KPD: KPD=kpd_last_day
        return(KPD, p_charge_mean, p_discharge_mean, p_abs_mean)

    #если данных нет
    else:
        KPD=kpd_last_day
        p_charge_mean=p_discharge_mean=p_abs_mean=0
    return(KPD, p_charge_mean, p_discharge_mean, p_abs_mean)

# передача данных по API
def post_api(val,ip,mrid,telpoint,purposeKey):
    req=requests.post('http://'+ip+'/api/values',json={"path":"/energyStoragingUnit[MRID=\""+mrid+"\"]/"+telpoint,"purposeKey":purposeKey,"value":val})
    print(req)
    print(req.request.body)

ip='127.0.0.1'
mrid_ess_1='_ESS_01'
mrid_ess_2='_ESS_02'
start_time='PT-25H'
Pnom_ess_1=18
Pnom_ess_2=30
kpd_last=True
KPD_ess_1, P_CH_1, P_DCH_1, P_ABS_1 = eff_ess_day(ip,mrid_ess_1,start_time,Pnom_ess_1,kpd_last)
KPD_ess_2, P_CH_2, P_DCH_2, P_ABS_2 = eff_ess_day(ip,mrid_ess_2,start_time,Pnom_ess_2,kpd_last)

print(KPD_ess_1,'   ',KPD_ess_2)

# передача расчётных значений КПД
telpoint='status'
purposeKey='TM1D'
post_api(KPD_ess_1,ip,mrid_ess_1,telpoint,purposeKey)
post_api(KPD_ess_2,ip,mrid_ess_2,telpoint,purposeKey)

# передача расчётных значений мощностей
telpoint='p'
purposeKey='AV1D'
post_api(P_CH_1,ip,mrid_ess_1,telpoint,purposeKey)
post_api(P_ABS_2,ip,mrid_ess_2,telpoint,purposeKey)
