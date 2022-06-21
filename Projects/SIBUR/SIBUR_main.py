
import datetime
import yaml
import time

from Server import Server
from ESS import ESS_params

with open('params.yml') as f:
    params = yaml.safe_load(f)

# Параметры сервера
ip = params['Amigo_web_server']['ip_address']
port = params['Amigo_web_server']['port']

Amigo = Server(ip, port)

# Параметры оборудования
ESS_1 = params['ESS_s']['ESS_1']
ESS_params = ESS_params(ESS_1['p_nom'], ESS_1['e_nom'])
PVS_1 = params['PVS_s']['PVS_1']
Grid_1 = params['Grid_s']['Grid_1']


def update_data():
    now = datetime.datetime.now()
    if now.minute == 0:
        print(">>> Расчёт запасённой энергии СЭС")
        try:
            ess_df_p = Amigo.get_data('energyStoragingUnit', ESS_1['mrid'], 'p', 'TM1M', 'PT-61M', 'now')
            pvs_p = Amigo.get_data('generatingUnit', PVS_1['mrid'], 'p', 'TM1M', 'PT-61M', 'now')
            grid_p = Amigo.get_data('externalGrid', Grid_1['mrid'], 'p', 'TM1M', 'PT-61M', 'now')
        except:
            print(">>> Данные не выгружены")

        # Расчёт запасённой энергии СЭС
        try:
            energy_safe = ESS_params.save_pvs(ess_df_p, pvs_p, grid_p, Grid_1['p_grid_delta'])
            print(f">>> Запасённая энергия СЭС: {energy_safe}")
        except:
            print(">>> Расчёт запасённой энергии не произведён, данные не корректные")

        # Загрузка данных в Амиго
        try:
            timeStamp = datetime.datetime(now.year, now.month, now.day, now.hour - 1)
            Amigo.post_data_with_time('generatingUnit', PVS_1['mrid'], 'e', 'AB1H', energy_safe, timeStamp)
        except:
            print(">>> Данные не отправлены")

        if now.hour == 0:
            print(">>> Расчёт КПД и количества циклов")
            try:
                ess_df_p = Amigo.get_data('energyStoragingUnit', ESS_1['mrid'], 'p', 'TM1M', 'P-1D', 'now')
                ess_df_e = Amigo.get_data('energyStoragingUnit', ESS_1['mrid'], 'e', 'TM1M', 'P-1D', 'now')
            except:
                print(">>> Данные не выгружены")

            # Расчёт количества циклов
            try:
                charge_number = ESS_params.charge_number(ess_df_p)
                print(f">>> Количество циклов: {charge_number}")
            except:
                print(">>> Расчёт количества циклов не произведён, данные не корректные")

            # Расчёт КПД
            try:
                eff_factor = ESS_params.eff_factor(ess_df_p, ess_df_e)
                print(f">>> КПД Накопителя: {eff_factor}")
            except:
                print(">>> Расчёт КПД не произведён, данные не корректные")

            # Загрузка данных в Амиго
            try:
                timeStamp = datetime.datetime(now.year, now.month, now.day - 1)
                Amigo.post_data_with_time('energyStoragingUnit', ESS_1['mrid'], 'chargeNumber', 'DT1D', charge_number, timeStamp)
                Amigo.post_data_with_time('energyStoragingUnit', ESS_1['mrid'], 'status', 'TM1D', eff_factor, timeStamp)
            except:
                print("Данные не отправлены")
    time.sleep(60)


if __name__ == "__main__":
    print(">>> Программа расчёта показателей СНЭЭ запущена")

    while True:
        update_data()
