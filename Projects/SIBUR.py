import yaml
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

# Выгрузка данных
ess_df_e = Amigo.get_data('energyStoragingUnit', ESS_1['mrid'], 'e', 'TM1M', 'PT-24H', 'PT-0M')
ess_df_p = Amigo.get_data('energyStoragingUnit', ESS_1['mrid'], 'p', 'TM1M', 'PT-24H', 'PT-0M')
pvs_p = Amigo.get_data('generatingUnit', PVS_1['mrid'], 'p', 'TM1M', 'PT-24H', 'PT-0M')
grid_p = Amigo.get_data('externalGrid', Grid_1['mrid'], 'p', 'TM1M', 'PT-24H', 'PT-0M')

# Расчёт КПД
eff_factor = ESS_params.eff_factor(ess_df_p, ess_df_e)

# Расчёт количества циклов
charge_number = ESS_params.charge_number(ess_df_p)

# Расчёт запасённой энергии СЭС
energy_safe = ESS_params.save_pvs(ess_df_p, pvs_p, grid_p, Grid_1['p_grid_delta'])

# Загрузка данных в Амиго
Amigo.post_data('energyStoragingUnit', ESS_1['mrid'], 'status', 'TM1D', eff_factor)
Amigo.post_data('energyStoragingUnit', ESS_1['mrid'], 'chargeNumber', 'DT1D', charge_number)
Amigo.post_data('generatingUnit', PVS_1['mrid'], 'e', 'AB1D', energy_safe)


