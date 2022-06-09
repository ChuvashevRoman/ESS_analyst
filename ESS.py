import datetime
import pandas as pd

class ESS_params:

    def __init__(self, p_nom, e_nom):
        self.p_nom = p_nom
        self.e_nom = e_nom

    def eff_factor(self, df_p, df_e):
        # Энергоёмкость на начало и конец периода
        ess_e_start = df_e['val'][0]
        ess_e_end = df_e['val'][len(df_e) - 1]

        # Расчёт энергии заряда разряда
        charge = 0
        discharge = 0
        for item in df_p['val']:
            if item < 0:
                charge += -item
            else:
                discharge += item

        # Расчёт КПД = 1 - (Уровень энергоёмкости на начало интервала + Энергия потребляемая СНЭ) /
        # (Уровень энергоёмкости на конец интервала + Энергия выдаваемая в сеть)
        eff_factor = 1 - (ess_e_end + discharge / 60) / \
                     (ess_e_start + charge / 60)

        return eff_factor

    def save_pvs(self, ess_p, pvs_p, grid_p, p_grid_delta):
        # Создание общего датафрейма
        ess_p.set_index('time', inplace=True)
        ess_p = ess_p.rename(columns={'val': 'ess_p'})
        pvs_p.set_index('time', inplace=True)
        pvs_p = pvs_p.rename(columns={'val': 'pvs_p'})
        grid_p.set_index('time', inplace=True)
        grid_p = grid_p.rename(columns={'val': 'grid_p'})
        df = ess_p.merge(pvs_p, how='inner', on='time')
        df = df.merge(grid_p, how='inner', on='time')

        # Расчёт запасённой энергии солнца
        energy_save = 0
        for i in range(len(df)):
            if df['grid_p'][i] < p_grid_delta and df['pvs_p'][i] > 0:
                energy_save -= df['ess_p'][i] / 60

        return energy_save

    def charge_number(self, ess_p):
        # Расчёт количества циклов
        energy_inject = 0

        for item in ess_p.val:
            if item > 0:
                energy_inject += item / 60

        charge_number = energy_inject / self.e_nom

        return charge_number


