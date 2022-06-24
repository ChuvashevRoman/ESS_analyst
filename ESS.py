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
        df_p['charge'] = df_p['val'].apply(lambda x: -x if x < 0 else 0)
        df_p['discharge'] = df_p['val'].apply(lambda x: x if x > 0 else 0)
        charge = df_p.set_index("time").groupby(pd.Grouper(freq="H")).mean()['charge'].sum()
        discharge = df_p.set_index("time").groupby(pd.Grouper(freq="H")).mean()['discharge'].sum()

        # Расчёт КПД = 1 - (Уровень энергоёмкости на начало интервала + Энергия потребляемая СНЭ) /
        # (Уровень энергоёмкости на конец интервала + Энергия выдаваемая в сеть)
        eff_factor = (ess_e_end + discharge) / \
                     (ess_e_start + charge)

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
        p_save_pvs = []
        for i in range(len(df)):
            if (df['pvs_p'][i] > 0) and (df['ess_p'][i] < 0) and (df['grid_p'][i] > 0):
                p_save_pvs.append(-df['ess_p'][i] - df['grid_p'][i])
            else:
                p_save_pvs.append(0)
        df["save_pvs"] = p_save_pvs
        energy_save = df.groupby(pd.Grouper(freq="H")).mean()['save_pvs'].sum()

        return energy_save

    def charge_number(self, ess_p):
        # Расчёт количества циклов
        ess_p['discharge'] = ess_p['val'].apply(lambda x: x if x > 0 else 0)
        energy_inject = ess_p.set_index("time").groupby(pd.Grouper(freq="H")).mean()['discharge'].sum()
        charge_number = energy_inject / self.e_nom

        return charge_number

    def mean_power(self, ess_p):
        # Расчёт средней мощности
        ess_p['charge'] = ess_p['val'].apply(lambda x: -x if x < 0 else 0)
        ess_p['discharge'] = ess_p['val'].apply(lambda x: x if x > 0 else 0)
        ess_p = ess_p.groupby(pd.Grouper(freq="H")).mean()
        mean_cahrge = ess_p["charge"][ess_p["charge"] > 0].mean()
        mean_discharge = ess_p["discharge"][ess_p["discharge"] > 0].mean()
        mean_power = (mean_cahrge + mean_discharge) / 2

        return mean_power