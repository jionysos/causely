import pandas as pd
import numpy as np
from tqdm import tqdm

def woe_iv(data, target, bins=10):
    """
    [Params]
    data: pd.DataFrame, Input data. 사전 결측 처리 및 데이터 유효성 정의가 완료되어야 함.
    target: pd.Series, 숫자 (0 또는 1)
    bins: int scalar, 연속형 변수를 몇 개 구간으로 자를 것인지.. 백분위수를 기준으로 자름
    
    [Returns]
    WoE: pd.DataFrame, 변수의 값별로 구해진 WoE(단위:%)
    IV: pd.DataFrame, 변수별로 구해진 IV
    """
    
    var_list = data.columns
    IV = pd.DataFrame()
    WoE = pd.DataFrame()
    
    for var in tqdm(var_list):
        # unique 개수가 bins를 초과하는 숫자형 변수에 대해 bins만큼 구간 나눔 - 백분위수 기준
        if (data[var].dtype.kind in 'bifc') and (len(np.unique(data[var])) > bins):
            binned_x = pd.qcut(data[var], bins, duplicates='drop')
            tmp_x = binned_x.apply(str)
            tmp = pd.DataFrame({'Var_name': var, 'x': tmp_x, 'y': target})
        # unique 개수가 bins 이하 또는 문자형 변수들은 문자성 변환 후 모두 사용
        else:
            tmp_x = data[var].apply(str)
            tmp = pd.DataFrame({'Var_name': var, 'x': tmp_x, 'y': target})
            
        # 결측은 따로 grouping 되도록 하여 구간별 전체 데이터 개수 및 event 개수 구함
        tmp_woe = tmp.groupby(['Var_name', 'x'], dropna=False).agg({'y': ['count', 'sum']}).reset_index()
        tmp_woe.columns = ['Var_name', 'Cut_off', 'N', 'Events']
        
        tmp_woe['Non_Events'] = tmp_woe['N'] - tmp_woe['Events']
        
        # Events 혹은 Non-Events의 값이 0인 경우를 대비해 0.5 더해줌
        tmp_woe['PCT_of_E'] = (tmp_woe['Events'] + 0.5) * 100 / (tmp_woe['Events'] + 0.5).sum()
        tmp_woe['PCT_of_NE'] = (tmp_woe['Non_Events'] + 0.5) * 100 / (tmp_woe['Non_Events'] + 0.5).sum()
        
        tmp_woe['WoE'] = np.log(tmp_woe['PCT_of_E'] / tmp_woe['PCT_of_NE'])
        tmp_woe['IV'] = (tmp_woe['PCT_of_E'] - tmp_woe['PCT_of_NE']) * tmp_woe['WoE']
        
        tmp_iv = pd.DataFrame({'Var_name': [var], 'IV': [tmp_woe['IV'].sum()]})
        
        WoE = pd.concat([WoE, tmp_woe], axis=0, ignore_index=True)
        IV = pd.concat([IV, tmp_iv], axis=0, ignore_index=True)
        
    return WoE, IV