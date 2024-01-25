import re
import time
import json
import requests
import pandas as pd

# type: 0-재무제표, 1-투자지표
# frq: 0-연간, 1-분기
# rpt: 재무제표 일 때 0-포괄손익계산서, 1-재무상태표, 2-현금흐름표
# rpt: 투자지표 일 때 1-수익성, 2-성장성, 3-안정성, 4-활동성, 5-가치지표

from enum import Enum
class Period(Enum):
    YEAR = 0 # 연간
    QUARTER = 1 # 분기

class DataType(Enum):
    FS = 0 # 재무제표
    IV = 1 # 투자지표

class FSType(Enum):
    IS = 0 # 손익계산서
    BS = 1 # 재무상태표
    CF = 2 # 현금흐름표

class IVType(Enum):
    PROFIT = 1 # 수익성
    GROWTH = 2 # 성장성
    STABILITY = 3 # 안정성
    ACTIVITY = 4 # 활동성
    VALUEABLE = 5 # 가치지표

def _get_nfn_page_name(type: DataType) -> str:
    return 'c1030001' if type == DataType.FS else 'c1040001'

def _get_nfn_api_name(type: DataType) -> str:
    return 'cF3002' if type == DataType.FS else 'cF4002'

def _get_encparam_url(stock_code: str, type: DataType) -> str:
    page_nm = _get_nfn_page_name(type)
    return f'https://navercomp.wisereport.co.kr/v2/company/{page_nm}.aspx?cmp_cd={stock_code}'

def _get_api_url(stock_code: str, type: DataType, subType: FSType|IVType, period: Period) -> str:
    api_nm = _get_nfn_api_name(type)
    frq = period.value
    rpt = subType.value
    return f'https://navercomp.wisereport.co.kr/v2/company/{api_nm}.aspx?cmp_cd={stock_code}&frq={frq}&rpt={rpt}&finGubun=MAIN&frqTyp={frq}&cn='

# def _get_iv_api_url(stock_code, ivType: IVType, period: Period) -> str:
#     api_nm = _get_nfn_api_name(DataType.IV)
#     frq = period.value
#     rpt = ivType.value
#     return f'https://navercomp.wisereport.co.kr/v2/company/{api_nm}.aspx?cmp_cd={stock_code}&frq={frq}&rpt={rpt}&finGubun=MAIN&frqTyp={frq}&cn='

def _get_headers(stock_code, type: DataType) -> dict:
    referer = _get_encparam_url(stock_code, type)
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Host': 'navercomp.wisereport.co.kr',
        'Referer': referer,
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }
    return headers

def _get_encparam(stock_code, type: DataType) -> str:
    url = _get_encparam_url(stock_code, type)
    headers = _get_headers(stock_code, type)

    res = requests.get(url, headers=headers)

    pattern = r"encparam:\s*'([^']*)'"
    match = re.search(pattern, res.text)

    if match:
        extracted_string = match.group(1)
        return extracted_string
    else:
        print("매치되는 문자열을 찾을 수 없습니다.")
        return ''

def crawl_nfn(stock_code, type: DataType, subType: FSType|IVType, period: Period) -> pd.DataFrame:
    headers = _get_headers(stock_code, type)
    encparam = _get_encparam(stock_code, type)
    url = f'{_get_api_url(stock_code, type, subType, period)}&encparam={encparam}'
    print(url)

    res = requests.get(url, headers=headers)
    json_data = res.json()

    extract_columns = ['ACC_NM'] + [f'DATA{num}' for num in range(1, 6)]
    renamed_columns = ['account_nm'] + list(map(lambda s: s.split('<br />')[0], json_data['YYMM']))[:5]

    df = pd.DataFrame.from_dict(json_data['DATA'])
    # df = df[df['GRP_TYP'] == 2]
    df = df[extract_columns]
    df.columns = renamed_columns

    time.sleep(0.5)

    return df

def crawl_nfn_fs(stock_code, type=0, frq=0):
    # type: 0-재무제표, 1-투자지표
    # frq: 0-연간, 1-분기
    # rpt: 재무제표 일 때 0-포괄손익계산서, 1-재무상태표, 2-현금흐름표
    # rpt: 투자지표 일 때 1-수익성, 2-성장성, 3-안정성, 4-활동성, 5-가치지표
    page_nm = 'c1030001' if type == 0 else 'c1040001'
    api_nm = 'cF3002' if type == 0 else 'cF4002'
    rpt_types = [0, 1, 2] if type == 0 else [1, 3, 5]

    encparam = _get_encparam(stock_code, type=type)
    headers = _get_headers(stock_code)

    time.sleep(1)

    df_list = []

    for rpt in rpt_types:
        url = f'https://navercomp.wisereport.co.kr/v2/company/{api_nm}.aspx?cmp_cd={stock_code}&frq={frq}&rpt={rpt}&finGubun=MAIN&frqTyp={frq}&cn=&encparam={encparam}'
        print(url)

        res = requests.get(url, headers=headers)
        json_data = res.json()

        extract_columns = ['ACC_NM'] + [f'DATA{num}' for num in range(1, 6)]
        renamed_columns = ['account_nm'] + list(map(lambda s: s.split('<br />')[0], json_data['YYMM']))[:5]

        df = pd.DataFrame.from_dict(json_data['DATA'])
        # df = df[df['GRP_TYP'] == 2]
        df = df[extract_columns]
        df.columns = renamed_columns
        df_list.append(df)

        time.sleep(1)

    return pd.concat(df_list).reset_index(drop=True)
