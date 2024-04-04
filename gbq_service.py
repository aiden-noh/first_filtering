from datetime import datetime
import numpy as np
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# 연간
def _convert(crawring_df: pd.DataFrame,
             stock_code: str,
             corp_name: str,
             type: str) -> pd.DataFrame:

    df = crawring_df.copy()

    # 순서 저장 후 짧은 구조 변환
    df['order'] = df.index.to_list()
    df = df.melt(['order', 'account_nm'])

    # 추가 데이터 세팅
    df['stock_code'] = stock_code
    df['corp_name'] = corp_name
    df['type'] = type

    # 연, 분기 데이터 세팅
    df.columns = ['order', 'account_name', 'date', 'amount', 'stock_code', 'corp_name', 'type']
    df['year'] = df.date.str.split('/').str[0].astype(int)
    df['month'] = df.date.str.split('/').str[1].astype(int)

    # 컬럼 정리
    df = df[['stock_code', 'corp_name', 'year', 'month', 'type', 'order', 'account_name', 'amount']]

    # id 생성
    df.index = df.stock_code + '_' + df.year.astype(str) + '_' + df.month.astype(str) + '_' + df.type + '_' + df.order.astype(str).str.zfill(3)
    df.index.name = 'id'

    return df

# 분기
def _convert_q(crawring_df: pd.DataFrame,
               stock_code: str,
               corp_name: str,
               type: str) -> pd.DataFrame:

    df = crawring_df.copy()

    # 순서 저장 후 짧은 구조 변환
    df['order'] = df.index.to_list()
    df = df.melt(['order', 'account_nm'])

    # 추가 데이터 세팅
    df['stock_code'] = stock_code
    df['corp_name'] = corp_name
    df['type'] = type

    # 연, 분기 데이터 세팅
    df.columns = ['order', 'account_name', 'date', 'amount', 'stock_code', 'corp_name', 'type']
    df['year'] = df.date.str.split('/').str[0].astype(int)
    df['month'] = df.date.str.split('/').str[1].astype(int)
    df['quarter'] = np.ceil(df.date.str.split('/').str[1].astype(int) / 3).astype(int)

    # 컬럼 정리
    df = df[['stock_code', 'corp_name', 'year', 'month', 'quarter', 'type', 'order', 'account_name', 'amount']]

    # id 생성
    df.index = df.stock_code + '_' + df.year.astype(str) + '_' + df.month.astype(str) + '_' + df.quarter.astype(str) + '_' + df.type + '_' + df.order.astype(str).str.zfill(3)
    df.index.name = 'id'

    return df

# def _clean_exist(df: pd.DataFrame):
#     delete_sql = f"""
#         DELETE FROM `hello-phase3.naver_fn_data.financial_statements_year`
#         WHERE id IN {str(tuple(df.index.tolist()))}
#     """

#     # SQL 쿼리 실행
#     job = client.query(delete_sql)
#     print(job.result())

def _upload(df: pd.DataFrame, table_name: str):
    table_id = f'hello-phase3.naver_fn_data.{table_name}'

    # job_config = bigquery.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("stock_code", bigquery.enums.SqlTypeNames.STRING),
        #     bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        #     bigquery.SchemaField("account_name", bigquery.enums.SqlTypeNames.STRING),
        # ]
    # )

    df['updated_at'] = datetime.now()
    job = client.load_table_from_dataframe(df, table_id)  # Make an API request.
    print(job.result(), '\nUploaded: ', len(df))  # Wait for the job to complete.

#########################################

def get_krx_list():
    query = """
        SELECT *
        FROM `hello-phase3.naver_fn_data.krx_list`
        WHERE updated_date = (SELECT MAX(updated_date) FROM `hello-phase3.naver_fn_data.krx_list`)
    """
    df = client.query(query).to_dataframe()
    return df

def convert_gbq_year(crawring_df: pd.DataFrame,
                     stock_code: str,
                     corp_name: str,
                     type: str) -> pd.DataFrame:

    df = _convert(crawring_df, stock_code, corp_name, type)
    return df

def convert_gbq_quarter(crawring_df: pd.DataFrame,
                        stock_code: str,
                        corp_name: str,
                        type: str) -> pd.DataFrame:

    df = _convert_q(crawring_df, stock_code, corp_name, type)
    return df

# 연간 재무제표
def load_to_fs_year(df: pd.DataFrame):
    _upload(df, 'financial_statements_year')

# 분기 재무제표
def load_to_fs_quarter(df: pd.DataFrame):
    _upload(df, 'financial_statements_quarter')

# 연간 투자지표
def load_to_iv_year(df: pd.DataFrame):
    _upload(df, 'investment_index_year')

# 분기 투자지표
def load_to_iv_quarter(df: pd.DataFrame):
    _upload(df, 'investment_index_quarter')

# def load_to_stock_info(df: pd.DataFrame):
#     _upload(df, 'stock_info')
