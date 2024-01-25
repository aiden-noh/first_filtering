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
    df['updated'] = datetime.now()

    # 연, 분기 데이터 세팅
    df.columns = ['order', 'account_name', 'date', 'amount', 'stock_code', 'corp_name', 'type', 'updated']
    df['year'] = df.date.str.split('/').str[0].astype(int)

    # 컬럼 정리
    df = df[['stock_code', 'corp_name', 'year', 'type', 'order', 'account_name', 'amount', 'updated']]

    # id 생성
    df.index = df.stock_code + '_' + df.year.astype(str) + '_' + df.type + '_' + df.order.astype(str).str.zfill(3)
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
    df['updated'] = datetime.now()

    # 연, 분기 데이터 세팅
    df.columns = ['order', 'account_name', 'date', 'amount', 'stock_code', 'corp_name', 'type', 'updated']
    df['year'] = df.date.str.split('/').str[0].astype(int)
    df['quarter'] = np.ceil(df.date.str.split('/').str[1].astype(int) / 3).astype(int)

    # 컬럼 정리
    df = df[['stock_code', 'corp_name', 'year', 'quarter', 'type', 'order', 'account_name', 'amount', 'updated']]

    # id 생성
    df.index = df.stock_code + '_' + df.year.astype(str) + '_' + df.quarter.astype(str) + '_' + df.type + '_' + df.order.astype(str).str.zfill(3)
    df.index.name = 'id'

    return df

def _clean_exist(df: pd.DataFrame):
    delete_sql = f"""
        DELETE FROM `hello-phase3.naver_fn_data.test_income_statement_year`
        WHERE id IN {str(tuple(df.index.tolist()))}
    """

    # SQL 쿼리 실행
    job = client.query(delete_sql)
    print(job.result())

def _upload(df: pd.DataFrame, table_name: str):
    table_id = f'hello-phase3.naver_fn_data.{table_name}'

    # job_config = bigquery.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("stock_code", bigquery.enums.SqlTypeNames.STRING),
        #     bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
        #     bigquery.SchemaField("account_name", bigquery.enums.SqlTypeNames.STRING),
        # ]
    # )

    job = client.load_table_from_dataframe(df, table_id)  # Make an API request.
    print(job.result())  # Wait for the job to complete.

# 연간
def upload_crawring_to_gbq(crawring_df: pd.DataFrame,
                           stock_code: str,
                           corp_name: str,
                           type: str):

    df = _convert(crawring_df, stock_code, corp_name, type)
    _clean_exist(df)
    _upload(df, 'test_income_statement_year')

# 분기
def upload_crawring_to_gbq_q(crawring_df: pd.DataFrame,
                             stock_code: str,
                             corp_name: str,
                             type: str):

    df = _convert_q(crawring_df, stock_code, corp_name, type)
    _clean_exist(df)
    _upload(df, 'test_income_statement_quarter')
