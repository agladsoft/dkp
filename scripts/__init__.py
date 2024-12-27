import os
import requests
from requests import Response
from dotenv import load_dotenv
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import Sequence

os.environ['XL_IDP_ROOT_DKP'] = "."

load_dotenv()


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


def group_columns(
    reference: Sequence,
    group_index: int,
    column_index: int,
    filter_key: int = None,
    filter_value: str = None
):
    result: dict = {}
    for row in reference:
        if filter_key is None or row[filter_key] == filter_value:
            if row[group_index] in result:
                result[row[group_index]] = result[row[group_index]] + (row[column_index],)
            else:
                result[row[group_index]] = (row[column_index],)
    return result


def group_nested_columns(
    reference: Sequence,
    block_index: int,
    group_index: int,
    column_index: int,
    filter_key: int,
    filter_value: str
):
    result: dict = {}
    for row in reference:
        if row[filter_key] == filter_value:
            block_key: str = row[block_index]
            table_key: str = row[group_index]
            if block_key not in result:
                result[block_key] = {}
            if table_key in result[block_key]:
                result[block_key][table_key] = result[block_key][table_key] + (row[column_index],)
            else:
                result[block_key][table_key] = (row[column_index],)
    return result


class MissingEnvironmentVariable(Exception):
    pass


client: Client = get_client(
    host=get_my_env_var('HOST'),
    database=get_my_env_var('DATABASE'),
    username=get_my_env_var('USERNAME_DB'),
    password=get_my_env_var('PASSWORD')
)
reference_dkp: Sequence = client.query("SELECT * FROM reference_dkp").result_rows

SHEETS_NAME: list = [column[2] for column in reference_dkp if column[0] == "Наименования листов"]
DKP_NAMES: list = [column[2] for column in reference_dkp if column[0] == "Наименования в файле"]

COLUMN_NAMES: dict = group_columns(
    reference=reference_dkp,
    group_index=3,
    column_index=2,
    filter_key=0,
    filter_value="Наименования столбцов"
)

BLOCK_NAMES: dict = group_columns(
    reference=reference_dkp,
    group_index=3,
    column_index=2,
    filter_key=0,
    filter_value="Наименования блоков"
)

BLOCK_TABLE_COLUMNS: dict = group_nested_columns(
    reference=reference_dkp,
    block_index=0,
    group_index=3,
    column_index=2,
    filter_key=1,
    filter_value="Столбцы таблиц в блоках",
)

DATE_FORMATS: list = [
    "%m/%d/%y",
    "%d.%m.%Y",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y",
    "%d%b%Y"
]

MONTH_NAMES: list = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]


def telegram(message) -> int:
    url: str = f"https://api.telegram.org/bot{os.environ['TOKEN_TELEGRAM']}/sendMessage"
    params: dict = {
        "chat_id": f"{os.environ['CHAT_ID']}/{os.environ['TOPIC']}",
        "text": message,
        "reply_to_message_id": os.environ['ID']
    }
    response: Response = requests.get(url, params=params)
    return response.status_code
