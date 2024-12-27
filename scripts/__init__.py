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
            block_key = row[block_index]
            table_key = row[group_index]
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

ERRORS: dict = {
    1: "Ошибка, которая возникает в случае не проставленной даты в начале названия файла",
    2: "Ошибка, которая возникает в случае изменения названия колонок или отсутствия столбца",
    3: "Ошибка, которая возникает в случае отсутствия даты, рейса или названия судна в шапке файла. "
       "Либо возникает при их обработке",
    4: "Ошибка, которая возникает в случае образования пустого json. Таким образом, в базе не будет данных",
    5: "Ошибка, которая возникает в случае обработки данных внутри таблицы."
       "Например, при извлечении числового значения мы получаем строку. Будет выглядеть так: error_code_5_in_row_1."
       "Что обозначает код ошибки № 5, которая произошла на 1 строке",
    6: "Неизвестная ошибка, которая возникает в случае обработки скриптов. Будет выглядеть так: error_code_6"
}


def telegram(message) -> int:
    chat_id: str = '-1002064780308'
    topic: str = '1069'
    message_id: str = '1071'
    url: str = f"https://api.telegram.org/bot{os.environ['TOKEN_TELEGRAM']}/sendMessage"
    params: dict = {
        "chat_id": f"{chat_id}/{topic}",
        "text": message,
        "reply_to_message_id": message_id
    }
    response: Response = requests.get(url, params=params)
    return response.status_code
