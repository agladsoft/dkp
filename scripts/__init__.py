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
    """
    Retrieves the value of the given environment variable.
    :param var_name: The name of the environment variable to retrieve.
    :return: The value of the given environment variable.
    :raises MissingEnvironmentVariable: If the given environment variable does not exist.
    """
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
    """
    Groups columns of a given `reference` by a given `group_index` and then by a given `column_index`.

    If `filter_key` and `filter_value` are provided, it filters the rows of `reference` that have the value
    `filter_value` in the column with index `filter_key`.

    :param reference: The sequence of rows to group.
    :param group_index: The index of the column to group the rows by.
    :param column_index: The index of the column to group by after grouping by `group_index`.
    :param filter_key: The index of the column to filter the rows by.
    :param filter_value: The value of the column with index `filter_key` to filter the rows by.
    :return: A dictionary of dictionaries, where the keys of the outer dictionary are the values of the column
             with index `group_index`, and the keys of the inner dictionaries are the values of the column with
             index `column_index`, and the values of the inner dictionaries are tuples of the values of the column
             with index `column_index`.
    """
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
    """
    Groups nested columns of a given `reference` by specified indices.

    This function processes a sequence of rows and groups them hierarchically based on the values at
    specified indices. It first groups the rows by `block_index` and within each block, it groups by `group_index`.
    It filters the rows using a specified `filter_key` and `filter_value`, only including rows
    that match the filter criteria.

    :param reference: The sequence of rows to group.
    :param block_index: The index of the column to group the blocks by.
    :param group_index: The index of the column to group within each block.
    :param column_index: The index of the column whose values are to be collected in the groups.
    :param filter_key: The index of the column to filter the rows by.
    :param filter_value: The value that the column at `filter_key` must equal for a row to be included.
    :return: A nested dictionary where the outer keys correspond to unique values of the column at `block_index`,
             the inner keys correspond to unique values of the column at `group_index`, and the inner values
             are tuples of values from the column at `column_index`.
    """
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
    url: str = f"https://api.telegram.org/bot{get_my_env_var('TOKEN_TELEGRAM')}/sendMessage"
    params: dict = {
        "chat_id": f"{get_my_env_var('CHAT_ID')}/{get_my_env_var('TOPIC')}",
        "text": message,
        "reply_to_message_id": get_my_env_var('ID')
    }
    response: Response = requests.get(url, params=params)
    return response.status_code
