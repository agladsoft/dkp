import os
import requests
from requests import Response
from dotenv import load_dotenv

os.environ['XL_IDP_ROOT_DKP'] = "."

load_dotenv()

SHEETS_NAME: list = ["ПЛАН_ПРОДАЖ", "ПЛАН-ПРОДАЖ", "ПЛАН ПРОДАЖ"]

DKP_NAMES: list = ["ДКП_ЮФО", "ДКП_ДВ"]

TRANSPOSE_NAMES: dict = {
    ("НАТУРАЛЬНЫЕ ПОКАЗАТЕЛИ, ктк",): "natural_indicators_ktk",
    ("СТАВКИ СОИСПОЛНИТЕЛЕЙ НА ЕДИНИЦУ", ): "unit_rates_co_executors",
    ("УДЕЛЬНЫЙ МАРЖИНАЛЬНЫЙ ДОХОД",): "specific_marginal_income",
    ("КОММЕНТАРИЙ К УСЛУГЕ",): "comment_service",
    ("СОИСПОЛНИТЕЛЬ",): "co_executor",
    ("ПРИЗНАК ВОЗМЕЩАЕМЫХ (76)",): "sign_compensatory",
    ("НАТУРАЛЬНЫЕ ПОКАЗАТЕЛИ, TEUS",): "natural_indicators_teus"
}

# Словарь, связывающий ключи TRANSPOSE_NAMES с соответствующими списками
REPEATED_COLUMNS_MAPPING: dict = {
    "natural_indicators_ktk": {
        ("янв",): "natural_indicators_ktk_jan",
        ("фев",): "natural_indicators_ktk_feb",
        ("мар",): "natural_indicators_ktk_mar",
        ("апр",): "natural_indicators_ktk_apr",
        ("май",): "natural_indicators_ktk_may",
        ("июн",): "natural_indicators_ktk_jun",
        ("июл",): "natural_indicators_ktk_jul",
        ("авг",): "natural_indicators_ktk_aug",
        ("сен",): "natural_indicators_ktk_sep",
        ("окт",): "natural_indicators_ktk_oct",
        ("ноя",): "natural_indicators_ktk_nov",
        ("дек",): "natural_indicators_ktk_dec"
    },
    "specific_marginal_income": {
        ("море",): "unit_margin_income_marine",
        ("порт",): "unit_margin_income_port",
        ("терминал1",): "unit_margin_income_terminal1",
        ("терминал2",): "unit_margin_income_terminal2",
        ("доп.терминал",): "unit_margin_income_other_terminal",
        ("авто1",): "unit_margin_income_avto1",
        ("авто2",): "unit_margin_income_avto2",
        ("авто3",): "unit_margin_income_avto3",
        ("жд1",): "unit_margin_income_rzhd1",
        ("жд2",): "unit_margin_income_rzhd2",
        ("таможня",): "unit_margin_income_custom",
        ("демерредж",): "unit_margin_income_demurrage",
        ("хранение",): "unit_margin_income_storage",
        ("прочие1",): "unit_margin_income_other1",
        ("прочие2",): "unit_margin_income_other2"
    },
    "comment_service": {
        ("море",): "service_marine",
        ("порт",): "service_port",
        ("терминал1",): "service_terminal1",
        ("терминал2",): "service_terminal2",
        ("доп.терминал",): "service_other_terminal",
        ("авто1",): "service_avto1",
        ("авто2",): "service_avto2",
        ("авто3",): "service_avto3",
        ("жд1",): "service_rzhd1",
        ("жд2",): "service_rzhd2",
        ("таможня",): "service_custom",
        ("демерредж",): "service_demurrage",
        ("хранение",): "service_storage",
        ("прочие1",): "service_other1",
        ("прочие2",): "service_other2"
    },
    "co_executor": {
        ("море",): "co_executor_marine",
        ("порт",): "co_executor_port",
        ("терминал1",): "co_executor_terminal1",
        ("терминал2",): "co_executor_terminal2",
        ("доп.терминал",): "co_executor_other_terminal",
        ("авто1",): "co_executor_avto1",
        ("авто2",): "co_executor_avto2",
        ("авто3",): "co_executor_avto3",
        ("жд1",): "co_executor_rzhd1",
        ("жд2",): "co_executor_rzhd2",
        ("таможня",): "co_executor_custom",
        ("демерредж",): "co_executor_demurrage",
        ("хранение",): "co_executor_storage",
        ("прочие1",): "co_executor_other1",
        ("прочие2",): "co_executor_other2"
    },
    "natural_indicators_teus": {
        ("янв",): "natural_indicators_teus_jan",
        ("фев",): "natural_indicators_teus_feb",
        ("мар",): "natural_indicators_teus_mar",
        ("апр",): "natural_indicators_teus_apr",
        ("май",): "natural_indicators_teus_may",
        ("июн",): "natural_indicators_teus_jun",
        ("июл",): "natural_indicators_teus_jul",
        ("авг",): "natural_indicators_teus_aug",
        ("сен",): "natural_indicators_teus_sep",
        ("окт",): "natural_indicators_teus_oct",
        ("ноя",): "natural_indicators_teus_nov",
        ("дек",): "natural_indicators_teus_dec"
    }
}

HEADERS_ENG: dict = {
    ("клиент",): "client",
    ("описание",): "description",
    ("стратегич. проект",): "project",
    ("груз",): "cargo",
    ("направление",): "direction",
    ("бассейн",): "bay",
    ("принадлежность ктк",): "owner",
    ("разм",): "container_size"
}

DATE_FORMATS: list = [
    "%m/%d/%y",
    "%d.%m.%Y",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y",
    "%d%b%Y"
]

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


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass
