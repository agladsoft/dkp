import json
import re
import sys
import app_logger
import numpy as np
import pandas as pd
from re import Match
from __init__ import *
from datetime import datetime
from typing import List, Dict, Optional, Union, Hashable

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", ""))


class JsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, DKP):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class DKP(object):
    def __init__(self, filename: str, folder: str):
        self.filename: str = filename
        self.basename_filename: str = os.path.basename(filename)
        self.folder: str = folder
        self.dict_columns_position: Dict[str, Optional[int]] = {
            "client": None,
            "description": None,
            "project": None,
            "cargo": None,
            "direction": None,
            "bay": None,
            "owner": None,
            "container_size": None,
            "unit_margin_income_marine": None,
            "unit_margin_income_port": None,
            "unit_margin_income_terminal1": None,
            "unit_margin_income_terminal2": None,
            "unit_margin_income_other_terminal": None,
            "unit_margin_income_avto1": None,
            "unit_margin_income_avto2": None,
            "unit_margin_income_avto3": None,
            "unit_margin_income_rzhd1": None,
            "unit_margin_income_rzhd2": None,
            "unit_margin_income_custom": None,
            "unit_margin_income_demurrage": None,
            "unit_margin_income_storage": None,
            "unit_margin_income_other1": None,
            "unit_margin_income_other2": None,
            "service_marine": None,
            "service_port": None,
            "service_terminal1": None,
            "service_terminal2": None,
            "service_other_terminal": None,
            "service_avto1": None,
            "service_avto2": None,
            "service_avto3": None,
            "service_rzhd1": None,
            "service_rzhd2": None,
            "service_custom": None,
            "service_demurrage": None,
            "service_storage": None,
            "service_other1": None,
            "service_other2": None,
            "co_executor_marine": None,
            "co_executor_port": None,
            "co_executor_terminal1": None,
            "co_executor_terminal2": None,
            "co_executor_other_terminal": None,
            "co_executor_avto1": None,
            "co_executor_avto2": None,
            "co_executor_avto3": None,
            "co_executor_rzhd1": None,
            "co_executor_rzhd2": None,
            "co_executor_custom": None,
            "co_executor_demurrage": None,
            "co_executor_storage": None,
            "co_executor_other1": None,
            "co_executor_other2": None
        }
        self.dict_block_position: Dict[str, Optional[int]] = {
            "natural_indicators_ktk": None,
            "unit_rates_co_executors": None,
            "specific_marginal_income": None,
            "comment_service": None,
            "co_executor": None,
            "sign_compensatory": None,
            "natural_indicators_teus": None
        }

    @staticmethod
    def is_digit(x: str) -> bool:
        """
        Checks if a value is a number.
        :param x:
        :return:
        """
        try:
            float(re.sub(r'(?<=\d) (?=\d)', '', x))
            return True
        except ValueError:
            return False

    @staticmethod
    def merge_two_dicts(x: Dict, y: Dict) -> dict:
        """
        Merges two dictionaries.
        :param x:
        :param y:
        :return:
        """
        z: Dict = x.copy()  # start with keys and values of x
        z.update(y)  # modifies z with keys and values of y
        return z

    @staticmethod
    def _get_list_columns() -> List[str]:
        """
        Getting all column names for all lines in the __init__.py file.
        :return:
        """
        list_columns = []
        for keys in list(COLUMN_NAMES.values()):
            list_columns.extend(iter(keys))
        return list_columns

    @staticmethod
    def remove_symbols_in_columns(row: Optional[str]) -> str:
        """
        Bringing the header column to a unified form.
        :param row:
        :return:
        """
        if row and isinstance(row, str):
            row: str = re.sub(r" +", " ", row).strip()
            row: str = re.sub(r"\n", " ", row).strip()
        return row

    def get_probability_of_header(self, row: list, list_columns: list) -> int:
        """
        Getting the probability of a row as a header.
        :param row:
        :param list_columns:
        :return:
        """
        row: list = list(map(self.remove_symbols_in_columns, row))
        count: int = sum(element in list_columns for element in row)
        return int(count / len(row) * 100)

    def get_columns_position(self, row: list, block_position: list, headers: dict, dict_columns_position) -> None:
        """
        Get the position of each column in the file to process the row related to that column.
        :param row:
        :param headers:
        :param block_position:
        :param dict_columns_position:
        :return:
        """
        start_index, end_index = block_position
        row: list = list(map(self.remove_symbols_in_columns, row))
        for index, col in enumerate(row):
            for eng_column, columns in headers.items():
                for column_eng in columns:
                    if col == column_eng and start_index <= index < end_index:
                        dict_columns_position[eng_column] = index

    def check_errors_in_columns(self, list_columns: list, dict_columns: dict, message: str, error_code: int) -> None:
        """
        Checks for the presence of all columns in the header.
        :param list_columns:
        :param dict_columns:
        :param message:
        :param error_code:
        :return:
        """
        if not all(i for i in list_columns if i is None):
            logger.error(message)
            logger.error(dict_columns)
            print(f"{error_code}", file=sys.stderr)
            telegram(f"Ошибка при обработке файла {self.basename_filename}, код ошибки {error_code}")
            sys.exit(error_code)

    def check_errors_in_header(self, row: list) -> None:
        """
        Checking for columns in the entire document, counting more than just columns on the same line.
        :param row:
        :return:
        """
        self.check_errors_in_columns(
            list_columns=list(self.dict_block_position.values()),
            dict_columns=self.dict_block_position,
            message="Error code 2: Block columns not in file or changed",
            error_code=2
        )
        self.get_columns_position(row, [0, len(row)], COLUMN_NAMES, self.dict_columns_position)

        items: list = list(self.dict_block_position.items())
        dict_block_position_ranges = {
            current_key: [start_index, next_index]
            for (current_key, start_index), (_, next_index) in zip(items, items[1:] + [(None, len(row))])
        }

        for col, block_position in dict_block_position_ranges.items():
            if repeated_column := BLOCK_TABLE_COLUMNS.get(col):
                self.get_columns_position(row, block_position, repeated_column, self.dict_columns_position)

        self.check_errors_in_columns(
            list_columns=list(self.dict_columns_position.values()),
            dict_columns=self.dict_columns_position,
            message="Error code 2: Column not in file or changed",
            error_code=2
        )

    def is_table_starting(self, row: list) -> bool:
        """
        Understanding when a headerless table starts.
        :param row:
        :return:
        """
        try:
            return row[self.dict_columns_position["client"]]
        except TypeError:
            return False

    def write_to_json(self, list_data: List[dict]) -> None:
        """
        Writing data to json.
        :param list_data:
        :return:
        """
        if not list_data:
            logger.error("Error code 4: length list equals 0!")
            print("4", file=sys.stderr)
            telegram(f"В Файле {self.basename_filename}: Отсутствуют данные : Error code 4: length list equals 0!")
            sys.exit(4)
        output_file_path = os.path.join(self.folder, f'{self.basename_filename}.json')
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(list_data, f, ensure_ascii=False, indent=4, cls=JsonEncoder)

    def get_content_in_table(
        self,
        index: Union[int, Hashable],
        index_month: int,
        month_string,
        row: list,
        metadata: dict
    ) -> dict:
        """
        Getting values from columns in a table.
        :param index:
        :param index_month:
        :param month_string:
        :param row:
        :param metadata:
        :return:
        """

        def safe_strip(value):
            if value:
                value = value.strip()  # Сначала убираем лишние пробелы
                if self.is_digit(value):
                    return float(value) if '.' in value else int(value)  # Проверка на тип данных
                return value
            return None

        logger.info(f'row {index} is {row}')
        parsed_record: dict = {
            "client": safe_strip(row[self.dict_columns_position["client"]]),
            "description": safe_strip(row[self.dict_columns_position["description"]]),
            "project": safe_strip(row[self.dict_columns_position["project"]]),
            "cargo": safe_strip(row[self.dict_columns_position["cargo"]]),
            "direction": safe_strip(row[self.dict_columns_position["direction"]]),
            "bay": safe_strip(row[self.dict_columns_position["bay"]]),
            "owner": safe_strip(row[self.dict_columns_position["owner"]]),
            "month": index_month,
            "month_string": month_string,
            "date": f"{metadata['year']}-{index_month:02d}-01",

            "container_count": next((
                safe_strip(row[self.dict_columns_position[key]])
                for key, val in BLOCK_TABLE_COLUMNS["natural_indicators_ktk"].items()
                if month_string in val
            ), None),
            "teu": next((
                safe_strip(row[self.dict_columns_position[key]])
                for key, val in BLOCK_TABLE_COLUMNS["natural_indicators_teus"].items()
                if month_string in val
            ), None),

            "unit_margin_income_marine": safe_strip(row[self.dict_columns_position["unit_margin_income_marine"]]),
            "unit_margin_income_port": safe_strip(row[self.dict_columns_position["unit_margin_income_port"]]),
            "unit_margin_income_terminal1": safe_strip(row[self.dict_columns_position["unit_margin_income_terminal1"]]),
            "unit_margin_income_terminal2": safe_strip(row[self.dict_columns_position["unit_margin_income_terminal2"]]),
            "unit_margin_income_other_terminal": safe_strip(
                row[self.dict_columns_position["unit_margin_income_other_terminal"]]
            ),
            "unit_margin_income_avto1": safe_strip(row[self.dict_columns_position["unit_margin_income_avto1"]]),
            "unit_margin_income_avto2": safe_strip(row[self.dict_columns_position["unit_margin_income_avto2"]]),
            "unit_margin_income_avto3": safe_strip(row[self.dict_columns_position["unit_margin_income_avto3"]]),
            "unit_margin_income_rzhd1": safe_strip(row[self.dict_columns_position["unit_margin_income_rzhd1"]]),
            "unit_margin_income_rzhd2": safe_strip(row[self.dict_columns_position["unit_margin_income_rzhd2"]]),
            "unit_margin_income_custom": safe_strip(row[self.dict_columns_position["unit_margin_income_custom"]]),
            "unit_margin_income_demurrage": safe_strip(row[self.dict_columns_position["unit_margin_income_demurrage"]]),
            "unit_margin_income_storage": safe_strip(row[self.dict_columns_position["unit_margin_income_storage"]]),
            "unit_margin_income_other1": safe_strip(row[self.dict_columns_position["unit_margin_income_other1"]]),
            "unit_margin_income_other2": safe_strip(row[self.dict_columns_position["unit_margin_income_other2"]]),

            "service_marine": safe_strip(row[self.dict_columns_position["service_marine"]]),
            "service_port": safe_strip(row[self.dict_columns_position["service_port"]]),
            "service_terminal1": safe_strip(row[self.dict_columns_position["service_terminal1"]]),
            "service_terminal2": safe_strip(row[self.dict_columns_position["service_terminal2"]]),
            "service_other_terminal": safe_strip(row[self.dict_columns_position["service_other_terminal"]]),
            "service_avto1": safe_strip(row[self.dict_columns_position["service_avto1"]]),
            "service_avto2": safe_strip(row[self.dict_columns_position["service_avto2"]]),
            "service_avto3": safe_strip(row[self.dict_columns_position["service_avto3"]]),
            "service_rzhd1": safe_strip(row[self.dict_columns_position["service_rzhd1"]]),
            "service_rzhd2": safe_strip(row[self.dict_columns_position["service_rzhd2"]]),
            "service_custom": safe_strip(row[self.dict_columns_position["service_custom"]]),
            "service_demurrage": safe_strip(row[self.dict_columns_position["service_demurrage"]]),
            "service_storage": safe_strip(row[self.dict_columns_position["service_storage"]]),
            "service_other1": safe_strip(row[self.dict_columns_position["service_other1"]]),
            "service_other2": safe_strip(row[self.dict_columns_position["service_other2"]]),

            "co_executor_marine": safe_strip(row[self.dict_columns_position["co_executor_marine"]]),
            "co_executor_port": safe_strip(row[self.dict_columns_position["co_executor_port"]]),
            "co_executor_terminal1": safe_strip(row[self.dict_columns_position["co_executor_terminal1"]]),
            "co_executor_terminal2": safe_strip(row[self.dict_columns_position["co_executor_terminal2"]]),
            "co_executor_other_terminal": safe_strip(row[self.dict_columns_position["co_executor_other_terminal"]]),
            "co_executor_avto1": safe_strip(row[self.dict_columns_position["co_executor_avto1"]]),
            "co_executor_avto2": safe_strip(row[self.dict_columns_position["co_executor_avto2"]]),
            "co_executor_avto3": safe_strip(row[self.dict_columns_position["co_executor_avto3"]]),
            "co_executor_rzhd1": safe_strip(row[self.dict_columns_position["co_executor_rzhd1"]]),
            "co_executor_rzhd2": safe_strip(row[self.dict_columns_position["co_executor_rzhd2"]]),
            "co_executor_custom": safe_strip(row[self.dict_columns_position["co_executor_custom"]]),
            "co_executor_demurrage": safe_strip(row[self.dict_columns_position["co_executor_demurrage"]]),
            "co_executor_storage": safe_strip(row[self.dict_columns_position["co_executor_storage"]]),
            "co_executor_other1": safe_strip(row[self.dict_columns_position["co_executor_other1"]]),
            "co_executor_other2": safe_strip(row[self.dict_columns_position["co_executor_other2"]]),

            "original_file_name": self.basename_filename,
            "original_file_parsed_on": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        return self.merge_two_dicts(metadata, parsed_record)

    def extract_metadata_from_filename(self) -> dict:
        """
        Extracts department and year information from the file name.
        :return:
        """
        logger.info(f'File - {self.basename_filename}. Datetime - {datetime.now()}')
        # Match department
        dkp_pattern: str = '|'.join(map(re.escape, DKP_NAMES))
        department_match: Match = re.search(rf'{dkp_pattern}', self.basename_filename)
        if not department_match:
            self.send_error(
                message='Error code 10: Department not in file name! File: ', error_code=10
            )
        metadata: dict = {'department': department_match.group(0)}
        # Match year
        year_match: Match = re.search(r'\d{4}', self.basename_filename)
        if not year_match:
            self.send_error(
                message='Error code 1: Year not in file name! File: ', error_code=1
            )
        metadata['year'] = int(year_match.group(0))

        return metadata

    def send_error(self, message: str, error_code: int) -> None:
        """
        Send error.
        :param message:
        :param error_code:
        :return:
        """
        error_message: str = f"{message}{self.basename_filename}"
        logger.error(error_message)
        telegram(error_message)
        sys.exit(error_code)

    def parse_sheet(self, df: pd.DataFrame, coefficient_of_header: int = 3) -> None:
        """
        Parse sheet.
        :param df:
        :param coefficient_of_header:
        :return:
        """
        list_data: list = []
        metadata: dict = self.extract_metadata_from_filename()
        list_columns: List[str] = self._get_list_columns()
        index: Union[int, Hashable]
        for index, row in df.iterrows():
            row = list(row.to_dict().values())
            if self.get_probability_of_header(row, list_columns) > coefficient_of_header:
                self.check_errors_in_header(row)
            elif not self.dict_columns_position["client"]:
                self.get_columns_position(row, [0, len(row)], BLOCK_NAMES, self.dict_block_position)
            elif self.is_table_starting(row):
                try:
                    list_data.extend(
                        self.get_content_in_table(index, index_month, month_string, row, metadata)
                        for index_month, month_string in enumerate(MONTH_NAMES, start=1)
                    )
                except (IndexError, ValueError, TypeError) as exception:
                    telegram(f"Ошибка возникла в строке {index + 1}. Файл - {self.basename_filename}")
                    logger.error(f"Error code 5: error processing in row {index + 1}! Exception - {exception}")
                    print(f"5_in_row_{index + 1}", file=sys.stderr)
                    sys.exit(5)
        self.write_to_json(list_data)

    def main(self) -> None:
        """
        Main function.
        :return:
        """
        try:
            sheets = pd.ExcelFile(self.filename).sheet_names
            logger.info(f"Sheets is {sheets}")
            needed_sheet: list = [sheet for sheet in sheets if sheet in SHEETS_NAME]
            if len(needed_sheet) > 1:
                raise ValueError(f"Нужных листов из SHEETS_NAME больше ОДНОГО: {needed_sheet}")
            for sheet in needed_sheet:
                df = pd.read_excel(self.filename, sheet_name=sheet, dtype=str, header=None)
                df = df.dropna(how='all').replace({np.nan: None, "NaT": None})
                self.parse_sheet(df)
        except Exception as exception:
            logger.error(f"Ошибка при чтении файла {self.basename_filename}: {exception}")
            telegram(f'Ошибка при обработке файла {self.basename_filename}. Ошибка : {exception}')
            print("unknown", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    logger.info(f"{os.path.basename(sys.argv[1])} has started processing")
    dkp: DKP = DKP(os.path.abspath(sys.argv[1]), sys.argv[2])
    dkp.main()
    logger.info(f"{os.path.basename(sys.argv[1])} has finished processing")
