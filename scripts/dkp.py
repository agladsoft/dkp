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
        if hasattr(obj, '__dict__'):
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
        Checks if a given string can be converted to a float.
        :param x: The string to check.
        :return: True if x can be converted to a float, False otherwise.
        """
        try:
            float(re.sub(r'(?<=\d) (?=\d)', '', x))
            return True
        except ValueError:
            return False

    @staticmethod
    def merge_two_dicts(x: Dict, y: Dict) -> dict:
        """
        Merge two dictionaries, x and y, into a new dictionary, z.
        The values in y will overwrite the values in x if there are any keys in common.

        :param x: The first dictionary to merge.
        :param y: The second dictionary to merge.
        :return: A new dictionary with the merged values.
        """
        z: Dict = x.copy()  # start with keys and values of x
        z.update(y)  # modifies z with keys and values of y
        return z

    @staticmethod
    def _get_list_columns() -> List[str]:
        """
        Returns a list of all possible column names.

        This function takes all the values from COLUMN_NAMES (which is a dictionary of lists)
        and flattens them into a single list. This list is then returned.

        :return: A list of column names.
        """
        list_columns: list = []
        for keys in list(COLUMN_NAMES.values()):
            list_columns.extend(iter(keys))
        return list_columns

    @staticmethod
    def remove_symbols_in_columns(row: Optional[str]) -> str:
        """
        Removes extra spaces and newline characters from a string.

        This method takes a string, removes any extra spaces and newline characters,
        and returns the cleaned string. If the input is not a string, it returns the input as is.

        :param row: The string from which symbols are to be removed.
        :return: The cleaned string with extra spaces and newline characters removed.
        """
        if row and isinstance(row, str):
            row: str = re.sub(r" +", " ", row).strip()
            row: str = re.sub(r"\n", " ", row).strip()
        return row

    def get_probability_of_header(self, row: list, list_columns: list) -> int:
        """
        Calculates the probability that a given row is a header.

        This method takes a row (a list of strings) and a list of column names,
        removes any extra spaces and newline characters from the strings in the row,
        and then counts how many of the strings in the row are in the list of column names.
        The count is then divided by the length of the row, and the result is multiplied by 100
        to get a percentage. The percentage is then returned as an integer.

        :param row: The row to calculate the probability for.
        :param list_columns: The list of column names.
        :return: The probability that the given row is a header, as an integer between 0 and 100.
        """
        row: list = list(map(self.remove_symbols_in_columns, row))
        count: int = sum(element in list_columns for element in row)
        return int(count / len(row) * 100)

    def get_columns_position(self, row: list, block_position: list, headers: dict, dict_columns_position) -> None:
        """
        Finds the position of all columns in a row.

        This method takes a row, a range of columns (block_position), a dictionary of headers,
        and a dictionary to store the positions of the columns.
        It removes any extra spaces and newline characters from the strings in the row,
        and then iterates over the strings in the row.
        For each string, it checks if it matches any of the strings in the headers dictionary,
        and if it does, it stores the index of the string in the row in the dict_columns_position dictionary.
        The index is stored with the English name of the column as the key.

        :param row: The row to find the columns in.
        :param block_position: A list containing the start and end index of the block of columns to search in.
        :param headers: A dictionary of headers, where the keys are the English names of the columns,
                        and the values are lists of strings that may appear in the row as the header.
        :param dict_columns_position: A dictionary to store the positions of the columns in.
        :return: None
        """
        start_index, end_index = block_position
        row: list = list(map(self.remove_symbols_in_columns, row))
        for index, col in enumerate(row):
            for eng_column, columns in headers.items():
                for column_eng in columns:
                    if col == column_eng and start_index <= index < end_index:
                        dict_columns_position[eng_column] = index

    def check_errors_in_columns(self, dict_columns: dict, message: str, error_code: int) -> None:
        """
        Checks if there are any empty columns in the given dictionary.

        This method takes a dictionary of columns, a message, and an error code.
        It checks if there are any empty columns in the dictionary (i.e., columns with a value of None).
        If there are, it logs an error message with the error code, prints the error code to stderr,
        sends a message to Telegram with the error code and the names of the empty columns,
        and then exits with the error code.

        :param dict_columns: A dictionary of columns, where the keys are the English names of the columns,
                             and the values are the positions of the columns in the row.
        :param message: The message to log and print in case of an error.
        :param error_code: The error code to exit with in case of an error.
        :return: None
        """
        if empty_columns := [key for key, value in dict_columns.items() if value is None]:
            logger.error(f"{message}. Empty columns - {empty_columns}")
            print(f"{error_code}", file=sys.stderr)
            telegram(
                f"Ошибка при обработке файла {self.basename_filename}. "
                f"Код ошибки - {error_code}. Пустые поля - {empty_columns}"
            )
            sys.exit(error_code)

    def check_errors_in_header(self, row: list) -> None:
        """
        Checks if there are any empty columns in the header of the given row.

        This method takes a row of the Excel file and checks if there are any empty columns in the header.
        If there are, it logs an error message with the error code 2, prints the error code to stderr,
        sends a message to Telegram with the error code and the names of the empty columns,
        and then exits with the error code 2.

        It also checks if there are any repeated columns in the header and if there are,
        it logs an error message with the error code 2, prints the error code to stderr,
        sends a message to Telegram with the error code and the names of the repeated columns,
        and then exits with the error code 2.

        :param row: The row of the Excel file to check.
        :return: None
        """
        self.check_errors_in_columns(
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
            dict_columns=self.dict_columns_position,
            message="Error code 2: Column not in file or changed",
            error_code=2
        )

    def is_table_starting(self, row: list) -> bool:
        """
        Checks if the table is starting in the given row.

        This method takes a row of the Excel file and checks if the table is starting in that row.
        It does this by trying to get the value of the column "client" in the row.
        If the column is not found, it returns False.
        If the column is found, it returns the value of the column.

        :param row: The row of the Excel file to check.
        :return: The value of the column "client" if the table is starting in the row, False otherwise.
        """
        try:
            return row[self.dict_columns_position["client"]]
        except TypeError:
            return False

    def write_to_json(self, list_data: List[dict]) -> None:
        """
        Writes the given list of dictionaries to a JSON file.

        This method takes a list of dictionaries and writes them to a JSON file.
        The file is written to the same folder as the given Excel file.
        The filename is the same as the Excel file, but with a `.json` extension instead of `.xls`.
        If the list of dictionaries is empty, it logs an error message with the error code 4,
        prints the error code to stderr, sends a message to Telegram with the error code and the name of the file,
        and then exits with the error code.

        :param list_data: The list of dictionaries to write to the JSON file.
        :return: None
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
        Extracts and processes data from a row in a table, returning a dictionary of parsed values.

        This function takes a row from a table and extracts various data points such as client details,
        project information, and financial metrics. It uses a helper function `safe_strip` to clean and
        convert the data appropriately. The parsed data is returned as a dictionary along with additional
        metadata.

        :param index: The index of the current row.
        :param index_month: The numeric representation of the month.
        :param month_string: The string representation of the month.
        :param row: The list of values in the current row.
        :param metadata: Additional metadata extracted earlier in the process.
        :return: A dictionary containing parsed and processed data from the row.
        """

        def safe_strip(value: str) -> Optional[str, float, int]:
            if not value:
                return None

            stripped_value = value.strip()  # Сначала убираем лишние пробелы
            if not self.is_digit(stripped_value):
                return stripped_value

            try:
                return float(stripped_value) if '.' in stripped_value else int(stripped_value)
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Failed to convert value '{stripped_value}' to number: {str(e)}. "
                    f"Returning original stripped string."
                )
                return stripped_value

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
        Extract metadata from filename.

        This method takes the filename of the given Excel file and extracts metadata from it.
        It matches the department and year from the filename and logs an error with the error code 10
        if the department is not found and an error with the error code 1 if the year is not found.
        Then it sends a message to Telegram with the error code and the filename
        if an error occurs and exits with the error code.

        :return: A dictionary with the extracted metadata.
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
        Sends an error message to the logger and Telegram, then exits with the given error code.

        This method takes a message and an error code, appends the current file's basename to the message,
        logs the error message, sends it to Telegram, and exits the program with the specified error code.

        :param message: The error message to log and send.
        :param error_code: The error code to exit with.
        :return: None
        """
        error_message: str = f"{message}{self.basename_filename}"
        logger.error(error_message)
        telegram(error_message)
        sys.exit(error_code)

    def parse_sheet(self, df: pd.DataFrame, coefficient_of_header: int = 3) -> None:
        """
        Parse a sheet of Excel file.

        This method takes a pandas DataFrame, representing a sheet of the Excel file,
        and parses it, extracting metadata from the filename,
        identifying the header and the table, and extracting content from the table.
        It then writes the extracted content to a JSON file.

        If an error occurs during processing, it logs an error message with the error code 5,
        sends a message to Telegram with the error code and the filename,
        and then exits with the error code 5.

        :param df: The pandas DataFrame representing the sheet of the Excel file.
        :param coefficient_of_header: The coefficient to determine if a row is a header or not.
        :return: None
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
        The main method of the class.

        This method reads the Excel file given by the filename, extracts the needed sheet,
        parses the sheet, and writes the extracted data to a JSON file.

        If an error occurs during processing, it logs an error message,
        sends a message to Telegram with the error message,
        and exits with the error code 1.
        :return: None
        """
        try:
            sheets: list = pd.ExcelFile(self.filename).sheet_names
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
