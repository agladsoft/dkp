import os
import logging

_log_format: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
_dateftm: str = "%d/%B/%Y %H:%M:%S"


def get_file_handler(name: str) -> logging.FileHandler:
    """
    Creates a file handler for logging.

    Creates a file handler for logging, named after the given name, and returns it.
    The file handler is configured to write to a file in the "logging" directory
    under the XL_IDP_ROOT_DKP environment variable. The file handler is set to
    log INFO and above, and is configured to use the _log_format and _dateftm
    variables for formatting.

    :param name: The name to give to the file handler, which will also be the
                 base name of the log file.
    :return: A logging.FileHandler object.
    """
    log_dir_name: str = f"{os.environ.get('XL_IDP_ROOT_DKP')}/logging"
    if not os.path.exists(log_dir_name):
        os.mkdir(log_dir_name)
    file_handler: logging.FileHandler = logging.FileHandler(f"{log_dir_name}/{name}.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(_log_format, datefmt=_dateftm))
    return file_handler


def get_logger(name: str) -> logging.getLogger:
    """
    Creates a logger with the given name.

    Creates a logger with the given name, and returns it. The logger is
    configured to log INFO and above, and is configured to use the
    get_file_handler to write to a file in the "logging" directory under the
    XL_IDP_ROOT_DKP environment variable.

    :param name: The name to give to the logger.
    :return: A logging.getLogger object.
    """
    logger: logging.getLogger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.addHandler(get_file_handler(name))
    return logger
