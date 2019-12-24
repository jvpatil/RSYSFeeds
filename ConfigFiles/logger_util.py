import logging
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import inspect
import traceback
runtime = datetime.now()
runtime = runtime.strftime("%d%m%Y_%H%M%S")

# FORMATTER_FILE = logging.Formatter("[%(asctime)s : %(name)s:%(lineno)s - %(funcName)20s() ] %(message)s")
# FORMATTER_FILE = logging.Formatter("[%(asctime)s %(name)s : %(lineno)s] [%(levelname)s] - %(message)s")
# FORMATTER_FILE = logging.Formatter("[%(asctime)s] [%(levelname)8s] --- %(message)s (%(filename)s:%(lineno)s)", "%Y-%m-%d %H:%M:%S")
FORMATTER_FILE = logging.Formatter("[%(asctime)s] [%(name)-45s :  %(lineno)4s] [%(levelname)4s] -- %(message)s")
FORMATTER_CONSOLE = logging.Formatter("[%(levelname)4s] - %(message)s")
# LOG_FILE = "..//messages"+runtime+".log"
# LOG_FILE = "..//messages.log"

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   # logger = logging.getLogger(test_class_name)
   console_handler.setFormatter(FORMATTER_CONSOLE)
   return console_handler

def get_file_handler(LOG_FILE, log_name_len):
   file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
   # FORMATTER_FILE = logging.Formatter("[%(asctime)s] [%(name)-"+str(log_name_len)+"s :  %(lineno)-4s] [%(levelname)4s] -- %(message)s")
   file_handler.setFormatter(FORMATTER_FILE)
   return file_handler

def get_logger(logger_name):
    split_logger_name = logger_name.split('::')  # TestName :: Package.Module
    test_class_name  = split_logger_name[0]      # TestName
    log_name = split_logger_name[1]              # Package.Module
    logger = logging.getLogger(log_name)
    if (logger.hasHandlers()):
        logger.handlers.clear()
    # logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG) # better to have too much log than not enough
    # logger.addHandler(get_console_handler())
    LOG_FILE ="..//Logs//"+test_class_name+".log"
    logger.addHandler(get_file_handler(LOG_FILE,len(log_name)))
    logger.addHandler(get_console_handler())
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger


def get_function_name():
    return traceback.extract_stack(None, 2)[0][2]

def get_function_parameters_and_values():
    frame = inspect.currentframe().f_back
    args, _, _, values = inspect.getargvalues(frame)
    return ([(i, values[i]) for i in args])

def test_method_details():
    logging.info('Running ' + get_function_name() + '(' + str(get_function_parameters_and_values()) +')')
    pass