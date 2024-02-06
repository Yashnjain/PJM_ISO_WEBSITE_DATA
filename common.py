import datetime
import logging
import os
from pathlib import Path
import datetime
from pythonjsonlogger import jsonlogger

DATABASE = "POWERDB_DEV"
SCHEMA = "PQUANT"
ROLE = "OWNER_POWERDB_DEV"


logpath = str(Path()) + str(Path('/logs/'))
if not os.path.exists(logpath):
    os.makedirs('logs')
now = datetime.datetime.today().strftime("%Y%m%d%H%M%S")
logfilename = Path(f'./logs/pjm_run_{now}.log').absolute().__str__()
handler = logging.FileHandler(logfilename,mode='w')

def add_module_handler(logger, level=logging.INFO)-> str:
    """Module handler for log file.
    
    :param logger: Logger object
    :type logger: :class:`logging.Logger`
    :param level: Log level to set., defaults to logging.DEBUG
    :type level: int, optional
    """
    

    if not logger.handlers:
        formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(name)s %(lineno)d %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
    logger.propagate = False
    return logfilename


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname