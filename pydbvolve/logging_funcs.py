# logging

import logging
import traceback
from datetime import datetime as dt

from global_settings import *

LOG_FORMAT = '%(asctime)s %(levelname)s %(migration_user)s: %(message)s'
LOG_ECHO_FORMAT = '%(asctime)s: %(message)s'


def set_log_file_name(config):
    """
    Returns a formatted log file name using values from the config (log_dir, version, migration_action) and current datetime
    """
    
    version = config['version']
    if version == LATEST_VERSION:
        version = 'latest'
    elif version == CURRENT_VERSION:
        version = 'current'
    elif version == BASELINE_VERSION:
        version = 'baseline'
    elif version == NEW_VERSION:
        version = 'new'
    
    config['log_file_name'] = os.path.join(config.get('log_dir', '.'), '.'.join((version.replace(' ', '_'), config['migration_action'].replace(' ', '_'), dt.now().strftime('%Y-%m-%d_%H:%M:%S'), 'log')))
    
    return config
# End set_log_file_name


def set_log_level(config):
    """
    Sets the default log level in the config dict. (logging.INFO)
    """
    
    config['logger_level'] = logging.INFO
    
    return config
# End set_log_level


def set_logger_name(config):
    """
    Sets the logger name in the config dict. ('pydbvovle')
    """
    
    config['logger_name'] = 'pydbvolve'
    
    return config
# End set_logger_name


def setup_error_log_handler(config):
    """
    Setup and add a logging handler specifically for logging.ERROR messages.
    """
    
    log = config.get('logger')
    if log:
		format_str = LOG_FORMAT if config.get('dry_run', False) else "DRY-RUN ** " + LOG_FORMAT
        formatter = logging.Formatter(format_str)
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(formatter)
        log_handler.setLevel(logging.WARNING)
        log.addHandler(log_handler)
    
    return config
# End setup_error_log_handler


def setup_file_logger(config):
    """
    Setup a file-based primary logger and set it in the config dict.
    Also (optionally) set a verbose handler (controlled with by config['verbose']).
    Also set a separate error handler.
    """
    
    # Pirmary logger
    format_str = LOG_FORMAT if config.get('dry_run', False) else "DRY-RUN ** " + LOG_FORMAT
    formatter = logging.Formatter(format_str)
    log_handler = logging.FileHandler(config.get('log_file_name', 'pydbvolve.log'))
    log_handler.setFormatter(formatter)
    log_handler.setLevel(config.get('log_level', logging.INFO))
    logger = logging.getLogger(config.get('logger_name', 'pydbvolve'))
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handler)
    
    # Verbosity logger
    if config.get('verbose'):
		format_str = LOG_ECHO_FORMAT if config.get('dry_run', False) else "DRY-RUN ** " + LOG_ECHO_FORMAT
        formatter = logging.Formatter(LOG_ECHO_FORMAT)
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(formatter)
        log_handler.setLevel(logging.INFO)
        logger.addHandler(log_handler)
    
    # Errors logger
    config['logger'] = logger
    setup_error_log_handler(config)
    
    return config
# End setup_file_logger


def setup_stream_logger(config):
    """
    Setup a stream-based logger and set it in the config dict.
    """
    
    format_str = LOG_FORMAT if config.get('dry_run', False) else "DRY-RUN ** " + LOG_FORMAT
    logging.basicConfig(format=format_str, level=config.get('log_level', logging.INFO))
    formatter = logging.Formatter(format_str)
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(formatter)
    log_handler.setLevel(config.get('log_level', logging.INFO))
    logger = logging.getLogger(config.get('logger_name', 'pydbvolve'))
    logger.addHandler(log_handler)
    
    config['logger'] = logger
    
    return config
# End setup_root_logger


def setup_log(config):
    """
    Sets the config for logging and creates the logger instance for pydbvolve
    """
    
    set_log_file_name(config)
    set_log_level(config)
    set_logger_name(config)
    
    if config.get('log_file_name'):
        setup_file_logger(config)
    else:
        setup_stream_logger(config)
    
    return config
# End open_log_file


def close_log(config):
    """
    Flush and close the handlers
    """
    
    log = config.get('log')
    if log:
        for h in log.handlers:
            h.flush()
            h.close()
            log.removeHandler(h)
# End close_log


def write_log(config, message, level=None):
    """
    Log the message using the config
    """
    
    log = config.get('logger')
    if log:
        log.log((level or config.get('log_level', logging.INFO)), message, extra=config)
    else:
        out = sys.stderr if level == logging.ERROR else sys.stdout
        print(message, file=out)
# End write_log


