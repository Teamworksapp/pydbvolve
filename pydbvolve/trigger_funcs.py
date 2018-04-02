# triggers


def pre_execution(config):
    """
    Called after initialization but before processing. Accepts the config dict as the sole argument.
    Overide this function in your config file for custom action.
    """
    
    return None
# End pre_execution


def post_execution(config):
    """
    Called after processing but before exit. Accepts the config dict as the sole argument.
    Overide this function in your config file for custom action.
    """
    
    return None
# End post_execution


def pre_script(config, migration):
    """
    Called after script is resolved, but before execution. Accepts config dict and migration dict as args.
    Overide this function in your config file for custom action.
    """
    
    return None
# End pre_script


def post_script(config, migration):
    """
    Called after script is executed. Accepts config dict and migration dict as args.
    Overide this function in your config file for custom action.
    """
    
    return None
# End post_script


def pre_statement(config, migration, statement):
    """
    Called on SQL migrations only. Called before individual statement execution.
    Accepts config dict, migration dict, and statement string as arguments.
    Overide this function in your config file for custom action.
    """
    
    return None
# End pre_statement


def post_statement(config, migration, statement, cursor):
    """
    Called on SQL migrations only. Called after individual statement execution.
    Accepts config dict, migration dict, and statement string as arguments.
    Overide this function in your config file for custom action.
    """
    
    return None
# End post_statement


