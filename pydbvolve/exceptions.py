# Exceptions


NO_ERROR = 0


class MigrationBaseError(Exception):
    code = 1
    
    @property
    def code(self):
        return self._code
# End MigrationBaseError


class EmergencyCleanExit(MigrationBaseError):
    code = 0
# End EmergencyExit


class MigrationArgumentError(MigrationBaseError):
    pass
# End MigrationArgumentError


class MigrationTableOutOfSync(MigrationBaseError):
    code = 2
# End MigrationTableOutOfSync


class MigrationTableConstraintError(MigrationBaseError):
    code = 3
# End MigrationTableConstraintError


class MigrationTableManagementError(MigrationBaseError):
    code = 4
# End MigrationTableManagementError


class MigrationExecutionError(MigrationBaseError):
    code = 5
# End MigrationExecutionError


class MigrationNotFound(MigrationBaseError):
    code = 6
# End MigrationNotFound


class MigrationFileTypeError(MigrationBaseError):
    code = 7
# End MigrationNotFound


class MigrationVerifyAppliedError(MigrationBaseError):
    code = 8
# End MigrationNotFound


def exception_code(exc, code=-255):
    import inspect
    
    klass = exc if inspect.isclass(exc) else type(exc)
    
    class MigrationException(klass):
        def __init__(self, code, *args):
            super().__init__(*args)
            self.code = code
        
        def __str__(self):
            return "{}: {} ({})".format(klass.__name__, super().__str__(), self.code)
    
    return MigrationException(getattr(exc, 'code', code), *exc.args)
# End exception_code

