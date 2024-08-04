class DenoKvError(BaseException):
    pass


class DenoKvValidationError(ValueError, DenoKvError):
    pass
