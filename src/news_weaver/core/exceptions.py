class CustomException(Exception):
    """Base class for other exceptions."""
    pass


class NotFoundError(CustomException):
    """Exception raised for not found errors."""
    def __init__(self, message='Not Found'):
        self.message = message
        super().__init__(self.message)


class ValidationError(CustomException):
    """Exception raised for validation errors."""
    def __init__(self, message='Validation Error'):
        self.message = message
        super().__init__(self.message)


class AuthenticationError(CustomException):
    """Exception raised for authentication errors."""
    def __init__(self, message='Authentication Failed'):
        self.message = message
        super().__init__(self.message)