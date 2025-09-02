from fastapi import HTTPException

class AppException(HTTPException):
    """برای خطاهای بیزنسی با پیام و دادهٔ استاندارد"""
    def __init__(self, status_code: int, message: str, data: dict | None = None, errors: list | None = None):
        super().__init__(status_code=status_code, detail=message)
        self.message = message
        self.data = data or {}
        self.errors = errors or []
