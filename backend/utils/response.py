def create_response(status: str, message: str = "", data: dict = None):
    if data is None:
        data = {}
    return {
        "status": status,
        "message": message,
        "data": data
    }
