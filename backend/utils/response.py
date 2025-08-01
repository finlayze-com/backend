from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

def create_response(status: str, message: str = "", data: dict = None, status_code: int = 200):
    if data is None:
        data = {}

    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder({
            "status": status,
            "status_code": status_code,
            "message": message,
            "data": data
        })
    )
