from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi import status as http_status

def create_response(
    status: str = "success",           # success | error | fail
    message: str = "",
    data: dict = None,
    status_code: int = http_status.HTTP_200_OK
):
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
