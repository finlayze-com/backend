# backend/users/routes/inscodeid.py
# -*- coding: utf-8 -*-
from fastapi import APIRouter, HTTPException, Query, Request, Depends
from pydantic import BaseModel, Field
from enum import Enum
from pathlib import Path
import os, tempfile, shutil
from fastapi import status as http_status

# -- خروجی استاندارد پروژه
try:
    from backend.utils.response import create_response
except Exception:
    def create_response(message: str = "ok", data=None, status_code: int = 200):
        return {"status": "success", "status_code": status_code, "message": message, "data": data or {}}

# -- مجوز یکپارچه
try:
    from backend.users.dependencies import require_permissions
    RequirePerm = lambda: Depends(require_permissions("inscode", "ALL"))
except Exception:
    # اگر سیستم مجوز در دسترس نبود، اجازه بده ولی هشدار بده (برای dev)
    def RequirePerm():
        return None

router = APIRouter(prefix="/admin/txt", tags=["اضافه کردن سهام"])

# =========================
#  مسیر مطمئن برای Document
# =========================
HERE = Path(__file__).resolve()
try:
    BACKEND_DIR = next(p for p in HERE.parents if p.name == "backend")
except StopIteration:
    BACKEND_DIR = HERE.parents[2]

DOCUMENT_DIR = BACKEND_DIR / "Document"

# اجازه override با .env (Cross-OS)
ENV_DIR = os.getenv("TXT_EXPORT_DIR")
TXT_EXPORT_DIR = Path(ENV_DIR).resolve() if ENV_DIR else DOCUMENT_DIR.resolve()

# =========================
#  لیست فایل‌ها (دراپ‌دان)
# =========================
class ListName(str, Enum):
    fund_balanced   = "fund_balanced"
    fund_fixincome  = "fund_fixincome"
    fund_gold       = "fund_gold"
    fund_inex_stock = "fund_inex_stock"
    fund_leverage   = "fund_leverage"
    fund_other      = "fund_other"
    fund_segment    = "fund_segment"
    fund_stock      = "fund_stock"
    fund_zafran     = "fund_zafran"
    kala            = "kala"
    option          = "option"
    tamin           = "tamin"
    saham           = "saham"
    saham_2         = "saham -2"
    saham_4         = "saham -4"
    saham_R         = "saham -R"

FILE_MAP = {
    "fund_balanced":   "fund_balanced.txt",
    "fund_fixincome":  "fund_fixincome.txt",
    "fund_gold":       "fund_gold.txt",
    "fund_inex_stock": "fund_inex_stock.txt",
    "fund_leverage":   "fund_leverage.txt",
    "fund_other":      "fund_other.txt",
    "fund_segment":    "fund_segment.txt",
    "fund_stock":      "fund_stock.txt",
    "fund_zafran":     "fund_zafran.txt",
    "kala":            "kala.txt",
    "option":          "option.txt",
    "tamin":           "tamin.txt",
    "saham":           "saham.txt",
    "saham -2":        "saham -2.txt",
    "saham -4":        "saham -4.txt",
    "saham -R":        "saham -R.txt",
}

class Action(str, Enum):
    add = "add"         # append
    remove = "remove"   # حذف از فایل

class InsCodeBody(BaseModel):
    insCode: int = Field(..., description="کد یکتا (insCode)")

# ==============
#  Helpers (async)
# ==============
async def _atomic_write(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="\n")
    try:
        with tmp as f:
            f.write(content)
        shutil.move(tmp.name, str(path))
    except Exception:
        try:
            os.remove(tmp.name)
        except Exception:
            pass
        raise

async def _read_codes(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def _safe_fullpath(filename: str) -> Path:
    fullpath: Path = (TXT_EXPORT_DIR / filename).resolve()
    # جلوگیری از خروج از دایرکتوری مجاز
    if TXT_EXPORT_DIR not in fullpath.parents and fullpath.parent != TXT_EXPORT_DIR:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail="مسیر فایل نامعتبر است",
        )
    return fullpath

# ===========
#  Main Route
# ===========
@router.post(
    "/edit",
    summary="افزودن/حذف یک insCode به/از فایل txt (Query: list_name, action)"
)
async def edit_txt(
    body: InsCodeBody,
    list_name: ListName = Query(..., description="نام فایل از دراپ‌دان"),
    action: Action = Query(..., description="add یا remove"),
    _perm = RequirePerm(),  # ⛔️ نیاز به 'inscode' یا 'ALL'
    request: Request = None
):
    filename = FILE_MAP[list_name.value]
    fullpath = _safe_fullpath(filename)
    code_str = str(body.insCode).strip()

    if action == Action.add:
        # اگر فایل وجود ندارد، خودکار ساخته می‌شود
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        existing = set(await _read_codes(fullpath))
        if code_str in existing:
            # ⛔️ تکراری
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail="این کد از قبل داخل فایل وجود دارد",
            )
        # ✅ اضافه کن
        try:
            with fullpath.open("a", encoding="utf-8") as f:
                f.write(code_str + "\n")
        except Exception:
            raise

        return create_response(
            status_code=http_status.HTTP_200_OK,
            message="به فایل اضافه شد",
            data={"file": filename, "path": str(fullpath), "insCode": body.insCode},
        )

    # action == remove
    if not fullpath.exists():
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="فایل پیدا نشد",
        )

    lines = await _read_codes(fullpath)
    new_lines = [x for x in lines if x != code_str]
    if len(new_lines) == len(lines):
        # ⛔️ کد در فایل نبود
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="این کد داخل فایل نبود",
        )

    await _atomic_write(fullpath, "\n".join(new_lines) + ("\n" if new_lines else ""))

    return create_response(
        status_code=http_status.HTTP_200_OK,
        message="از فایل حذف شد",
        data={"file": filename, "path": str(fullpath), "insCode": body.insCode},
    )
