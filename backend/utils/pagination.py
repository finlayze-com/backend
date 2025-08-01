# backend/utils/pagination.py

from math import ceil

def paginate(query, page: int = 1, size: int = 10):
    """
    صفحه‌بندی روی یک SQLAlchemy query
    """
    total = query.count()
    items = query.offset((page - 1) * size).limit(size).all()
    return {
        "items": items,
        "total": total,
        "page": page,
        "size": size,
        "pages": ceil(total / size) if size else 1
    }
