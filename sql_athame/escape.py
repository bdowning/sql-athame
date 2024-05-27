import math
import uuid
from collections.abc import Sequence
from typing import Any


def escape(value: Any) -> str:
    if isinstance(value, str):
        return f"E{value!r}"
    elif isinstance(value, float) or isinstance(value, int):
        if math.isnan(value):
            raise ValueError("Can't escape NaN float")
        elif math.isinf(value):
            raise ValueError("Can't escape infinite float")
        return f"{value!r}"
    elif isinstance(value, uuid.UUID):
        return f"{str(value)!r}::UUID"
    elif isinstance(value, Sequence):
        args = ", ".join(escape(x) for x in value)
        return f"ARRAY[{args}]"
    elif value is None:
        return "NULL"
    else:
        raise TypeError(f"Can't escape type {type(value)}")
