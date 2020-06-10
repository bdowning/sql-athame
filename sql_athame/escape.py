import math
import uuid
from typing import Any, Sequence


def escape(value: Any):
    if isinstance(value, str):
        return f"E{repr(value)}"
    elif isinstance(value, float) or isinstance(value, int):
        if math.isnan(value):
            raise ValueError("Can't escape NaN float")
        elif math.isinf(value):
            raise ValueError("Can't escape infinite float")
        return f"{repr(value)}"
    elif isinstance(value, uuid.UUID):
        return f"{repr(str(value))}::UUID"
    elif isinstance(value, Sequence):
        args = ", ".join(escape(x) for x in value)
        return f"ARRAY[{args}]"
