from typing import TYPE_CHECKING, Any, Dict, List

from .types import FlatPart, Placeholder, Slot

try:
    from sqlalchemy.sql import bindparam, text
    from sqlalchemy.sql.elements import BindParameter

    def sqlalchemy_text_from_fragment(self: "Fragment") -> Any:
        parts: List[FlatPart] = []
        self.flatten_into(parts)
        bindparams: Dict[str, Any] = {}
        out_parts: List[str] = []
        for part in parts:
            if isinstance(part, Slot):
                out_parts.append(f"(:{part.name})")
            elif isinstance(part, Placeholder):
                key = f"_arg_{part.name}_{id(part)}"
                if isinstance(part.value, BindParameter):
                    bindparams[key] = bindparam(key, part.value.value, part.value.type)
                else:
                    bindparams[key] = bindparam(key, part.value)
                out_parts.append(f"(:{key})")
            else:
                assert isinstance(part, str)
                out_parts.append(part)
        query = "".join(out_parts).strip()
        return text(query).bindparams(*bindparams.values())

except ImportError:

    def sqlalchemy_text_from_fragment(self: "Fragment") -> Any:
        raise ImportError("No sqlalchemy installed")


if TYPE_CHECKING:
    from .base import Fragment
