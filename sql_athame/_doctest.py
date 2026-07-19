import uuid
from dataclasses import dataclass
from typing import Any

from .dataclasses import ModelBase

USER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
USER_ID_2 = uuid.UUID("00000000-0000-0000-0000-000000000002")
USER_ID_3 = uuid.UUID("00000000-0000-0000-0000-000000000003")


@dataclass
class User(ModelBase, table_name="users", primary_key="id"):
    id: uuid.UUID
    name: str
    email: str | None


@dataclass
class InsertUser(ModelBase, table_name="users"):
    name: str
    email: str | None = None


def user_row(
    *,
    user_id: uuid.UUID = USER_ID,
    name: str = "Alice",
    email: str | None = None,
) -> dict[str, Any]:
    return {
        "id": user_id,
        "name": name,
        "email": email,
    }
