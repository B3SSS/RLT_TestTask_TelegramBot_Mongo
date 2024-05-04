from enum import Enum

from pydantic import BaseModel


class GroupType(Enum):
    hour = "hour"
    day = "day"
    month = "month"


class AggregateRequest(BaseModel):
    dt_from: str
    dt_upto: str
    group_type: GroupType