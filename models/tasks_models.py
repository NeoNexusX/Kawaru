import platform
from typing import Dict, List
from pydantic import BaseModel
from models.cmds_models import CMDS

# task group to deal with a series of cmds
class TASK(BaseModel):
    platform: str
    cycle: str
    trigger: str
    tasks: List[CMDS]
    activate: bool