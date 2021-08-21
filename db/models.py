from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator
from enum import Enum, IntEnum
from tortoise.models import Model

from models.UserModel import User_Pydantic, UserIn_Pydantic, User
from models.PreviousYearQuestionPapers import prevYearQuestPaper,prevYearQuestPaper_Pydantic,prevYearQuestPaperIn_Pydantic
from models.LeadershipBoard import LeadershipBoardIn_Pydantic,LeadershipBoard_Pydantic,LeadershipBoard

