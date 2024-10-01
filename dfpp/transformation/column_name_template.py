"""store basic column name and column value conventions"""

from enum import StrEnum, Enum

dimension_canonical_names = ["sex", "year", "age", "alpha_3_code", "country_or_area", "education"]

class SexEnum(StrEnum, Enum):
    MALE = "male"
    FEMALE = "female"
    BOTH = "both"
    OTHER = "other"
    TOTAL = "total"
