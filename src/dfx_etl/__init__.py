"""
An ETL pipeline for managing indicator data for the Data Futures Exchange (DFx).
"""

from . import utils, validation
from .database import *
from .pipelines import *
from .storage import *

__version__ = "1.0.0"
