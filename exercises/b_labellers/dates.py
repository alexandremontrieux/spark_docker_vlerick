import datetime


from pyspark.sql import DataFrame
from pyspark.sql.functions import dayofyear
import holidays
be_holidays = holidays.country_holidays('BE')


def is_belgian_holiday(date: datetime.date) -> bool:
    pass


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    return frame("is_weekend", dayofweek("execution_date").isin([1,7]))
   
    pass


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    return frame(new_colname, colname.isin(be_holidays()))
    
    
    pass


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass
