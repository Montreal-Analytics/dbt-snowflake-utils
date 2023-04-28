{% macro create_udf_business_hours() %}

create or replace function {{target.schema}}.udf_business_hours(datetime1 TIMESTAMP_NTZ, datetime2 TIMESTAMP_NTZ)
returns int
language python
runtime_version = 3.8
packages = ('numpy','holidays')
handler = 'business_hours_py'
as

$$
from datetime import datetime
from datetime import timedelta
import holidays
import numpy as np

def business_hours_py(datetime1, datetime2):

    years = [*range(2000,2030)]

    US_holiday_list = list(holidays.US(years=years))
    days = np.busday_count(
        datetime1.date(),
        datetime2.date(),
        holidays = US_holiday_list)
    complete_days_hours = (days-1) * 8
    
    # Jump forward to the next non-holiday
    while datetime1 in US_holiday_list or datetime1.weekday() in [5,6]: 
        datetime1 = datetime1 + timedelta(days=1)
        datetime1 = datetime1.replace(hour=9,minute=0,second=0)

    # Jump back to the last non-holiday
    while datetime2 in US_holiday_list or datetime2.weekday() in [5,6]: 
        datetime2 = datetime2 - timedelta(days=1)
        datetime2 = datetime2.replace(hour=9,minute=0,second=0)

    # Hard coded opening and closing times
    biz_opened = datetime1.replace(hour=9, minute=0, second=0)
    biz_closed = datetime1.replace(hour=17, minute=0, second=0)

    if datetime1.date() == datetime2.date():
        return round((datetime2 - datetime1).seconds/60/60,2)
    else:
        duration_day_zero = round((biz_closed - datetime1 ).seconds/60/60, 2)
        duration_day_n = round((datetime2 - biz_opened).seconds/60/60, 2)
        return round(duration_day_zero + duration_day_n + complete_days_hours, 2)
    
$$

{% endmacro %}
