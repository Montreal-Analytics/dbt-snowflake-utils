{% macro create_udf_business_hours() %}

create or replace function {{target.schema}}.udf_business_hours(start_datetime TIMESTAMP_NTZ, end_datetime TIMESTAMP_NTZ, country STRING)
returns number
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

def business_hours_py(start_datetime, end_datetime, country='US'):

    years = [*range(1990,2030)]
    
    # Hard coded business hours, opening and closing times
    workhours_per_day = 8
    biz_opened = start_datetime.replace(hour=9, minute=0, second=0)
    biz_closed = start_datetime.replace(hour=17, minute=0, second=0)

    holiday_list = list(holidays.country_holidays(country, years=years))
    days = np.busday_count(
        start_datetime.date(),
        end_datetime.date(),
        holidays = holiday_list)
    
    # Calculate hours for full days (excluding first day and last day)
    complete_days_hours = (days - 2) * workhours_per_day

    # Jump forward to the next non-holiday
    while start_datetime.date() in holiday_list or start_datetime.weekday() in [5,6]: 
        start_datetime = start_datetime + timedelta(days=1)
        start_datetime = start_datetime.replace(hour=9,minute=0,second=0)

    # Jump back to the last non-holiday
    while end_datetime.date() in holiday_list or end_datetime.weekday() in [5,6]: 
        end_datetime = end_datetime - timedelta(days=1)
        end_datetime = end_datetime.replace(hour=17,minute=0,second=0)
        
    if start_datetime.date() == end_datetime.date():
        return round((end_datetime - start_datetime).seconds/60/60,2)
    else:
        duration_day_zero = round((biz_closed - start_datetime ).seconds/60/60, 2)
        duration_day_n = round((end_datetime - biz_opened).seconds/60/60, 2)
        return round(duration_day_zero + duration_day_n + complete_days_hours, 2)

$$

{% endmacro %}
