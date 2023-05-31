{% macro create_udf_business_hours() %}

create or replace function {{target.schema}}.udf_business_hours(start_datetime TIMESTAMP_NTZ, end_datetime TIMESTAMP_NTZ, weekmask STRING, country STRING)
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

def business_hours_py(start_datetime, end_datetime, weekmask = '1111100', country='US'):

    # Year range for holidays spanning the years below
    
    years = [*range(1990,2030)]
    
    # Hard coded business hours, opening and closing times

    opening_hour = 9
    closing_hour = 17
    workhours_per_day = closing_hour-opening_hour
    omitted_dow = []
    
    # Create list of omitted days of week for exclusion of hourly calcs from first and last days of date range
    
    for count, i in enumerate(weekmask):
        if i=='0':
            omitted_dow.append(count)

    # Create open and closing datetimes to establish day 0 and day n durations

    first_day_closed = start_datetime.replace(hour=closing_hour, minute=0, second=0)
    last_day_opened = end_datetime.replace(hour=opening_hour, minute=0, second=0)

    holiday_list = list(holidays.country_holidays(country, years=years))
    days = np.busday_count(
        start_datetime.date(),
        end_datetime.date(),
        weekmask = weekmask,
        holidays = holiday_list)
    
    # Calculate hours for full business days (excluding first day, last day is automatically excluded in busday_count)
    
    complete_days_hours = (days - 1) * workhours_per_day

    # Jump forward to the next non-holiday
    # When start_datetime is iterated beyond end_datetime, this indicates entirety of hours occuring during holidays or
    # weekend, therefore return 0 via max(0,business_hours). 

    while start_datetime.date() in holiday_list or start_datetime.weekday() in omitted_dow: 
        start_datetime = start_datetime.replace(hour=opening_hour,minute=0,second=0) + timedelta(days=1)

    if start_datetime.date() == end_datetime.date():
        business_hours = round((end_datetime - start_datetime).seconds/60/60,2)
        return max(0, business_hours)
    else:
        duration_day_zero = round((first_day_closed - start_datetime ).seconds/60/60, 2)

        if end_datetime.date() in holiday_list or end_datetime.weekday() in omitted_dow: 
            # If end_datetime falls on a holiday or weekend, the previous full day is accounted for in 
            # complete_day_hours calc so early return just duration_day_zero and complete_day_hours
            business_hours = round(duration_day_zero + complete_days_hours, 2)
            return max(0,business_hours)
        else:
            # If end_datetime does not fall on holiday or weekend, calculate last day's working hours 
            # and return total sum
            duration_day_n = (end_datetime - last_day_opened).seconds/60/60
            business_hours = round(duration_day_zero + complete_days_hours + duration_day_n, 2)
            return max(0, business_hours)

$$

{% endmacro %}
