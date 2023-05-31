{% macro create_udf_business_days() %}

create or replace function {{target.schema}}.udf_business_days(date1 TIMESTAMP_NTZ, date2 TIMESTAMP_NTZ, country STRING)
returns int
language python
runtime_version = 3.8
packages = ('numpy','holidays')
handler = 'business_days_py'
as

$$
import numpy as np
import holidays as holidays

def business_days_py(start_date, end_date, weekmask='1111100', country='US'):

    # calculate business days between two dates
    start_date = start_date.date()
    end_date = end_date.date()
    years = [*range(1990,2030)]

    holiday_list = list(holidays.country_holidays(country, years=years))

    return np.busday_count(
        start_date, 
        end_date, 
        weekmask = weekmask, 
        holidays = holiday_list
        )
    
$$

{% endmacro %}
