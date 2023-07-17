{% macro create_udf_business_days() %}

create or replace function {{target_schema}}.udf_business_days(start_datetime DATE, end_datetime DATE, weekmask STRING, country STRING)
returns int
language python
runtime_version = 3.8
packages = ('numpy','holidays')
handler = 'business_days_py'
as

$$
import numpy as np
import holidays as holidays

def business_days_py(start_date, end_date, weekmask, country):
    # calculate business days between two dates
    years = [*range(1990,2030)]

    holiday_list = list(holidays.country_holidays(country, years=years))

    return np.busday_count(
        start_date, 
        end_date, 
        weekmask = weekmask, 
        holidays = holiday_list
        )
    
$$

-- overload file to default weekmask as Monday to Friday (weekmask='1111100')
create or replace function {{target_schema}}.udf_business_days(start_datetime DATE, end_datetime DATE, country STRING)
returns int
language python
runtime_version = 3.8
packages = ('numpy','holidays')
handler = 'business_days_py'
as

$$
import numpy as np
import holidays as holidays

def business_days_py(start_date, end_date, country):

    # calculate business days between two dates
    years = [*range(1990,2030)]
    weekmask='1111100'

    holiday_list = list(holidays.country_holidays(country, years=years))

    return np.busday_count(
        start_date, 
        end_date, 
        weekmask = weekmask, 
        holidays = holiday_list
        )
    
$$

-- finally, overload function to default weekmask as M-F and country as 'US'
create or replace function {{target_schema}}.udf_business_days(start_datetime DATE, end_datetime DATE)
returns int
language python
runtime_version = 3.8
packages = ('numpy','holidays')
handler = 'business_days_py'
as

$$
import numpy as np
import holidays as holidays

def business_days_py(start_date, end_date):

    # calculate business days between two dates
    years = [*range(1990,2030)]
    weekmask='1111100'
    country='US'

    holiday_list = list(holidays.country_holidays(country, years=years))

    return np.busday_count(
        start_date, 
        end_date, 
        weekmask = weekmask, 
        holidays = holiday_list
        )
    
$$

{% endmacro %}
