#!/usr/bin/env python3

# psycopg2-binary
import pandas
from io import StringIO
from sqlalchemy import create_engine

f = open('P9-ConsumerComplaints.csv', 'rb')

data = []
for line in f:
    data.append(line.decode('UTF-8'))

listToStr = ' '.join(map(str, data))

c = pandas.read_csv(StringIO(listToStr), dtype={}, parse_dates=[0, 13])

c['Timely Response'] = c['Timely Response'].apply(lambda x: x == 'Yes')
c['Consumer Disputed'] = c['Consumer Disputed'].apply(lambda x: x == 'Yes')

print(c.dtypes)
# print(dir(c))
print(c)

engine = create_engine('postgresql://postgres:postgres@localhost:5432/tickets')
c.to_sql('ticket_dataset', engine, if_exists='replace')

"""create table ticket_dataset"""

"""
create index on ticket_dataset("Date Received");
"""
"""
select 
"Product Name" as product, 
count("Issue") as issues,
count(case when "Timely Response" then 1 end) as timely, 
count(case when "Consumer Disputed" then 1 end) as disputed 
from (select * from ticket_dataset 
	  where ("Date Received" >= '2013-08-04' and "Date Received" <= '2013-08-06')
	 ) as temptbl
group by "Product Name"
order by issues desc;
"""


"""
create index on ticket_dataset("Company", "State Name");
"""
"""
with x as (
	select "State Name", count("Issue") 
	from ticket_dataset 
	where "Company" like 'Ocwen' and "State Name" is not null
	group by "State Name"
), y as (
	select "State Name"
	from x
	where x.count = (select max(x.count) from x)
)
select * from ticket_dataset
where "Company" like 'Ocwen' and "State Name" like (select "State Name" from y);
"""



"""Small version"""
"""
select * from (
  select *, dense_rank() over(order by cnt desc) rn from (
    select *, count(*) over(partition by state) cnt
    from issues where company = 'ddd' and state is not null
  ) x
) x where rn = 1
"""