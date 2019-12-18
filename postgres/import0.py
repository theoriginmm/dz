#!/usr/bin/env python3

# psycopg2-binary
import pandas
from io import StringIO
from sqlalchemy import create_engine

f = open('movie_metadata.csv', 'rb')

data = []
for line in f:
    data.append(line.decode('UTF-8'))

listToStr = ' '.join(map(str, data))

c = pandas.read_csv(StringIO(listToStr), usecols=['genres', 'actor_1_name', 'movie_title', 'imdb_score'], sep=',', converters={'genres': str})
c['genres'] = c['genres'].apply(lambda x: x.split('|'))

print(c.dtypes)
print(c['genres'].tail(10))
print(dir(c))

engine = create_engine('postgresql://postgres:postgres@localhost:5432/movies')

with engine.connect() as con:
    con.execute("""
        DROP TABLE IF EXISTS public.movie_dataset;
        CREATE TABLE public.movie_dataset
        (
            index int8,
            genres _varchar,
            actor_1_name varchar,
            movie_title varchar,
            imdb_score float8
        );
        """)

c.to_sql('movie_dataset', engine, if_exists='append')

"""
CREATE OR REPLACE FUNCTION make_tsvector(movie_title TEXT, genres TEXT)
   RETURNS tsvector AS $$
BEGIN
  RETURN (to_tsvector('english', movie_title) ||
    to_tsvector('english', genres));
END
$$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE INDEX IF NOT EXISTS igx_gin ON movie_dataset
  USING gin(make_tsvector(movie_title, genres));


select * 
from movie_dataset 
where make_tsvector(movie_title, genres) @@ plainto_tsquery('spi crime') 
order by imdb_score desc;
"""



'''
select * 
from movie_dataset 
where to_tsvector(movie_title) @@ plainto_tsquery('spi') 
and genres @> '{Crime}'::varchar[] 
order by imdb_score desc;
'''