from faker import Faker
import pandas as pd
from collections import Counter
import sqlframe as sf
from uuid import uuid4

ps = dict()

ps['logging format'] = 'pid %(process)5s [%(asctime)s] ' + \
                       '%(filename)12.12s:%(lineno)4d ' + \
                       '%(levelname)5.5s: %(message)s'


#------------------------------------------------------------------------------#

def test_create():

    faker = Faker()
    data = []
    for i in range(50):
        data.append(dict(name=faker.name(), yob=faker.random_int(1910,1999), height=faker.random_int(150,190)))

    df = sf.DataFrame(data)
    assert df != None

#------------------------------------------------------------------------------#

def test_get_all():

    faker = Faker()

    data = []
    for i in range(50):
        data.append(dict(name=faker.name(), yob=faker.random_int(1930,2001), height=faker.random_int(150,195)))

    df = sf.DataFrame(data, alias='t')

    tall = df.get_all('select * from t where t.height >= 180')

    assert len(tall) <= len(data)

    if len(tall) > 0:
        assert isinstance(tall[0], dict)
        assert set(tall[0].keys()) == {'name','yob', 'height'}

#------------------------------------------------------------------------------#

def test_len():

    faker = Faker()
    data = []
    for i in range(50):
        data.append(dict(name=faker.name(), yob=faker.random_int(1910,1999), height=faker.random_int(150,190)))

    df = sf.DataFrame(data, alias='t')

    cnt = len(df)

    assert isinstance(cnt, int)
    assert cnt == len(data)

#------------------------------------------------------------------------------#

def test_execute():

    faker = Faker()

    data = []
    for i in range(50):
        data.append(dict(name=faker.name(), yob=faker.random_int(1910,1999), height=faker.random_int(150,190)))

    df = sf.DataFrame(data, alias='t')

    assert len(df) == len(data)

    rows = df.get_all(f"select * from t where name = 'Mary Knuth'")

    assert len(rows) == 0

    df.execute(f"insert into t (name, yob, height) values ('Mary Knuth',1990, 178)")

    assert len(df) == len(data) + 1

    rows = df.get_all(f"select * from t where name = 'Mary Knuth' and yob = 1990")

    assert len(rows) == 1


#------------------------------------------------------------------------------#

def test_iter():

    faker = Faker()

    data = []
    for i in range(50):
        data.append(dict(name=faker.name(), yob=faker.random_int(1910,1999), height=faker.random_int(150,190)))

    df = sf.DataFrame(data, alias='t')

    tall1 = df.get_all('select * from t where t.height >= 180')

    tall2 = []
    for row in df.get_iter('select * from t where t.height >= 180'):
        tall2.append(dict(row))

    sorted1 = sorted(tall1, key=lambda x: (x['height'], x['yob'], x['name']))
    sorted2 = sorted(tall2, key=lambda x: (x['height'], x['yob'], x['name']))

    assert sorted1 == sorted2

#------------------------------------------------------------------------------#

def test_to_dict():

    faker = Faker()

    data = []
    for i in range(50):
        year = faker.random_element([1999,2000,2001])
        data.append(dict(name=faker.name(), yob=year, height=faker.random_int(150,190)))

    df = sf.DataFrame(data, alias='t')

    counts = df.get_dict('select yob, count(1) as cnt from t group by 1')

    assert set(counts.keys()) == {1999, 2000, 2001}

    assert sum(x for x in counts.values()) == len(data)

    data = []
    for i in range(50):
        year = faker.random_element([1999,2000,2001])
        city = faker.random_element(['London', 'Madrid', 'Caracas', 'Lisboa'])
        data.append(dict(name=faker.name(), birth=year, city=city))

    df = sf.DataFrame(data, alias='t')

    counts = df.get_dict('select city, birth, count(1) as cnt from t group by 1, 2')

    assert set(counts.keys()) == set(['London', 'Madrid', 'Caracas', 'Lisboa'])

    Sum = 0
    for city, years in counts.items():
        for year, x in years.items():
            Sum += x

    assert Sum == len(data)

    data = []
    for i in range(50):
        year = faker.random_element([1999,2000,2001])
        city = faker.random_element(['London', 'Madrid', 'Caracas', 'Lisboa'])
        data.append(dict(name=faker.name(), birth=year, city=city))

    df = sf.DataFrame(data, alias='t')

    lists = df.get_dict('select city, group_concat(name) as names from t group by 1')

    assert set(lists.keys()) == set(['London', 'Madrid', 'Caracas', 'Lisboa'])

    Sum = 0
    for city, x in lists.items():
        Sum += len(x.split(','))

    assert Sum == len(data)

#------------------------------------------------------------------------------#

def test_parquet():

    faker = Faker()

    data = []
    for i in range(50):
        year = faker.random_element([1999,2000,2001])
        data.append(dict(name=faker.name(), yob=year, height=faker.random_int(150,190)))

    df = pd.DataFrame(data)

    fn = f'/tmp/sqlframe.{uuid4()}.parquet'

    df.to_parquet(fn)

    rows1 = list(dict(row) for __, row in df.iterrows())

    df2 = sf.read_parquet(fn, alias='t')

    assert len(data) == len(df) == len(df2)

    rows2 = df2.get_all(f'select * from t')

    print(rows1)
    print(rows2)
    assert rows1 == rows2
