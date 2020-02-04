# sqlframe

Handle dataframes with pure SQL.

Data frames are one of the most pervasive data structures in data science programming.
Frames are meaninful abstractions that help to express common operations on tabular data stored in
primary memory. As such, they have become quite popular among data analysts.

For python, this trend has materialized on packages such as pandas and pyspark.

The aim of this package is manipulate in-memory data frames
with pure SQL. To this end, a simple API is developed, with familiar methods: `get_all`,
`get_one` and `execute`  that get passed an SQL query and execute it on table data..

# Advantages

1. Use well known SQL. No need to learn yet another SQL-like API
2. In-memory `SQLite3` database engine
3. High performance execution.

# Basic usage

Let us show a simple example of a product inventory

```python
    import sqlframe as sqf

    data = []
    # product_id, unit price, quantity, category
    data.append(('P1',40,100, 'A'))
    data.append(('P2',25,50, 'C'))
    data.append(('P3',1.5,0, 'B'))
    data.append(('P4',7.5,210, 'C'))
    data.append(('P5',11,0, 'C'))
    data.append(('P6',30,10, 'A'))
    data.append(('P7',41,210, 'A'))
    data.append(('P8',19.99,0, 'C'))

     # Create dataframe, alias = 't' by default
    df = sqf.DataFrame(data, columns=['product_id','price','quantity', 'category'])

    # Get all  products
    all_products = df.get_all(f'select * from t')

    # [ {'product_id': 'P1', 'price': 40., 'quantity': 100, 'category': 'A' },
    #   {'product_id': 'P2', 'price': 25., 'quantity': 50, 'category': 'C' },
    #    ...
    #   {'product_id': 'P8', 'price': 19.99, 'quantity': 0, 'category': 'C' } ]

    # Get P6
    P6 = df.get_one(f"select * from t where product_id='P6'")

    #  {'product_id': 'P6', 'price': 30., 'quantity': 10, 'category': 'A' }

    # Get max price in the inventory
    max_price = df.get_one(f'select max(price) as max_price from t')

    # {'max_price' : 41 }

    # Get all  available products
    available = df.get_all(f'select * from t where quantity > 0')

    # [ {'product_id': 'P1', 'price': 40., 'quantity': 100, 'category': 'A' },
    #   {'product_id': 'P2', 'price': 25., 'quantity': 50, 'category': 'C' },
    #    ...
    #   {'product_id': 'P7', 'price': 41., 'quantity': 210, 'category': 'A' } ]

    # Count number of available products per category (using get_dict)
    available_count = df.get_dict(f'''select category, count(1) cnt from t
                                      where quantity > 0 group by 1 order by 1''')

    # {'A': 3, 'C':2}

    sales = []
    sales.append(('P1',2))
    sales.append(('P7',4))
    sales.append(('P6',1))
    # Create a Sales dataframe with alias s
    sales_df = sqf.DataFrame(sales, columns=['product_id', 'quantity'], alias='s')

    # Obtain revenue per product from sales
    revenue = df.get_all(f"""select t.product_id, t.price * s.quantity as amount
                             from t join s on t.product_id = s.product_id""")

    # UPDATE inventory after sales
    df.execute(f'''update t set quantity = (select t.quantity - s.quantity from s
                                            where s.product_id = t.product_id)
                   where exists (select * from s where s.product_id = t.product_id)''')

    # Get new quantity of P7
    P7 = df.get_one(f"select * from t where product_id='P7'")

    # Save the updated inventory as csv
    df.to_csv('inventory.csv')

    # Save the updated inventory as parquet file
    df.to_parquet('inventory.parquet')

    # Create pandas dataframe
    import pandas as pd

    pdf = pd.DataFrame(df.get_rows('select * from t'))

    print(pdf)

```

# How to install

Download repository and move file `sqlframe.py` to your project. Take a look at `requirements.txt`

# Why ?

Inspecting the API of data frame oriented tools, one can see some convergence
toward structured query language idioms.

SQL (a well stablished language, around for more
than 40 years) expresses many of tabular operations quite efficiently, and although
not exempt from shorcomings, it is not surprising that its syntax is often a target of
table manipulation and ORMs tools.

Their level of success on this regard is a matter of debate and personal preference.
However, it is not hard to make
the case for **not** mimicking SQL. Instead, leave pure SQL to express SQL-like operations and concentrate
API  design on augmenting what is already achieved by SQL.

# About this release

1. This is an experimental version and should not be used for production.
2. Just `SQLite3` syntax is supported
3. Indexes not supported

# TODO

1. Allow other SQL engines (MySQL, PostgreSQL, Oracle, etc)
2. Improve memory comsumption
3. Support indexes
