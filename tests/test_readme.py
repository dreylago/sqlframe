
import sqlframe as sqf



def test_readme():

    # Product inventory example

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

    assert set(x['product_id'] for x in all_products) ==  {'P1','P2', 'P3','P4', 'P5', 'P6', 'P7', 'P8'}

    # Get P6
    P6 = df.get_one(f"select * from t where product_id='P6'")

    assert P6['product_id'] == 'P6'

    # Get max price in the inventory
    max_price = df.get_one(f'select max(price) as max_price from t')

    assert max_price['max_price'] == 41.

    # Get all  available products
    available = df.get_all(f'select * from t where quantity > 0')

    assert set(x['product_id'] for x in available) ==  {'P1','P2', 'P4', 'P6', 'P7'}

    # Count number of available products per category (using get_dict)
    available_count = df.get_dict(f'select category, count(1) cnt from t where quantity > 0 group by 1 order by 1')

    assert available_count == {'A': 3, 'C':2}

    sales = []
    sales.append(('P1',2))
    sales.append(('P7',4))
    sales.append(('P6',1))
    # Create a Sales dataframe with alias s
    sales_df = sqf.DataFrame(sales, columns=['product_id', 'quantity'], alias='s')

    # Obtain revenue per product from sales
    revenue = df.get_all(f"""select t.product_id, t.price * s.quantity as amount from t join s on t.product_id = s.product_id""")

    for row in revenue:
        if row['product_id'] == 'P1':
            assert row['amount'] == 80.

    # UPDATE inventory after sales
    df.execute(f'''update t set quantity = (select t.quantity - s.quantity from s where s.product_id = t.product_id)
                   where exists (select * from s where s.product_id = t.product_id)''')

    # Get new quantity of P7
    P7 = df.get_one(f"select * from t where product_id='P7'")

    assert P7['quantity'] == 206

    # Save the updated inventory as csv
    df.to_csv('inventory.csv')

    # Save the updated inventory as parquet file
    df.to_parquet('inventory.parquet')

    # Create pandas dataframe
    import pandas as pd

    pdf = pd.DataFrame(df.get_all('select * from t'))

    print(pdf)


