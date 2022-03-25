import mongo_connection as mdb_c
import sql_connection as sql_c


def secure_value(value):
    """Turns empty strings and integers (0 and "0") into None. Returns all other values unchanged."""
    if value == 0:
        return None
    if value == '0':
        return None
    return value


def create_big_product_query(product):
    """"""
    sql_query_ip = 'INSERT INTO  products ( '

    bad_values = ['_id', 'name', 'type']
    cat_keys = ['price', 'properties']
    wanted_keys = ['_id', 'brand', 'category', 'color', 'fast_mover', 'flavor', 'gender', 'herhaalaankopen',
            'name', 'selling_price', 'availability', 'discount', 'doelgroep', 'eenheid', 'factor', 'folder_actief',
            'gebruik', 'geschikt_voor', 'geursoort', 'huidconditie', 'huidtype', 'huidtypegezicht', 'inhoud', 'klacht',
            'kleur', 'leeftijd', 'mid', 'online_only', 'serie', 'shopcart_promo_item', 'shopcart_promo_price', 'soort',
            'soort_mondverzorging', 'sterkte', 'type', 'typehaarkleuring', 'typetandenbostel', 'variant', 'waterproof',
            'weekdeal', 'recommendable', 'sub_category', 'sub_sub_category', 'sub_sub_sub_category']

    wanted_values = ()

    for key, value in product.items():
        # check if the field is a wanted sub_directory
        if key in cat_keys:
            # checks all keys and values in the sub_directory
            for sub_key, sub_value in value.items():
                if sub_key in wanted_keys:
                    # filters out the discounted price in the price sub-directory. Discount in 'properties' is recorded.
                    if not (key == 'price' and sub_key == 'discount'):
                        # changes the column name when the sql_field is named differently than the mdb field
                        if sub_key in bad_values:
                            sql_query_ip += 'product_{}, '.format(sub_key)
                        else:
                            sql_query_ip += '{}, '.format(sub_key)
                        wanted_values += (secure_value(sub_value),)
        elif key in wanted_keys:
            # changes the column name when the sql_field is named differently than the mdb field
            if key in bad_values:
                sql_query_ip += 'product_{}, '.format(key)
            else:
                sql_query_ip += '{}, '.format(key)
            wanted_values += (secure_value(value),)

    sql_query_vp = 'VALUES (' + '%s, ' * len(wanted_values)

    sql_query = sql_query_ip[:-2] + ')' + sql_query_vp[:-2] + ')' + 'ON CONFLICT DO NOTHING'
    print(product['price']['selling_price'], product['name'])
    print(wanted_values)
    print(sql_query)
    return sql_query, wanted_values


def create_product_query(product):
    """
    Takes information about a product (dict) as input. Constructs a sql query to insert this product with name, id and
    price into a sql database. Returns the constructed sql query (str).
    """
    sql_query = 'INSERT INTO products (product__id, product_name, selling_price) VALUES (%s, %s, %s)'

    values = (product['_id'], product['name'], product['price']['selling_price'])

    return sql_query, values


def upload_product(product):
    """
    Takes information about a product (dict) as input. Uploads the product to the local sql database. Uploads the
    product id, the name and the price.
    """
    connection, cursor = sql_c.connect()
    sql_query = create_big_product_query(product)
    cursor.execute(sql_query)
    connection.commit()
    sql_c.disconnect(connection, cursor)


def upload_all_products():
    """Loads all products from the local mongodb database."""
    database = mdb_c.connect_mdb()
    products_collection = database.products
    sql_connection, sql_cursor = sql_c.connect()
    for product in list(products_collection.find()):
        sql_query, format_values = create_big_product_query(product)
        sql_cursor.execute(sql_query, format_values)
        print(sql_query, format_values)

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)
