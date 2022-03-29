import mongo_connection as mdb_c
import sql_connection as sql_c

# TODO: make 1 big sql statement and execute it in one go.


def secure_value(value):
    """Turns empty strings and integers (0 and "0") into None. Returns all other values unchanged."""
    if value == 0:
        return None
    if value == '0':
        return None
    return value


def create_product_query(product):
    """
    Takes products data (dict) as input.
    Selects desired data. Creates an sql query to insert this data in the recommendation_products table.
    Returns this query.
    """
    sql_query_format = 'INSERT INTO  products ({}) VALUES ({})'

    fields_to_insert = ''

    # field names that are different between mongodb and sql.
    bad_values = ['_id', 'name', 'type']
    # wanted subdirectories
    cat_keys = ['price', 'properties']
    # wanted fields
    wanted_keys = ['_id', 'brand', 'category', 'color', 'fast_mover', 'flavor', 'gender', 'herhaalaankopen',
            'name', 'selling_price', 'availability', 'discount', 'doelgroep', 'eenheid', 'factor', 'folder_actief',
            'gebruik', 'geschikt_voor', 'geursoort', 'huidconditie', 'huidtype', 'huidtypegezicht', 'inhoud', 'klacht',
            'kleur', 'leeftijd', 'mid', 'online_only', 'serie', 'shopcart_promo_item', 'shopcart_promo_price', 'soort',
            'soort_mondverzorging', 'sterkte', 'type', 'typehaarkleuring', 'typetandenbostel', 'variant', 'waterproof',
            'weekdeal', 'recommendable', 'sub_category', 'sub_sub_category', 'sub_sub_sub_category']

    wanted_values = ()

    for key, value in product.items():
        # check if the field is a wanted subdirectory
        if key in cat_keys:
            # checks all keys and values in the subdirectory
            for sub_key, sub_value in value.items():
                if sub_key in wanted_keys:
                    # filters out the discounted price in the price subdirectory. Discount in 'properties' is recorded.
                    if not (key == 'price' and sub_key == 'discount'):
                        # changes the column name when the sql_field is named differently than the mdb field
                        if sub_key in bad_values:
                            fields_to_insert += 'product_{}, '.format(sub_key)
                        else:
                            fields_to_insert += '{}, '.format(sub_key)
                        wanted_values += (secure_value(sub_value),)
        elif key in wanted_keys:
            # changes the column name when the sql_field is named differently than the mdb field
            if key in bad_values:
                fields_to_insert += 'product_{}, '.format(key)
            else:
                fields_to_insert += '{}, '.format(key)
            wanted_values += (secure_value(value),)

    sql_query = sql_query_format.format(fields_to_insert[:-2], ('%s, ' * len(wanted_values))[:-2])

    return sql_query, wanted_values


def upload_product(product):
    """Takes information about a product (dict) as input. Uploads the product to the local sql database."""
    connection, cursor = sql_c.connect()
    sql_query = create_product_query(product)
    cursor.execute(sql_query)
    connection.commit()
    sql_c.disconnect(connection, cursor)


def upload_all_products():
    """Loads all products from the local mongodb database. Uploads the products to the local sql database."""
    database = mdb_c.connect_mdb()
    products_collection = database.products
    sql_connection, sql_cursor = sql_c.connect()
    for product in products_collection.find():
        product = dict(product)
        sql_query, format_values = create_product_query(product)
        sql_cursor.execute(sql_query, format_values)

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)


if __name__ == '__main__':
    upload_all_products()
