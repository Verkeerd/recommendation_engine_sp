import mongo_connection as mdb_c
import sql_connection as sql_c
import transfer_functions as shared


def create_product_query():
    """
    Takes products data (dict) as input.
    Selects desired data. Creates an sql query to insert this data in the recommendation_products table.
    Returns this query.
    """
    return """

    INSERT INTO  products (product__id, product_name, brand, category, color, fast_mover, flavor, gender, 
    herhaalaankopen, recommendable, sub_category, sub_sub_category, sub_sub_sub_category, selling_price, availability, 
    discount, doelgroep, eenheid, factor, folder_actief, gebruik, geschikt_voor, geursoort, huidconditie, huidtype, 
    huidtypegezicht, inhoud, klacht, kleur, leeftijd, mid, online_only, serie, shopcart_promo_item, 
    shopcart_promo_price, soort, soort_mondverzorging, sterkte, product_type, typehaarkleuring, typetandenbostel, 
    variant, waterproof, weekdeal) 

    VALUES ({} %s);
    """.format('%s,' * 43)


def get_product_values(product):
    """"""
    result = tuple()
    wanted_fields = ['_id', 'name', 'brand', 'category', 'color', 'fast_mover', 'flavor', 'gender', 'herhaalaankopen',
                     'recommendable', 'sub_category', 'sub_sub_category', 'sub_sub_sub_category']

    wanted_price_fields = ['selling_price']

    wanted_property_fields = ['availability', 'discount', 'doelgroep', 'eenheid', 'factor', 'folder_actief', 'gebruik',
                              'geschikt_voor', 'geursoort', 'huidconditie', 'huidtype', 'huidtypegezicht', 'inhoud',
                              'klacht', 'kleur', 'leeftijd', 'mid', 'online_only', 'serie', 'shopcart_promo_item',
                              'shopcart_promo_price', 'soort', 'soort_mondverzorging', 'sterkte', 'type',
                              'typehaarkleuring', 'typetandenbostel', 'variant', 'waterproof', 'weekdeal']

    for field in wanted_fields:
        result += (shared.secure_dict_item(product, field),)
    for field in wanted_price_fields:
        result += (shared.secure_dict_item_double(product, 'price', field),)
    for field in wanted_property_fields:
        result += (shared.secure_dict_item_double(product, 'properties', field),)

    return result


def upload_product(product):
    """Takes information about a product (dict) as input. Uploads the product to the local sql database."""
    connection, cursor = sql_c.connect()
    sql_query = create_product_query()
    values_to_insert = get_product_values(product)
    cursor.execute(sql_query, values_to_insert)
    connection.commit()
    sql_c.disconnect(connection, cursor)


def upload_all_products():
    """Loads all products from the local mongodb database. Uploads the products to the local sql database."""
    database = mdb_c.connect_mdb()
    products_collection = database.products
    sql_connection, sql_cursor = sql_c.connect()
    sql_query = create_product_query()
    values_to_insert = []
    for product in products_collection.find():
        values_to_insert.append(get_product_values(product))

    sql_cursor.executemany(sql_query, values_to_insert)

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)


if __name__ == '__main__':
    upload_all_products()
