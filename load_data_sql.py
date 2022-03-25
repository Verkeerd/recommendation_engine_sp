import sql_connection as sql_c


def all_product_prices():
    """Fetches the price of all products in the sql web-shop database. Returns these values (list) [(int)]."""
    sql_query = """SELECT selling_price FROM products"""
    # connect
    connection, cursor = sql_c.connect()

    cursor.execute(sql_query)
    # The sql query returns a list with tuples containing the price. For each item in the list we select the first index
    # after that we are left with a list with all prices.
    products = [product[0] for product in cursor.fetchall()]
    sql_c.disconnect(connection, cursor)

    return list(products)


def name_price_products():
    """
    Fetches the price and the product name of all products in the sql web-shop database. Returns these values (list)
    [(int, str)].
    """
    sql_query = """SELECT selling_price, product_name FROM products"""
    connection, cursor = sql_c.connect()
    cursor.execute(sql_query)
    products = cursor.fetchall()

    sql_c.disconnect(connection, cursor)

    return products


if __name__ == '__main__':
    print(name_price_products())
