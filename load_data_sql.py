import sql_connection as sql_c


# products
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


# profiles
def profile_ids():
    """"""
    connection, cursor = sql_c.connect()

    cursor.execute("""SELECT profile__id FROM profiles""")
    profiles = cursor.fetchall()

    sql_c.disconnect(connection, cursor)

    return profiles


def all_users_profile_type_query():
    """"""
    return "SELECT profile__id FROM recommendations WHERE segment = '{}'"


def fetch_profile_type(profile_id):
    """"""
    connection, cursor = sql_c.connect()
    cursor.execute("""SELECT segment FROM recommendations WHERE profile__id = '{}'""".format(profile_id))
    try:
        return cursor.fetchone()
    except:
        cursor.execute("""SELECT segment FROM preferences WHERE session__id = (
    SELECT session__id FROM sessions WHERE buid = (
    SELECT buid FROM buid WHERE profile__id = '{}'))""".format(profile_id))
        try:
            return cursor.fetchall()
        except:
            return None


# recommended products
def recommended_products_profile_query():
    """"""
    # returns all products that are/have been recommended to this profile.
    # TODO: Filter bought (non-repeat) products
    # TODO: Filter often recommended but never bought products. Cap it? → depending on how this pans out in the final
    #  design, probably restructure the database
    return "SELECT product__id FROM recommendation_products WHERE recommendation_id =" \
           "(SELECT recommendation_id FROM recommendations WHERE profile__id = '{}')"



def recommended_products_several_profiles_query():
    """"""
    # returns all products that are/have been recommended to this profile.
    # TODO: Filter bought (non-repeat) products
    # TODO: Filter often recommended but never bought products. Cap it? → depending on how this pans out in the final
    #  design, probably restructure the database
    return "SELECT product__id FROM recommendation_products WHERE recommendation_id IN (" \
           "(SELECT recommendation_id FROM recommendations WHERE profile__id IN ({})))"


if __name__ == '__main__':
    pass
