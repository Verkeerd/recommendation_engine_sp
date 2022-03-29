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
    """Fetches all profile_ids available and returns them (tuple)"""
    connection, cursor = sql_c.connect()

    cursor.execute("""SELECT profile__id FROM profiles""")
    profiles = cursor.fetchall()

    sql_c.disconnect(connection, cursor)

    return profiles


def all_users_profile_type_query():
    """
    Returns an sql query (str) to fetch all profile_ids that are of a certain product_type. The product type can be
    inserted through string formatting.
    """
    return "SELECT profile__id FROM recommendations WHERE segment = '{}'"


def fetch_profile_type(profile_id):
    """
    Takes a profile_id (str) as input. Fecthes 'segment' under preferences, when available. Otherwise, fetches
    the segments of the user's sessions and returns the segment that occurs most often.
    Returns segment (used as profile_type) (str).
    Returns None is there are no segments recorded.
    """
    connection, cursor = sql_c.connect()
    cursor.execute("""SELECT segment FROM recommendations WHERE profile__id = '{}'""".format(profile_id))
    profile_type = cursor.fetchone()
    # if profile type is not found in recommendations, looks in sessions for a profile_type
    if not profile_type:
        cursor.execute("""SELECT segment FROM sessions WHERE session__id IN (
        SELECT session__id FROM sessions WHERE buid IN (
        SELECT buid FROM buid WHERE profile__id = '{}'))""".format(profile_id))
        profile_type_possebils = cursor.fetchall()
        # loops until not empty segment is found
        while not profile_type:
            # escapes if no segments are found recorded in the sessions
            if not profile_type_possebils:
                return None
            # takes the last segment
            profile_type = profile_type_possebils[-1]
            profile_type_possebils.pop()
        # TODO Fetch modus (without counting none)

    sql_c.disconnect(connection, cursor)

    return profile_type[0]


# recommended products
def recommended_products_profile_query():
    """
    Returns an sql query (str) that fetches all products, recommended to a profile id. The profile_id can be inserted
    through string formatting.
    """
    # returns all products that are/have been recommended to this profile.
    # TODO: Filter bought (non-repeat) products
    # TODO: Filter often recommended but never bought products. Cap it? → depending on how this pans out in the final
    #  design, probably restructure the database
    return "SELECT product__id FROM recommendation_products WHERE recommendation_id =" \
           "(SELECT recommendation_id FROM recommendations WHERE profile__id = '{}')"


def recommended_products_several_profiles_query():
    """
    Returns an sql query (str) that fetches all products, recommended to several profile ids. The profile ids can be
    inserted through string formatting.
    """
    # returns all products that are/have been recommended to this profile.
    # TODO: Filter bought (non-repeat) products
    # TODO: Filter often recommended but never bought products. Cap it? → depending on how this pans out in the final
    #  design, probably restructure the database
    return "SELECT product__id FROM recommendation_products WHERE recommendation_id IN (" \
           "(SELECT recommendation_id FROM recommendations WHERE profile__id IN ({})))"
