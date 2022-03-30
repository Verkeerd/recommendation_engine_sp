import load_data_sql as sql_l
import sql_connection as sql_c
import random


def recommended_products_profile_type(profile_type, amount=4):
    """
    Takes a profile_type (str), amount (int) as input. Fetches the product_id of all products that are recommended to
    the profiles of the given profile_type.
    Returns (input) amount of product_ids (tuple) (str).

    logical framework:

    r(x, y) = x is recommended to y (product x is recorded under recommended_products under y)
    i(x, y) = x is interesting to y (product x should be recommended to the user on the website.)
    s(x, y) = x is similar to y (profiles x and y have the same segment in recommendation)
    r = product is interesting for the customer

    [ r(x, y) → i(x, y) ∧ r(y, z)) → i(x, z) ]
    """
    sql_connection, sql_cursor = sql_c.connect()

    user_query = sql_l.all_users_profile_type_query().format(profile_type)
    sql_cursor.execute(sql_l.recommended_products_several_profiles_query().format(user_query))
    result = [product[0] for product in sql_cursor.fetchmany(amount)]

    sql_c.disconnect(sql_connection, sql_cursor)

    return result


def recommend_products_profile(profile_id, amount=4):
    """
    Takes a profile_id (str) and optionally an amount (int) as input. Fetches the user's profile_type. Then fetches
    products recommended to
    of product """
    profile_type = sql_l.fetch_profile_type(profile_id)

    return recommended_products_profile_type(profile_type, amount)


def recently_bought_products(amount=4):
    """"""
    sql_connection, sql_cursor = sql_c.connect()
    sql_cursor.execute("""SELECT op.product__id 
    FROM ordered_products op, sessions s 
    WHERE s.session__id = op.session__id 
    ORDER BY s.session_end DESC;""")
    # returns the 4 most recently sold items.
    return [product[0] for product in sql_cursor.fetchmany(amount)]


if __name__ == '__main__':
    # profiles = sql_l.profile_ids()
    # wanted_products = False
    #
    # while not wanted_products:
    #     random_integer = random.randint(0, len(profiles))
    #     random_profile_id = profiles[random_integer][0]
    #
    #     wanted_products = recommend_products_profile(random_profile_id)
    #
    # print('profile {}'.format(random_profile_id))
    #
    # print("The products I recommended are {}".format(wanted_products))
    print(recently_bought_products(10))
