import load_data_sql as sql_l
import sql_connection as sql_c
import random


def recommended_products_profile_type(profile_type, amount=4):
    """
    Takes a profile_type (str), amount (int) as input. Fetches the product_id of all products that are recommended to
    the profiles of the given profile_type.
    Returns (input) amount of product_ids (tuple) (str).

    Logical framework:

    V(x, y) = x is viewed by y
    S(x, y) = x is similar to y
    R(x, y) = x x is in the recommendation_table of y
    I(x, y) = x is interesting to y (product x should be recommended to the user on the website.)

    A product is interesting to the customer when it is in the recommendation_table
    ((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)
    A product that is interesting for a customer is also interesting for similar customers
    (I(x, c) ∧ S(c, c2)) -> I(x, c2)

    A product is interesting for to the customer when it is in the recommendation_table of similar customers
    ((((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)) ∧ S(c, c2)) -> I(y, c2)
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
    profiles = sql_l.profile_ids()
    wanted_products = False

    while not wanted_products:
        random_integer = random.randint(0, len(profiles))
        random_profile_id = profiles[random_integer][0]

        wanted_products = recommend_products_profile(random_profile_id)

    print('profile {}'.format(random_profile_id))

    print("The products I recommended are {}".format(wanted_products))
