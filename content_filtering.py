import sql_connection as sql_c
import load_data_sql as sql_l
import random


def recommend_products(profile_id, amount=4):
    """
    Takes a profile_id (str), amount (int) as input. Fetches the product_id of all products that have been recommended
    to this user.
    Returns the products ids (tuple) (str).
    logical framework:

    V(x, y) = x is viewed y
    S(x, y) = x is similar to y
    R(x, y) = x is in the recommendation_table of y
    I(x, y) = x is interesting to y

    A product is in the recommendation_table when it has been viewed before
    V(x, c) -> R(x, c)
    A product is in the recommendation_table when it is similar to a product that had been viewed before
    (V(x, c) ∧ S(x, y)) -> R(y, c)

    A product is interesting to the customer when it is in the recommendation_table
    ((V(x, c) ∧ S(x, y)) ∨ V(y, c)) -> R(y, c) -> I(y, c)
    """
    sql_connection, sql_cursor = sql_c.connect()
    sql_query = sql_l.recommended_products_profile_query()

    sql_cursor.execute(sql_query.format(profile_id))
    products = sql_cursor.fetchmany(amount)

    sql_c.disconnect(sql_connection, sql_cursor)

    return products


if __name__ == '__main__':
    profiles = sql_l.profile_ids()
    recommended_products = False

    while not recommended_products:
        random_integer = random.randint(0, len(profiles))
        random_profile_id = profiles[random_integer][0]
        recommended_products = recommend_products(random_profile_id)

    print('profile {}'.format(random_profile_id))
    if len(recommended_products) >= 10:
        print("The products I recommended are {}".format(recommended_products[:10]))
    else:
        print("The products I recommended are {}".format(recommended_products))

