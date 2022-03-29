import sql_connection as sql_c
import load_data_sql as sql_l
import random


def recommend_products(profile_id):
    """
    Takes a profile_id as input. fetches the product_id of all products that have been recommended to this user.
    Returns the products ids (tuple).
    """
    sql_connection, sql_cursor = sql_c.connect()
    sql_query = sql_l.recommended_products_profile_query()

    sql_cursor.execute(sql_query.format(profile_id))
    products = sql_cursor.fetchall()

    sql_c.disconnect(sql_connection, sql_cursor)

    return products


if __name__ == '__main__':
    profiles = sql_l.profile_ids()
    recommended_products = False

    while not recommended_products:
        random_integer = random.randint(0, len(profiles))
        random_profile_id = profiles[random_integer][0]
        print(random_profile_id)

        recommended_products = recommend_products(random_profile_id)
        print(recommended_products)

