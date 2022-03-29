import load_data_sql as sql_l
import sql_connection as sql_c
import random


def recommended_products_profile_type(profile_type):
    """
    Takes a profile_type as input. Fetches the product_id of all products that are recommended to the profiles of the
    given profile_type.
    Returns the product_ids (list) [str].
    """
    sql_connection, sql_cursor = sql_c.connect()

    user_query = sql_l.all_users_profile_type_query().format(profile_type)
    sql_cursor.execute(sql_l.recommended_products_several_profiles_query().format(user_query))
    result = sql_cursor.fetchall()

    sql_c.disconnect(sql_connection, sql_cursor)

    return result


if __name__ == '__main__':
    profiles = sql_l.profile_ids()
    wanted_profile_type = False

    while not wanted_profile_type:
        random_integer = random.randint(0, len(profiles))
        random_profile_id = profiles[random_integer][0]

        wanted_profile_type = sql_l.fetch_profile_type(random_profile_id)

    print('profile {}'.format(random_profile_id))
    print(wanted_profile_type, type(wanted_profile_type))

    recommended_products = recommended_products_profile_type(wanted_profile_type)
    print("The products I recommended are {}".format(recommended_products_profile_type(wanted_profile_type)[:10]))
