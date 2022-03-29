import load_data_sql as sql_l
import sql_connection as sql_c
import random


def recommended_products_profile_type(profile_type):
    """"""
    sql_connection, sql_cursor = sql_c.connect()
    user_query = sql_l.all_users_profile_type_query().format(profile_type)
    sql_cursor.execute(sql_l.recommended_products_several_profiles_query().format(user_query))
    result = sql_cursor.fetchall()
    return result


if __name__ == '__main__':
    profiles = sql_l.profile_ids()
    profile_type = False

    while not profile_type:
        random_integer = random.randint(0, len(profiles))
        random_profile_id = profiles[random_integer][0]
        print(random_profile_id)

        profile_type = sql_l.fetch_profile_type(random_profile_id)
        print(profile_type)
    profile_type = profile_type[0]
    print(recommended_products_profile_type(profile_type))

