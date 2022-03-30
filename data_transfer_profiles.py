import mongo_connection as mdb_c
import sql_connection as sql_c
import transfer_functions as shared


# queries
def create_profile_query():
    """
    Takes profile-data (dict) as input.
    Creates an sql query to insert the following data into the profile table:
    - profile id
    - created data
    - first order
    - the latest order
    - the latest activity
    returns this query (str).
    """
    return """INSERT INTO profiles (profile__id, created, latest_activity, latest_order, first_order) 
    VALUES (%s, %s, %s, %s, %s);"""


def create_buid_query():
    """
    Takes a profile_id (str) and a buid (str) as input.
    Creates an sql query to insert this data in the buid table and returns this query.
    """
    return """INSERT INTO buid (buid, profile__id) 
    VALUES (%s, %s) ON CONFLICT ON CONSTRAINT c_b_pk DO NOTHING;"""


def create_recommendation_query():
    """
    Takes profile_data (dict) and a recommendation_id (str) as input.
    Selects the following data from the profile:
    - profile_id
    - recommendation time
    - segment
    - last visit
    - total pageviewcount
    - total viewed count
    Creates an sql query to insert this data in the recommendation table and returns this query.
    """
    return """INSERT INTO recommendations (recommendation_id, profile__id, recommendation_time, segment, 
    last_visit, total_pageview_count, total_viewed_count) 
    VALUES (%s, %s, %s, %s, %s, %s, %s);"""


def create_recommendation_product_query():
    """
    Takes a recommendation_id (int) and, product__id (str) and a recommendation_type (str) as input.
    Creates an sql query to insert this data in the recommendation_products table and returns this query.
    """
    # source where exists/for share clause: Erwin Brandstetter
    # https://dba.stackexchange.com/questions/252875/how-to-make-on-conflict-work-for-compound-foreign-key-columns
    return """INSERT INTO recommendation_products (recommendation_id, product__id, recommendation_type)
                   SELECT rp.*
                   FROM  (VALUES (%s, %s, %s)) rp(recommendation_id, product__id, recommendation_type)
                
                   WHERE  EXISTS (
                   SELECT FROM products p                                 
                   WHERE p.product__id = rp.product__id 
                   FOR    SHARE                                                -- weakest lock
                   );"""


# retrieve values
def profile_table_values(profile):
    """
    Takes profile-data (dict) as input.
    Selects the following data from the profile:
    - profile id
    - created data
    - first order
    - the latest order
    - the latest activity
    returns this data (tuple) (str, date_time, date_time, date_time, date_time)
    """
    wanted_values = (str(profile['_id']),
                     shared.secure_dict_item_double(profile, 'sm', 'created'),
                     shared.secure_dict_item(profile, 'latest_activity'),
                     shared.secure_dict_item_double(profile, 'order', 'latest'),
                     shared.secure_dict_item_double(profile, 'order', 'first'))
    if len(wanted_values) != 5:
        print(wanted_values)
    return wanted_values


def recommendation_table_values(profile, recommendation_id):
    """"""
    wanted_values = (recommendation_id,
                     str(profile['_id']),
                     profile['recommendations']['timestamp'],
                     profile['recommendations']['segment'],
                     profile['recommendations']['latest_visit'],
                     profile['recommendations']['total_pageview_count'],
                     profile['recommendations']['total_viewed_count'])
    return wanted_values


def all_values_for_profiles(profile, sql_cursor):
    """"""
    buids = shared.secure_dict_item(profile, 'buids')
    # skips profiles without associated buids.
    if not buids:
        return (list(),) * 4
    profile_values = [profile_table_values(profile)]
    buid_values = list()
    recommendation_values = list()
    recommended_products_values = list()

    for buid in buids:
        buid_values.append((str(buid), str(profile['_id'])))

    recommendation_data = shared.secure_dict_item(profile, 'recommendations')
    if recommendation_data:
        sql_cursor.execute("""SELECT nextval('recommendations_recommendation_id_seq')""")
        recommendation_id = sql_cursor.fetchone()[0]

        recommendation_values = [recommendation_table_values(profile, recommendation_id)]

        for viewed_item in recommendation_data['viewed_before']:
            recommended_products_values.append((recommendation_id, viewed_item, 'viewed'))
        for similar_item in recommendation_data['similars']:
            recommended_products_values.append((recommendation_id, similar_item, 'similar'))

    return profile_values, buid_values, recommendation_values, recommended_products_values


def upload_profile(profile):
    """
    Takes an active sql_cursor and profile-data (dict) as input.
    Creates several sql queries to upload the profile data to the following sql tables:
    - profiles
    - buid
    - recommendations (if present)
    - recommendation_products (if present)
    Executes the sql queries.
    """
    sql_connection, sql_cursor = sql_c.connect()
    profile_values, buid_values, recommendation_values, recommended_products_values = all_values_for_profiles(profile,
                                                                                                              sql_cursor)
    if profile_values:
        profile_query = create_profile_query()
        buid_query = create_buid_query()

        sql_cursor.execute(profile_query, profile_values)
        sql_cursor.executemany(buid_query, buid_values)
        if recommendation_values:
            recommendation_query = create_recommendation_query()
            sql_cursor.execute(recommendation_query, recommendation_values)
            if recommended_products_values:
                recommended_product_query = create_recommendation_product_query()
                sql_cursor.executemany(recommended_product_query, recommended_products_values)

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)


def upload_all_profiles():
    """Loads all profiles from the local mongodb database. Uploads the profiles to the local sql database."""
    database = mdb_c.connect_mdb()
    profile_collection = database.profiles
    sql_connection, sql_cursor = sql_c.connect()

    # creates queries
    profile_query = create_profile_query()
    buid_query = create_buid_query()
    recommendation_query = create_recommendation_query()
    recommended_products_query = create_recommendation_product_query()

    # creates a list for every table. We can create a tuple for every row we want to insert for the appropriate list.
    profile_values = list()
    buid_values = list()
    recommendation_values = list()
    recommended_products_values = list()

    for profile in profile_collection.find():
        temp_profile, temp_buids, temp_rec, temp_rec_prod = all_values_for_profiles(profile, sql_cursor)
        profile_values += temp_profile
        buid_values += temp_buids
        recommendation_values += temp_rec
        recommended_products_values += temp_rec_prod

    print('compiling complete')

    sql_cursor.executemany(profile_query, profile_values)
    print('profile table updated')
    sql_cursor.executemany(buid_query, buid_values)
    print('buid table updated')
    sql_cursor.executemany(recommendation_query, recommendation_values)
    print('recommendation table updated')
    sql_cursor.executemany(recommended_products_query, recommended_products_values)
    print('recommended_products_query table updated')
    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)


if __name__ == '__main__':
    upload_all_profiles()
