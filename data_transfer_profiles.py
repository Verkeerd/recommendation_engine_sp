import mongo_connection as mdb_c
import sql_connection as sql_c
import transfer_functions as shared


# queries
def create_profile_query():
    """
    Creates an sql query to insert the following data into the profile table:
    - profile id
    - created data
    - first order
    - the latest order
    - the latest activity
    Returns this query (str).
    """
    return """INSERT INTO profiles (profile__id, created, latest_activity, latest_order, first_order) 
    VALUES (%s, %s, %s, %s, %s);"""


def create_buid_query():
    """
    Creates an sql query to insert the following data in the buid table:
    - profile_id
    - buid
    Returns this query (str).
    """
    return """INSERT INTO buid (buid, profile__id) 
    VALUES (%s, %s) ON CONFLICT ON CONSTRAINT c_b_pk DO NOTHING;"""


def create_recommendation_query():
    """
    Selects the following data from the profile:
    - profile_id
    - recommendation time
    - segment
    - last visit
    - total pageviewcount
    - total viewed count
    Creates an sql query to insert the following data into the recommendation table:

    Returns this query (str).
    """
    return """INSERT INTO recommendations (recommendation_id, profile__id, recommendation_time, segment, 
    last_visit, total_pageview_count, total_viewed_count) 
    VALUES (%s, %s, %s, %s, %s, %s, %s);"""


def create_recommendation_product_query():
    """
    Creates an sql query to insert the following data in the recommendation_products table:
    - recommendation_id
    - product__id
    - recommendation_type
    Returns the query (str).
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
    Returns this data (tuple) (str, date_time, date_time, date_time, date_time)
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
    """
    Takes profile_data (dict) and a recommendation_id (str) as input.
    Selects the following data from the profile:
    - profile_id
    - recommendation time
    - segment
    - last visit
    - total pageviewcount
    - total viewed count
    Returns the recommendation_id together with this data selection (tuple)
    (int, str, datetime, datetime, datetime, int, int)
    """
    wanted_values = (recommendation_id,
                     str(profile['_id']),
                     profile['recommendations']['timestamp'],
                     profile['recommendations']['segment'],
                     profile['recommendations']['latest_visit'],
                     profile['recommendations']['total_pageview_count'],
                     profile['recommendations']['total_viewed_count'])
    return wanted_values


def all_values_for_profiles(profile, sql_cursor):
    """
    Takes a profile (dict) and the active sql_cursor as input.
    Selects where present the wanted data from the profile.
    Wanted data is data we want to upload to the following sql tables:
    - profiles
    - buid
    - recommendations (if present)
    - recommendation_products (if present)
    Returns profile_values, buid_values, recommendation_values, recommendation_products_values (tuple) ([], [], [], [])
    """
    buids = shared.secure_dict_item(profile, 'buids')
    # skips profiles without associated buids.
    if not buids:
        return (list(),) * 4

    # creates the lists we are going to fill and return
    profile_values = [profile_table_values(profile)]    # fetches profile_values
    buid_values = list()
    recommendation_values = list()
    recommended_products_values = list()

    # selects buid and profile_id as values for every buid associated with the profile. Adds them to the buid value list
    for buid in buids:
        buid_values.append((str(buid), str(profile['_id'])))

    recommendation_data = shared.secure_dict_item(profile, 'recommendations')
    if recommendation_data:
        # selects the recommendation_id by using the sql-sequence
        sql_cursor.execute("""SELECT nextval('recommendations_recommendation_id_seq')""")
        recommendation_id = sql_cursor.fetchone()[0]
        # fetches recommendation_values
        recommendation_values = [recommendation_table_values(profile, recommendation_id)]
        # selects recommendation_id, product_id and a tag ('viewed' or 'similar') as values for every recommended
        # product associated with the profile. Adds them to the recommended values list.
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
    # selects all wanted values from profile and puts them in a lists.
    # One list contains values to insert into one table in sql
    profile_values, buid_values, recommendation_values, rec_products_values = all_values_for_profiles(profile,
                                                                                                      sql_cursor)
    # Does nothing if no values were generated
    if profile_values:
        profile_query = create_profile_query()
        buid_query = create_buid_query()

        sql_cursor.execute(profile_query, profile_values)
        sql_cursor.executemany(buid_query, buid_values)
        # inserts recommendations if they present in the profile
        if recommendation_values:
            recommendation_query = create_recommendation_query()
            sql_cursor.execute(recommendation_query, recommendation_values)
            if rec_products_values:
                recommended_product_query = create_recommendation_product_query()
                sql_cursor.executemany(recommended_product_query, rec_products_values)

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

    # creates a value list for every sql table.
    # We can add a tuple to these lists for every row we want to insert into the sql table.
    profile_values = list()
    buid_values = list()
    recommendation_values = list()
    recommended_products_values = list()

    for profile in profile_collection.find():
        # selects all wanted values from profile in lists.
        temp_profile, temp_buids, temp_rec, temp_rec_prod = all_values_for_profiles(profile, sql_cursor)
        # adds fetched values to the bigger value lists.
        profile_values += temp_profile
        buid_values += temp_buids
        recommendation_values += temp_rec
        recommended_products_values += temp_rec_prod

    print('compiling complete')

    # executes the insert statement for each sql table.
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
