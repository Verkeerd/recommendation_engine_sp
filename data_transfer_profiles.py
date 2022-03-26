import mongo_connection as mdb_c
import sql_connection as sql_c


def create_profile_query(profile):
    """
    Takes profile-data (dict) as input.
    Selects the following data from the profile:
    - profile id
    - created data
    - first order
    - the latest order
    - the latest activity
    Creates an sql query to insert this data into the profile table and returns this query.
    """
    sql_query_qp = 'INSERT INTO profiles (profile__id)'
    sql_query_vp = ' VALUES (%s)'

    wanted_values = (str(profile['_id']),)
    try:
        wanted_values += (profile['sm']['created'],)
        sql_query_qp = sql_query_qp[:-1] + ', created)'
        sql_query_vp = sql_query_vp[:-1] + ', %s)'
    except KeyError:
        pass
    try:
        wanted_values += (profile['order']['latest'], profile['order']['first'])
        sql_query_qp = sql_query_qp[:-1] + ', latest_order, first_order)'
        sql_query_vp = sql_query_vp[:-1] + ', %s, %s)'
    except KeyError:
        pass
    try:
        wanted_values += (profile['latest_activity'],)
        sql_query_qp = sql_query_qp[:-1] + ', latest_activity)'
        sql_query_vp = sql_query_vp[:-1] + ', %s)'
    except KeyError:
        pass

    sql_query = sql_query_qp + sql_query_vp

    return sql_query, wanted_values


def create_buid_query(profile_id, buid):
    """
    Takes a profile_id (str) and a buid (str) as input.
    Creates an sql query to insert this data in the buid table and returns this query.
    """
    sql_query = """INSERT INTO buid (buid, profile__id) 
    VALUES (%s, %s) ON CONFLICT ON CONSTRAINT c_b_pk DO NOTHING;"""

    return sql_query, (str(buid), str(profile_id))


def create_recommendation_query(profile, recommendation_id):
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
    sql_query = """INSERT INTO recommendations (recommendation_id, profile__id, recommendation_time, segment, 
    last_visit, total_pageview_count, total_viewed_count) 
    VALUES (%s, %s, %s, %s, %s, %s, %s);"""

    profile_rec = profile['recommendations']
    wanted_values = (recommendation_id,
                     str(profile['_id']),
                     profile_rec['timestamp'],
                     profile_rec['segment'],
                     profile_rec['latest_visit'],
                     profile_rec['total_pageview_count'],
                     profile_rec['total_viewed_count'])

    return sql_query, wanted_values


def create_recommendation_product_query(recommendation_id, product__id, recommendation_type):
    """
    Takes a recommendation_id (int) and, product__id (str) and a recommendation_type (str) as input.
    Creates an sql query to insert this data in the recommendation_products table and returns this query.
    """
    # source where exists/for share clause: Erwin Brandstetter
    # https://dba.stackexchange.com/questions/252875/how-to-make-on-conflict-work-for-compound-foreign-key-columns
    sql_query = """INSERT INTO recommendation_products (recommendation_id, product__id, recommendation_type)
                   SELECT rp.*
                   FROM  (VALUES (%s, %s, %s)) rp(recommendation_id, product__id, recommendation_type)
                
                   WHERE  EXISTS (
                   SELECT FROM products p                                 
                   WHERE p.product__id = rp.product__id 
                   FOR    SHARE                                                -- weakest lock
                   );"""

    return sql_query, (recommendation_id, str(product__id), recommendation_type)


def upload_profile(sql_cursor, profile):
    """
    Takes an active sql_cursor and profile-data (dict) as input.
    Creates several sql queries to upload the profile data to the following sql tables:
    - profiles
    - buid
    - recommendations (if present)
    - recommendation_products (if present)
    Executes the sql queries.
    """
    # skips profiles without associated buids.
    try:
        buids = profile['buids']
        if len(buids) == 0:
            return None
    except KeyError:
        return None

    # creates entry in profile table
    profile_query, profile_values = create_profile_query(profile)
    sql_cursor.execute(profile_query, profile_values)

    # creates entries in buid table
    for buid in buids:
        buid_query, buid_values = create_buid_query(profile['_id'], buid)
        sql_cursor.execute(buid_query, buid_values)
    # creates entries in recommendation table and recommendation_product table if data is available.
    try:
        recommendation_data = profile['recommendations']
        # generates recommendation_id with the sql_sequence
        sql_cursor.execute("""SELECT nextval('recommendations_recommendation_id_seq')""")
        recommendation_id = sql_cursor.fetchone()[0]
        # creates entry for recommendation table
        recommendation_query, recommendation_values = create_recommendation_query(profile, recommendation_id)
        sql_cursor.execute(recommendation_query, recommendation_values)
        # creates entries for recommended_products table
        for viewed_item in recommendation_data['viewed_before']:
            rp_query, rp_values = create_recommendation_product_query(recommendation_id, viewed_item, 'viewed')
            sql_cursor.execute(rp_query, rp_values)
        for similar_item in recommendation_data['similars']:
            rp_query, rp_values = create_recommendation_product_query(recommendation_id, similar_item, 'similar')
            sql_cursor.execute(rp_query, rp_values)
    except KeyError:
        return None


def upload_all_profiles():
    """Loads all profiles from the local mongodb database. Uploads the profiles to the local sql database."""
    database = mdb_c.connect_mdb()
    profile_collection = database.profiles
    sql_connection, sql_cursor = sql_c.connect()

    for profile in profile_collection.find():
        profile = dict(profile)
        upload_profile(sql_cursor, profile)

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)


if __name__ == '__main__':
    upload_all_profiles()
