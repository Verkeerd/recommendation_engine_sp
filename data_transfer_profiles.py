import mongo_connection as mdb_c
import sql_connection as sql_c


def create_profile_query(profile):
    """"""
    sql_query = 'INSERT INTO profiles (profile__id, created, latest_order, first_order, latest_activity) ' \
                'VALUES (%s, %s, %s, %s, %s)'

    wanted_values = (str(profile['_id']),
                     profile['sm']['created'],
                     profile['order']['latest'],
                     profile['order']['first'],
                     profile['latest_activity'])
    print(sql_query, wanted_values)
    return sql_query, wanted_values


def create_buid_query(profile_id, buid):
    """"""
    sql_query = """INSERT INTO buid (buid, profile__id) 
    VALUES (%s, %s)"""

    return sql_query, (str(buid), str(profile_id))


def create_recommendation_query(profile):
    """"""
    sql_query = """INSERT INTO recommendations (profile__id, recommendation_time, segment, last_visit, total_pageview_count, 
    total_viewed_count) 
    VALUES (%s, %s, %s, %s, %s, %s)"""

    profile_rec = profile['recommendations']
    wanted_values = (str(profile['_id']),
                     profile_rec['timestamp'],
                     profile_rec['segment'],
                     profile_rec['latest_visit'],
                     profile_rec['total_pageview_count'],
                     profile_rec['total_viewed_count'])

    return sql_query, wanted_values


def create_recommendation_product_query(recommendation_id, product__id, recommendation_type):
    """"""
    sql_query = """INSERT INTO recommendation_products (recommendation_id, product__id, recommendation_type) 
        VALUES (%s, %s, %s) ON CONFLICT CONSTRAINT c_rp_fk_pid DO NOTHING"""

    return sql_query, (recommendation_id, str(product__id), recommendation_type)


def upload_profile(profile):
    """"""
    sql_connection, sql_cursor = sql_c.connect()
    # creates entry in profile table
    profile_query, profile_values = create_profile_query(profile)
    sql_cursor.execute(profile_query, profile_values)
    buids = profile['buids']
    # creates entries in buid table
    for buid in buids:
        buid_query, buid_values = create_buid_query(profile['_id'], buid)
        sql_cursor.execute(buid_query, buid_values)
    # creates entries in recommendation table and recommendation_product table if data is available.
    try:
        recommendation_data = profile['recommendations']
        recommendation_query, reccomendation_values = create_recommendation_query(profile)
        sql_cursor.execute(recommendation_query, reccomendation_values)
        recommendation_id = sql_cursor.fetchone()[0]
        for viewed_item in recommendation_data['viewed_before']:
            rp_query, rp_values = create_recommendation_product_query(recommendation_id, viewed_item, 'viewed')
            sql_cursor.execute(rp_query, rp_values)
        for similar_item in recommendation_data['similars']:
            rp_query, rp_values = create_recommendation_product_query(recommendation_id, similar_item, 'similar')
            sql_cursor.execute(rp_query, rp_values)
    except KeyError:
        pass

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)


def upload_all_profiles():
    """Loads all sessions from the local mongodb database."""
    database = mdb_c.connect_mdb()
    profile_collection = database.profiles
    sql_connection, sql_cursor = sql_c.connect()

    for profile in list(profile_collection.find()):
        print(profile['_id'])
        # creates entry in profile table
        profile_query, profile_values = create_profile_query(profile)
        sql_cursor.execute(profile_query, profile_values)
        buids = profile['buids']
        # creates entries in buid table
        for buid in buids:
            buid_query, buid_values = create_buid_query(profile['_id'], buid)
            sql_cursor.execute(buid_query, buid_values)
      # creates entries in recommendation table and recommendation_product table if data is available.
        try:
            recommendation_data = profile['recommendations']
            recommendation_query, reccomendation_values = create_recommendation_query(profile)
            sql_cursor.execute(recommendation_query, reccomendation_values)
            recommendation_id = sql_cursor.fetchone()[0]
            for viewed_item in recommendation_data['viewed_before']:
                rp_query, rp_values = create_recommendation_product_query(recommendation_id, viewed_item, 'viewed')
                sql_cursor.execute(rp_query, rp_values)
            for similar_item in recommendation_data['similars']:
                rp_query, rp_values = create_recommendation_product_query(recommendation_id, similar_item, 'similar')
                sql_cursor.execute(rp_query, rp_values)
        except KeyError:
            continue

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)


if __name__ == '__main__':
    upload_all_profiles()
