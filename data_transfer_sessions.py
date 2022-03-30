import psycopg2.extras

import mongo_connection as mdb_c
import sql_connection as sql_c
import transfer_functions as shared
import time


# TODO: save compiled sql insert statement before exiting # TODO: save compiled sql insert statement before exiting code
# TODO: compile ddl file and compress for more speedy execution (maybe?)
# TODO: find workaround for memory issue with one (to four) sql insert statements.


def create_session_query():
    """
    Takes session_data (dict) as input.
    Selects the following data from the session:
    - session_id
    - buid
    - session start
    - session end
    - has sale
    - segment
    Creates an sql query to insert this data in the sessions table and returns this query.
    """
    return """
    INSERT INTO sessions (session__id, buid, session_start, session_end, has_sale, segment) 
    
    VALUES (%s, %s, %s, %s, %s, %s);
    """


def create_event_query():
    """
    Takes event_data (dict), a session_id (str) and number of previous events (int) as input.
    Selects the following data from the event:
    - time
    - source
    - action
    - type
    - product (if present)
    - time on page (if present)
    - click count (if present)
    - elements clicked (if present)
    - scrolls down (if present)
    - scrolls up (if present)
    Creates an sql query to insert this data (with the other input data) in the events table.
    Returns this query.
    """
    return """
    INSERT INTO events (session__id, previous_events, event_time, event_source, event_action, page_type, product, 
    time_on_page, click_count, elements_clicked, scrolls_down, scrolls_up) 
    
    VALUES ({});
    """.format(('%s, ' * 12)[:-2])


def create_ordered_product_query():
    """
    Takes a session_id (str) and, product__id (str) and a product_count (int) as input.
    Creates an sql query to insert this data in the ordered_products table and returns this query.
    """
    # source where exists/for share clause: Erwin Brandstetter
    # https://dba.stackexchange.com/questions/252875/how-to-make-on-conflict-work-for-compound-foreign-key-columns
    return """INSERT INTO ordered_products (session__id, product__id, total) 
                   SELECT op.*
                   FROM  (VALUES (%s, %s, %s)) op(session__id, product__id, total)
                
                   WHERE  EXISTS (
                   SELECT FROM products p                                 
                   WHERE p.product__id = op.product__id 
                   FOR    SHARE);
                   """


def create_preference_query():
    """Returns the query to insert data into the preference table without arguments."""
    return """
    INSERT INTO preferences (session__id, category, preference, views, sales) 
    
    VALUES (%s, %s, %s, %s, %s);
    """


def get_session_values(session, known_buids):
    buid = shared.secure_dict_item_double(session, 'buid', 0)
    if not buid:
        buid = '0'
    if buid not in known_buids:
        buid = '0'

    return (session['_id'],
            buid,
            session['session_start'],
            session['session_end'],
            session['has_sale'],
            shared.secure_dict_item(session, 'segment'))


def get_event_values(event, session_id, previous_events):
    wanted_values = (session_id,
                     previous_events,
                     event['t'],
                     event['source'],
                     event['action'],
                     event['pagetype'])

    uncertain_values = ['product', 'time_on_page', 'click_count', 'elements_clicked', 'scrolls_down', 'scrolls_up']
    for value in uncertain_values:
        wanted_values += (shared.secure_dict_item(event, value),)
    return wanted_values


def get_order_values(order, session_id):
    """
    Takes an active sql_cursor, order-data (dict) and a session_id (str) as input.
    Uploads each individual product with a product count to the ordered_products table.
    """
    result = list()
    if not order:
        return result

    product_dict = dict()
    for product in order['products']:
        product_id = product['id']
        try:
            product_dict[product_id] += 1
        except KeyError:
            product_dict[product_id] = 1

    for product_id, total_ordered in product_dict.items():
        result.append((session_id, product_id, total_ordered))

    return result


def all_values_session(session, known_buids):
    """"""
    events = shared.secure_dict_item(session, 'events')
    if not events:
        return tuple(), tuple(), tuple(), tuple()

    session_values = get_session_values(session, known_buids)
    events_values = list()
    ordered_products_values = list()
    preferences_values = tuple()

    for previous_events, event in enumerate(events):
        events_values.append((get_event_values(event, session['_id'], previous_events)))

    order = shared.secure_dict_item(session, 'order')
    if order:
        ordered_products_values = get_order_values(order, session['_id'])

    preferences = shared.secure_dict_item(session, 'preferences')
    if preferences:
        for category, entries in list(preferences.items()):
            entry_name, view_count = list(entries.items())[0]
            preferences_values = (session['_id'],
                                  category,
                                  entry_name,
                                  shared.secure_dict_item(view_count, 'views'),
                                  shared.secure_dict_item(view_count, 'sales'))

    return session_values, events_values, ordered_products_values, preferences_values


def get_preference_values(preferences, session_id):
    result = list()
    if not preferences:
        return result
    for category, entry_data in list(preferences.items()):
        entry_name, view_count = list(entry_data.items())[0]
        result.append((session_id, category, entry_name, view_count['views']))
    return preferences


def upload_session(sql_cursor, session, known_buids):
    """
    Takes an active sql_cursor and profile-data (dict) as input.
    Creates several sql queries to upload the profile data to the following sql tables:
    - sessions
    - events
    - orders (if present)
    - ordered_products (if present)
    - preferences (if present)
    Executes the sql queries.
    """
    # skips the sessions if there are no events linked to the session.
    try:
        events = session['events']
    except KeyError:
        return None
    # creates entry for session
    session_query, session_values = create_session_query()
    sql_cursor.execute(session_query, session_values)
    session_id = session['_id']
    # creates entries for events
    for previous_events, event in enumerate(events):
        create_event_query()
    try:
        order = session['order']
        if order:
            ordered_products_values = get_order_values(order, session_id)
            order_query = create_ordered_product_query()
    except KeyError:
        pass
    try:
        preferences = session['preferences']
        for category, entry_data in list(preferences.items()):
            entry_name, view_count = list(entry_data.items())[0]
            sql_cursor.execute(create_preference_query(), (session_id, category, entry_name, view_count['views']))
    except KeyError:
        pass


def upload_all_sessions():
    """Loads all sessions from the local mongodb database. Uploads the sessions to the local sql database."""
    start_time = time.time_ns()
    database = mdb_c.connect_mdb()
    session_collection = database.sessions
    sql_connection, sql_cursor = sql_c.connect()

    sql_cursor.execute("""SELECT buid FROM buid;""")
    known_buids = [data[0] for data in sql_cursor.fetchall()]

    # creates queries for tables
    session_query = create_session_query()
    event_query = create_event_query()
    ordered_products_query = create_ordered_product_query()
    preferences_query = create_preference_query()

    # fetches data to put into all tables from all documents
    for session_value, event_value, ordered_products_value, preference_value in iter(
            all_values_session(session, known_buids) for session in session_collection.find()):
        # uploads data to the session table
        sql_cursor.execute(session_query, session_value)
        # uploads data to the event table
        psycopg2.extras.execute_batch(sql_cursor, event_query, event_value)

        if ordered_products_value:
            # uploads data to the ordered_products table
            psycopg2.extras.execute_batch(sql_cursor, ordered_products_query, ordered_products_value)

        if preference_value:
            # uploads data to the preference table
            sql_cursor.execute(preferences_query, preference_value)

    sql_connection.commit()
    sql_c.disconnect(sql_connection, sql_cursor)

    return (time.time_ns() - start_time) / (9*60)


if __name__ == '__main__':
    print(upload_all_sessions())
