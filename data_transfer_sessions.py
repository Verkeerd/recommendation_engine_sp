import psycopg2.extras
import datetime
import mongo_connection as mdb_c
import sql_connection as sql_c
import transfer_functions as shared
import time


# TODO: save compiled sql insert statement before exiting # TODO: save compiled sql insert statement before exiting code
# TODO: compile (ddl?) file to work around memory problem and still execute in one go. (or find better solution)
# TODO: and compress for more speedy execution (maybe?)


# queries
def create_session_query():
    """
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
    Returns this query (str).
    """
    return """
    INSERT INTO events (session__id, previous_events, event_time, event_source, event_action, page_type, product, 
    time_on_page, click_count, elements_clicked, scrolls_down, scrolls_up) 
    
    VALUES ({});
    """.format(('%s, ' * 12)[:-2])


def create_ordered_product_query():
    """
    Creates an sql query to insert the following data in the ordered_products table:
    - session_id
    - product_id
    - product_count
    Returns this query.
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
    """
    Returns the query to insert the following data into the preference table:
    - session_id
    - category
    - preference
    - views
    - sales
    Returns the query (str).
    """
    return """
    INSERT INTO preferences (session__id, category, preference, views, sales) 
    
    VALUES (%s, %s, %s, %s, %s);
    """


# fetch values
def get_session_values(session):
    """
    Takes session_data (dict), known_buids (list) as input.
    Selects the following data from the session:
    - session_id
    - buid
    - session start
    - session end
    - has sale
    - segment
    Returns these values in order (tuple).
    """
    buid = str(shared.secure_dict_fetch_double(session, 'buid', 0))

    wanted_values = (str(session['_id']),
                     buid,
                     session['session_start'],
                     session['session_end'],
                     session['has_sale'],
                     shared.secure_dict_fetch(session, 'segment'))

    return wanted_values


def get_event_values(event, session_id, previous_events):
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
    Returns the selected values in order (tuple).
    """
    wanted_values = (session_id,
                     previous_events,
                     event['t'],
                     event['source'],
                     event['action'],
                     event['pagetype'])

    uncertain_values = ['product', 'time_on_page', 'click_count', 'elements_clicked', 'scrolls_down', 'scrolls_up']
    for value in uncertain_values:
        wanted_values += (shared.secure_dict_fetch(event, value),)

    return wanted_values


def get_order_values(order, session_id):
    """
    Takes an order (dict) and a session_id (str) as input.
    Counts how often each product appears in the order dict and puts following data in a tuple:
    - session_id
    - product_id
    - product_count
    Puts these tuples in a list and returns this (list) [(str, str, int)]
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


def get_preference_values(preferences, session_id):
    """
    Takes preferences (dict) and session_id (str) as input.
    Selects the following data from preferences for every item in preferences:
    - category
    - preference
    - views
    - sales
    Puts the session_id and these values (in order) in a tuple. Places the tuples in a list.
    Returns this (list) [(int, str, str, int, int)]
    """
    result = list()
    if not preferences:
        return result
    for category, entry_data in list(preferences.items()):
        entry_name, view_count = list(entry_data.items())[0]
        result.append((session_id,
                       category,
                       entry_name,
                       shared.secure_dict_fetch(view_count, 'views'),
                       shared.secure_dict_fetch(view_count, 'sales')))
    return preferences


def all_values_session(session):
    """
    Takes a profile (dict) and the active sql_cursor as input.

    Selects where present the wanted data from the profile.
    Wanted data is data we want to upload to the following sql tables:
    - sessions
    - events
    - ordered_products (if present)
    - preferences (if present)

    Returns session_values, event_values, ordered_products_values, preference_values (tuple) ([], [], [], []
    """
    events = shared.secure_dict_fetch(dict(session), 'events')
    if not events:
        return tuple(), tuple(), tuple(), tuple()

    # creates the lists we are going to fill and return
    session_values = get_session_values(session)  # fetches session values
    events_values = list()
    ordered_products_values = list()
    preferences_values = tuple()

    # selects event values for every event associated with the session. Adds them to the event value list
    for previous_events, event in enumerate(events):
        events_values.append((get_event_values(event, session_values[0], previous_events)))

    # selects order values for every order associated with the session. Adds them to the order value list
    order = shared.secure_dict_fetch(session, 'order')
    if order:
        ordered_products_values = get_order_values(order, session_values[0])

    preferences = shared.secure_dict_fetch(session, 'preferences')
    if preferences:
        # selects preference_category, preference and view_count or sale_count for every preference associated with
        # the session. Adds them to the preference value list.
        # TODO: ↓ Think of better names! also for the preferences table in the sql database.
        preferences_values = list()
        for category, entries in list(preferences.items()):
            entry_name, present_counter = list(entries.items())[0]
            preference = (session_values[0],
                          category,
                          entry_name,
                          shared.secure_dict_fetch(present_counter, 'views'),
                          shared.secure_dict_fetch(present_counter, 'sales'))
            preferences_values.append(preference)

    return session_values, events_values, ordered_products_values, preferences_values


# upload
def upload_session(session):
    """
    Takes an active sql_cursor and a session (dict) as input.
    Creates several sql queries to upload the profile data to the following sql tables:
    - sessions
    - events
    - ordered_products (if present)
    - preferences (if present)
    Executes the sql queries.
    """
    # skips the sessions if there are no events linked to the session.
    sql_connection, sql_cursor = sql_c.connect()
    session_value, event_value, ordered_products_value, preference_value = all_values_session(session)
    if not session_value:
        # TODO: raise error? maybe log it somewhere
        return None
        # uploads data to the session table
    # creates sql_queries
    session_query = create_session_query()
    event_query = create_event_query()
    ordered_products_query = create_ordered_product_query()
    preferences_query = create_preference_query()

    # uploads data to the session table
    sql_cursor.execute(session_query, session_value)
    # uploads data to the event table
    psycopg2.extras.execute_batch(sql_cursor, event_query, event_value)

    if ordered_products_value:
        # uploads data to the ordered_products table
        psycopg2.extras.execute_batch(sql_cursor, ordered_products_query, ordered_products_value)

    if preference_value:
        # uploads data to the preference table
        psycopg2.extras.execute_batch(sql_cursor, preferences_query, preference_value)
    sql_connection.commit()

    sql_c.disconnect(sql_connection, sql_cursor)

    return None


def upload_all_sessions():
    """Loads all sessions from the local mongodb database. Uploads the sessions to the local sql database."""
    start_time = time.time()
    database = mdb_c.connect_mdb()
    session_collection = database.sessions
    sql_connection, sql_cursor = sql_c.connect()
    print('busy')

    # creates queries for tables
    session_query = create_session_query()
    event_query = create_event_query()
    ordered_products_query = create_ordered_product_query()
    preferences_query = create_preference_query()

    # fetches data to put into all tables for all documents and loads this into the cursor
    for session_value, event_value, ordered_products_value, preference_value in iter(
            all_values_session(session) for session in session_collection.find()):
        if session_value:
            # uploads data to the session table
            sql_cursor.execute(session_query, session_value)
            # uploads data to the event table
            psycopg2.extras.execute_batch(sql_cursor, event_query, event_value)

            if ordered_products_value:
                # prints session_id sometimes to show progress
                print(session_value[0])
                # uploads data to the ordered_products table
                psycopg2.extras.execute_batch(sql_cursor, ordered_products_query, ordered_products_value)

            if preference_value:
                # uploads data to the preference table
                psycopg2.extras.execute_batch(sql_cursor, preferences_query, preference_value)

    sql_connection.commit()
    print('\ndata is committed')
    sql_c.disconnect(sql_connection, sql_cursor)

    return time.time() - start_time


if __name__ == '__main__':
    print('upload took {:.1f} minutes'.format(upload_all_sessions() / 60))
