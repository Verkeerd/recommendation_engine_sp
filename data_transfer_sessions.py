import mongo_connection as mdb_c
import sql_connection as sql_c

def create_session_query(session):
    """"""
    sql_query_qp = """INSERT INTO sessions (session__id, buid, session_start, session_end, has_sale)"""
    sql_query_vp = """ VALUES (%s, %s, %s, %s, %s)"""

    try:
        buid = session['buid'][0]
    except KeyError:
        buid = '0'

    wanted_values = (session['_id'],
                     buid,
                     session['session_start'],
                     session['session_end'],
                     session['has_sale'])

    try:
        wanted_values += (session['segment'],)
        sql_query_qp = sql_query_qp[:-1] + ', segment)'
        sql_query_vp = sql_query_vp[:-1] + ', %s)'
    except KeyError:
        pass

    sql_query = sql_query_qp + sql_query_vp

    return sql_query, wanted_values


def create_event_query(event, session_id, previous_events):
    """"""
    sql_query = """INSERT INTO events (session__id, previous_events, event_time, event_source, event_action, page_type, 
    product, time_on_page, click_count, elements_clicked, scrolls_down, scrolls_up)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    wanted_values = (session_id,
                     previous_events,
                     event['t'],
                     event['source'],
                     event['action'],
                     event['pagetype'],
                     event['product'],
                     event['time_on_page'],
                     event['click_count'],
                     event['elements_clicked'],
                     event['scrolls_down'],
                     event['scrolls_up'])

    return sql_query, wanted_values


def create_ordered_product_query(session_id, product_id, total):
    """"""
    sql_query = """INSERT INTO orders (session_id, product_id, total) VALUES (%s, %s, %s)"""

    return sql_query, (session_id, product_id, total)


def upload_order(sql_cursor, order, session_id):
    """"""
    product_dict = {}
    for product in order:
        if product in product_dict:
            product_dict[product] += 1
        else:
            product_dict[product] = 1

    for product_id, total_ordered in product_dict.items():
        op_query, op_values = create_ordered_product_query(session_id, product_id, total_ordered)
        sql_cursor.execute(op_query, op_values)


def create_preference_query():
    """"""
    sql_query = """INSERT INTO preferences (session__id, category, preference, viewcount) 
    VALUES (%s, %s, %s, %s)"""

    return sql_query


def upload_session(sql_cursor, session):
    """"""
    # skips the sessions if there are no events linked to the session.
    try:
        events = session['events']
    except KeyError:
        return None
    # creates entry for session
    session_query, session_values = create_session_query(session)
    sql_cursor.execute(session_query, session_values)
    session_id = session['_id']
    print(session_id)
    # creates entries for events
    print(events)
    for previous_events, event in enumerate(events):
        create_event_query(event, session_id, previous_events)
    try:
        order = session['order']
        if order:
            upload_order(sql_cursor, order, session_id)
    except KeyError:
        pass
    try:
        preferences = session['preferences']
        for category, entries in list(preferences.items()):
            entry_name, view_count = list(entries.items())[0]
            print(session_id, category, entry_name, view_count)
            sql_cursor.execute(create_preference_query(), (session_id, category, entry_name, view_count['views']))
    except KeyError:
        pass


def upload_all_sessions():
    """Loads all sessions from the local mongodb database."""
    database = mdb_c.connect_mdb()
    session_collection = database.sessions
    print('1')
    print(session_collection)
    sql_connection, sql_cursor = sql_c.connect()
    print('2')
    for session in session_collection.find():
        session = dict(session)
        print('3')
        print(session)
        upload_session(sql_cursor, session)

    sql_c.disconnect(sql_connection, sql_cursor)


if __name__ == '__main__':
    upload_all_sessions()
