import web

db = web.database(dbn='sqlite',
                  db='AuctionBase'  # add your SQLite database filename
                  )


######################BEGIN HELPER METHODS######################

# Enforce foreign key constraints
# WARNING: DO NOT REMOVE THIS!
def enforceForeignKey():
    db.query('PRAGMA foreign_keys = ON')


# initiates a transaction on the database
def transaction():
    return db.transaction()


# Sample usage (in auctionbase.py):
#
# t = sqlitedb.transaction()
# try:
#     sqlitedb.query('[FIRST QUERY STATEMENT]')
#     sqlitedb.query('[SECOND QUERY STATEMENT]')
# except Exception as e:
#     t.rollback()
#     print str(e)
# else:
#     t.commit()
#
# check out http://webpy.org/cookbook/transactions for examples

# returns the current time from your database
def getTime():
    # update the query string to match
    # the correct column and table name in your database
    query_string = 'select Time from CurrentTime'
    results = query(query_string)
    # alternatively: return results[0]['currenttime']
    # print str(results)
    return results[0].Time  # update this as well to match the
    # column name


# returns a single item specified by the Item's ID in the database
# Note: if the `result' list is empty (i.e. there are no items for a
# a given ID), this will throw an Exception!
def getItemById(item_id):
    # rewrite this method to catch the Exception in case `result' is empty
    query_string = 'select * from Items where ItemID = $itemID'
    try:
        result = query(query_string, {'itemID': item_id})
        return result[0]
    except IndexError:
        return None


def getUserById(user_id):
    # rewrite this method to catch the Exception in case `result' is empty
    query_string = 'select * from Users where UserID = $userID'
    try:
        result = query(query_string, {'userID': user_id})
        return result[0]
    except IndexError:
        return None


def getStatusByItemId(item_id):
    query_string = 'select Started, Ends from Items where ItemID = $itemID '
    try:
        result = query(query_string, {'itemID': item_id})
        started = result[0]['Started']
        ends = result[0]['Ends']
        now = getTime()
        status = 'Not Started' if (started > now) else 'Closed' if (ends < now) else 'Open'
        return status
    except IndexError:
        return None


def getCategoriesByItemId(item_id):
    query_string = 'select Category from Categories where ItemID = $itemID'
    try:
        results = query(query_string, {'itemID': item_id})
        return results
    except IndexError:
        return None


def getBidsByItemId(item_id):
    query_string = 'select * from Bids where ItemID = $itemID'
    try:
        bids = query(query_string, {'itemID': item_id})
        return bids
    except IndexError:
        return None


# wrapper method around web.py's db.query method
# check out http://webpy.org/cookbook/query for more info
def query(query_string, vars={}):
    return list(db.query(query_string, vars))


#####################END HELPER METHODS#####################

# TODO: additional methods to interact with your database,
# e.g. to update the current time

def getItemsOnSearch(itemID='', userID='', minPrice='', maxPrice='', status=''):
    _query = 'SELECT * FROM Items'
    no_params = (itemID == '' and userID == '' and minPrice == '' and maxPrice == '' and minPrice == '')

    if not no_params:
        _query += ' WHERE '
        if (itemID != ''):
            _query += 'ItemID = ' + itemID

        if (userID != ''):
            if (itemID != ''):
                _query += ' AND '
            _query += ' Seller_UserID = ' + "'" + userID + "'"

        if (minPrice != ''):
            if (itemID != '' or userID != ''):
                _query += ' AND '
            _query += ' Currently >= ' + minPrice

        if (maxPrice != ''):
            if (itemID != '' or userID != '' or minPrice != ''):
                _query += ' AND '
            _query += ' Currently <= ' + maxPrice

        if (status != 'all'):
            if (itemID != '' or userID != '' or minPrice != '' or maxPrice != ''):
                _query += ' AND '
            if status == 'open':
                _query += '(select Time from CurrentTime) between Started and Ends'
            elif status == 'notStarted':
                _query += 'Started > (select Time from CurrentTime)'
            elif status == 'close':
                _query += 'Ends < (select Time from CurrentTime)'

    result = query(_query)
    print _query  # debug
    return result
