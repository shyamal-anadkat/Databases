#!/usr/bin/env python

import sys;

sys.path.insert(0, 'lib')  # this line is necessary for the rest
import os  # of the imports to work!

import web
import sqlitedb
from jinja2 import Environment, FileSystemLoader
from datetime import datetime


###########################################################################################
##########################DO NOT CHANGE ANYTHING ABOVE THIS LINE!##########################
###########################################################################################

######################BEGIN HELPER METHODS######################

# helper method to convert times from database (which will return a string)
# into datetime objects. This will allow you to compare times correctly (using
# ==, !=, <, >, etc.) instead of lexicographically as strings.

# Sample use:
# current_time = string_to_time(sqlitedb.getTime())

def string_to_time(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')


# helper method to render a template in the templates/ directory
#
# `template_name': name of template file to render
#
# `**context': a dictionary of variable names mapped to values
# that is passed to Jinja2's templating engine
#
# See curr_time's `GET' method for sample usage
#
# WARNING: DO NOT CHANGE THIS METHOD
def render_template(template_name, **context):
    extensions = context.pop('extensions', [])
    globals = context.pop('globals', {})

    jinja_env = Environment(autoescape=True,
                            loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
                            extensions=extensions,
                            )
    jinja_env.globals.update(globals)

    web.header('Content-Type', 'text/html; charset=utf-8', unique=True)

    return jinja_env.get_template(template_name).render(context)


#####################END HELPER METHODS#####################

urls = ('/currtime', 'curr_time',
        '/selecttime', 'select_time',
        '/search', 'search',
        '/add_bid', 'add_bid',
        '/auction/(.*)', 'auction_detail',
        '/', 'home'
        # TODO: add additional URLs here
        # first parameter => URL, second parameter => class name
        )


class curr_time:
    # A simple GET request, to '/currtime'
    #
    # Notice that we pass in `current_time' to our `render_template' call
    # in order to have its value displayed on the web page
    def GET(self):
        current_time = sqlitedb.getTime()
        return render_template('curr_time.html', time=current_time)


# temp-home landing page
class home:
    def GET(self):
        return render_template('home.html')


class search:
    def GET(self):
        return render_template('search.html')

    def POST(self):
        try:
            search_params = web.input()

            item_id = search_params['itemID']
            user_id = search_params['userID']
            min_price = search_params['minPrice']
            max_price = search_params['maxPrice']
            status = search_params['status']
            items = []

            items = sqlitedb.getItemsOnSearch(item_id, user_id, min_price, max_price, status)
            # @TODO queries in sqlitedb
            message = 'Success ! Retreived ' + str(len(items)) + ' results.'

        except Exception as e:
            message = str(e)

        return render_template('search.html', search_result=items, message=message)


class add_bid:
    def GET(self):
        return render_template('add_bid.html')

    def POST(self):
        try:
            post_params = web.input()
            add_result = ''
            item_id = post_params['itemID']
            price = post_params['price']
            user_id = post_params['userID']
            current_time = sqlitedb.getTime()

            # Validation checks

            # make all input fields required
            if (item_id == '' or price == '' or user_id == ''):
                return render_template('add_bid.html', message='Error: All fields are required')

            # don't accept bids on items that don't exist
            if (sqlitedb.getItemById(item_id) == None):
                return render_template('add_bid.html', message='Error: Invalid Item ID !')

            # Don't accept bids from users that don't exist
            if (sqlitedb.getUserById(user_id) == None):
                return render_template('add_bid.html', message='Error: Invalid User ID !')

            # @TODO: add more validation checks


            # insert transaction
            message = ''
            t = sqlitedb.transaction()
            try:
                sqlitedb.db.insert('Bids', ItemID=item_id, UserID=user_id, Amount=price, Time=current_time)

            except Exception as e:
                t.rollback()
                message = 'Error ! Bid did not get added.'
                print str(e)

            else:
                t.commit()
                message = 'Added Bid !'
                print 'commited ' + str(t)
                # message = 'insert success'

                # @TODO validations
                # add_result = 'executed.'

        except Exception as e:
            message = str(e)

        return render_template('add_bid.html', message=message, add_result=add_result)


class select_time:
    # Aanother GET request, this time to the URL '/selecttime'
    def GET(self):
        return render_template('select_time.html')

    # A POST request
    #
    # You can fetch the parameters passed to the URL
    # by calling `web.input()' for **both** POST requests
    # and GET requests
    def POST(self):
        post_params = web.input()
        MM = post_params['MM']
        dd = post_params['dd']
        yyyy = post_params['yyyy']
        HH = post_params['HH']
        mm = post_params['mm']
        ss = post_params['ss'];
        enter_name = post_params['entername']

        selected_time = '%s-%s-%s %s:%s:%s' % (yyyy, MM, dd, HH, mm, ss)
        update_message = '(Hello, %s. Previously selected time was: %s.)' % (enter_name, selected_time)
        # TODO: save the selected time as the current time in the database
        # insert transaction

        t = sqlitedb.transaction()
        try:
            _query = 'UPDATE CurrentTime SET Time = $time'
            sqlitedb.db.query(_query, {'time': selected_time})
        except Exception as e:
            t.rollback()
            print str(e)
            update_message = str(e)
        else:
            t.commit()

        # Here, we assign `update_message' to `message', which means
        # we'll refer to it in our template as `message'
        return render_template('select_time.html', message=update_message)

class auction_detail:
    def GET(self, item):
        auction = web.input(item=None)
        categories = sqlitedb.getCategoriesByItemId(auction.item)
        return render_template('auction_detail.html', status = '', bids ='', categories=categories, details='')


###########################################################################################
##########################DO NOT CHANGE ANYTHING BELOW THIS LINE!##########################
###########################################################################################

if __name__ == '__main__':
    web.internalerror = web.debugerror
    app = web.application(urls, globals())
    app.add_processor(web.loadhook(sqlitedb.enforceForeignKey))
    app.run()
