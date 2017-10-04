#!/bin/sh

rm -f user.dat bid.dat category.dat item.dat categoryList.dat;

python skeleton_parser-pycharm.py ebay_data/items-*.json

sort users.dat | uniq > user.dat
sort bids.dat | uniq > bid.dat
sort categories.dat | uniq > category.dat
sort items.dat | uniq > item.dat
sort category_list.dat | uniq > categoryList.dat

rm -f users.dat bids.dat categories.dat items.dat category_list.dat;