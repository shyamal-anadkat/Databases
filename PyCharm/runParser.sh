rm -rf *.dat;

python skeleton_parser-pycharm.py ebay_data/items-*.json

sort users.dat | uniq > user.dat
sort bids.dat | uniq > bid.dat
sort categories.dat | uniq > category.dat
sort items.dat | uniq > item.dat
sort category_list.dat | uniq > categoryList.dat

rm users.dat;
rm bids.dat;
rm categories.dat;
rm items.dat;
rm category_list.dat;