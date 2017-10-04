#!/usr/bin/env bash
rm *.dat
echo "Running parser..."
./runParser.sh
echo "Creating database..."
sqlite3 my.db < create.sql
echo "Loading tables..."
sqlite3 my.db < load.txt
echo "Testing queries..."
answers=(13422 80 8365 1046871451 3130 6717 150)
for i in {1..7}
do
  if [ $(sqlite3 my.db < query${i}.sql) != ${answers[$(( $i - 1 ))]} ]
  then
    echo "Query $i failed!"
  else
    echo "Query $i succeded!"
  fi
done