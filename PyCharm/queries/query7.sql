SELECT DISTINCT Categories.ItemID
FROM Categories, Bids
WHERE Categories.ItemID = Bids.ItemID
AND Amount > 100;
