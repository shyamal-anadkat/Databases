SELECT COUNT(*)
FROM (
    SELECT DISTINCT Bids.UserID
    FROM Bids, Item
    WHERE Bids.UserID = Item.UserID
);
