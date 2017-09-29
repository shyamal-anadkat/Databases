SELECT COUNT(*)
FROM (
    SELECT DISTINCT Bids.UserID
    FROM Bids, Items
    WHERE Bids.UserID = Items.SellerID
);
