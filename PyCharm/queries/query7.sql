SELECT COUNT(*)
FROM (
    SELECT DISTINCT Category.Category
    FROM Category, Bids
    WHERE Category.ItemID = Bids.ItemID
    AND Bids.Amount > 100
);
