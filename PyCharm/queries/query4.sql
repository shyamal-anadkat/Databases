SELECT DISTINCT ItemID
FROM Bids
WHERE Price = (
    SELECT Price
    FROM Bids
    ORDER BY Price
    LIMIT 1
);
