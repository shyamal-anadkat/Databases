SELECT DISTINCT ItemID
FROM Item
WHERE (
    SELECT COUNT(*)
    FROM Categories
    WHERE Item.ItemID = Categories.ItemID
) = 4;
