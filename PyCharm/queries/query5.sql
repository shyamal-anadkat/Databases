SELECT COUNT(*)
FROM (
    SELECT DISTINCT User.UserID
    FROM User, Item
    WHERE User.UserID = Item.UserID
    AND User.Rating > 100
)
