SELECT COUNT(*)
FROM (
    SELECT DISTINCT Users.UserID, Users.Rating
    FROM Users, Items
    WHERE Users.UserID = Items.SellerID
    AND Users.Rating > 1000
);
