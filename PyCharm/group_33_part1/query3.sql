SELECT COUNT(*)
FROM (
  SELECT ItemID
  FROM Category
  GROUP BY ItemID
  HAVING COUNT(Category) = 4
);