SELECT COUNT(*)
FROM User
WHERE Location <> null
AND Country <> null
AND Rating > 100;
