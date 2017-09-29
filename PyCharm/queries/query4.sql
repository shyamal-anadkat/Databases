select ItemID
from Items
where Currently=(select MAX(Currently) from Items);