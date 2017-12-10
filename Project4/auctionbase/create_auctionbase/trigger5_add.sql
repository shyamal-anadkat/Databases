-- description: Constraint 11

PRAGMA foreign_keys = ON;

drop trigger if exists trigger5;

create trigger trigger5
	before insert on Bids
	for each row when (NEW.Time < (Select i.Started from Items i where NEW.ItemID = i.ItemID))
	begin
		SELECT raise(rollback, 'Bid Failed! No bids may be placed before auction start time or after end time.');
	end;