-- description: Constraint 15

PRAGMA foreign_keys = ON;

drop trigger if exists trigger2;

create trigger trigger2
	before insert on Bids
	for each row when (NEW.Time != (Select c.Time from CurrentTime c))
	begin
		SELECT raise(rollback, 'Failed ! Time of the bid must match Current Time.');
	end;