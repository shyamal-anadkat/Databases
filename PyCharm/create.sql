drop table if exists Item; 
drop table if exists User; 
drop table if exists Category;
drop table if exists Bids; 
drop table if exits CategoryList;

create table Item(
	ItemID INTEGER PRIMARY KEY, 
	SellerID TEXT NOT NULL,
	Name TEXT NOT NULL,
	Buy_Price REAL,
	First_Bid REAL NOT NULL,
	Started DATETIME,
	Ends DATETIME CHECK (Ends > Starts),
	Description TEXT NOT NULL,
	FOREIGN KEY(SellerID) REFERENCES User(UserID)

);
create table User(
	UserID TEXT PRIMARY KEY,
	Location TEXT,
	Country TEXT,
	Rating INTEGER
);
create table Category(
	ItemID INTEGER,
	Category TEXT,
	UNIQUE
);
create table Bids(
	UserID
	Time
	Amount
	ItemID
);
create table CategoryList(

);
