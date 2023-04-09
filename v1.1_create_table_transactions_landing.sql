CREATE TABLE transactions_landing(
    TxDate varchar(255), 
    Amount varchar(255), 
    TxDescription varchar(255) ,
    Balance varchar(255),
    DataSource varchar(255),
    Line integer,
    CONSTRAINT PK_Tx primary key (TxDate, Balance)
);