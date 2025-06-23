CREATE TABLE transactions_v2 (
    transaction_id Uint64,
    user_id Uint64,
    category String,
    amount Double,
    currency String,
    transaction_datetime Datetime
    PRIMARY KEY (transaction_id)
);


