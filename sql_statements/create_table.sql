-- Creación de la tabla network
CREATE TABLE network (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    name NVARCHAR(100) NOT NULL
);

-- Creación de la tabla contract
CREATE TABLE contract (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    network_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES network(id),
    contract_address NVARCHAR(42) NOT NULL,
    name NVARCHAR(100),
    ticker NVARCHAR(10),
    is_active BIT DEFAULT 0,
    start_date DATETIME,
    end_date DATETIME
);

-- Creación de la tabla logs
CREATE TABLE logs (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    insert_date DATETIME DEFAULT GETDATE(),
    block_from INT NOT NULL,
    block_to INT NOT NULL,
    txs_insert INT NOT NULL,
    txs_amount INT NOT NULL,
    refreshed BIT DEFAULT 0,
    refreshed_date DATETIME,
    refreshed_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES logs(id)
);

-- Creación de la tabla transaction
CREATE TABLE transactions (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    hash NVARCHAR(66) NOT NULL,
    date DATETIME NOT NULL,
    block_number INT NOT NULL,
    [from] NVARCHAR(42) NOT NULL,
    [to] NVARCHAR(42) NOT NULL,
    amount DECIMAL(38, 18) NOT NULL,
    log_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES logs(id)
);

