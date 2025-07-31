CREATE TABLE IF NOT EXISTS nasdaq_data (
    DateTime TIMESTAMP NOT NULL,
    Open FLOAT,
    High FLOAT,
    Low FLOAT,
    Close FLOAT,
    Volume INTEGER,
    TickVolume INTEGER,
    source TEXT,
    Timestamp TIMESTAMP DEFAULT NOW()
);