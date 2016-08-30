def create_DB(conn):
    with conn:
        conn.executescript("""
        PRAGMA journal_mode = WAL;

        CREATE TABLE IF NOT EXISTS obj (
            objid INTEGER PRIMARY KEY AUTOINCREMENT,
            obj TEXT UNIQUE NOT NULL
        );
        CREATE INDEX IF NOT EXISTS obj_obj ON obj (obj);

        CREATE TABLE IF NOT EXISTS key (
            keyid INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE NOT NULL
        );
        CREATE INDEX IF NOT EXISTS key_key ON key (key);

        CREATE TABLE IF NOT EXISTS map (
            serial INTEGER NOT NULL PRIMARY KEY,
            objid INTEGER NOT NULL,
            keyid INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            listid INTEGER,
            UNIQUE (objid, keyid),
            FOREIGN KEY (objid) REFERENCES obj (objid),
            FOREIGN KEY (keyid) REFERENCES key (keyid),
            FOREIGN KEY (listid) REFERENCES list (listid)
        );
        CREATE INDEX IF NOT EXISTS map_list ON map (listid);

        CREATE TABLE IF NOT EXISTS list (
            itemid INTEGER PRIMARY KEY AUTOINCREMENT,
            listid INTEGER NOT NULL,
            value NOT NULL,
            CONSTRAINT unique_list_value UNIQUE (listid, value) ON CONFLICT IGNORE
        );
        CREATE INDEX IF NOT EXISTS list_id ON list (listid);
        CREATE INDEX IF NOT EXISTS list_value ON list (value);

        CREATE TABLE IF NOT EXISTS sync_state (
            peername STRING PRIMARY KEY,
            last_received INTEGER NOT NULL
        );
        """)
