##### This file is an example of how to connect to the sqlite database

from __future__ import annotations

import sqlite3

from config.configuration import get_logger

logger = get_logger("data_conn")

db_file = "simple_crawler/data/2025_05_12_20_37_33/sqlite.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

if __name__ == "__main__":
    data = cursor.execute("SELECT * FROM urls").fetchall()
    breakpoint()
