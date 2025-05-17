##### This file is an example of how to connect to the sqlite database

from __future__ import annotations

import os
import sqlite3
import sys

loc = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(f"{loc}/simple_crawler")
from config.configuration import get_logger  # noqa

logger = get_logger("data_conn")

db_file = "simple_crawler/data/2025_05_12_20_37_33/sqlite.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

if __name__ == "__main__":
    data = cursor.execute("SELECT * FROM urls").fetchall()
    # breakpoint()
