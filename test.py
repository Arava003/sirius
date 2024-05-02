from db_api import get_info_from_db
import pandas as pd
import sqlite3



df = get_info_from_db()
print(df)