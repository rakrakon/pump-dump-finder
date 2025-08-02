import sqlite3
import os

def connect_db():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(os.path.dirname(current_dir), "data")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "chat_history.db")

    return sqlite3.connect(db_path)

def create_table(conn):
    conn.execute('''
        CREATE TABLE chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_symbol TEXT NOT NULL UNIQUE,
            content TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()


if __name__ == "__main__":
    db_connection = connect_db()
    create_table(db_connection)
    db_connection.close()