import json

def upsert_conversation(conn, company_symbol, conversation_json):
    """
    Insert or update the conversation for a company.
    """
    content_str = json.dumps(conversation_json)
    conn.execute(
        '''
        INSERT INTO chat_messages (company_symbol, content)
        VALUES (?, ?)
        ON CONFLICT(company_symbol) DO UPDATE SET
            content = excluded.content,
            timestamp = CURRENT_TIMESTAMP
        ''',
        (company_symbol, content_str)
    )
    conn.commit()

def get_conversation_by_company(conn, company_symbol):
    cursor = conn.execute(
        'SELECT content, timestamp FROM chat_messages WHERE company_symbol = ?',
        (company_symbol,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "content": json.loads(row[0]),
            "timestamp": row[1]
        }
    return None

def update_conversation_by_id(conn, conversation_id, new_conversation_json):
    content_str = json.dumps(new_conversation_json)
    conn.execute(
        'UPDATE chat_messages SET content = ?, timestamp = CURRENT_TIMESTAMP WHERE id = ?',
        (content_str, conversation_id)
    )
    conn.commit()

def get_conversation_by_id(conn, conversation_id):
    cursor = conn.execute(
        'SELECT id, company_symbol, content, timestamp FROM chat_messages WHERE id = ?',
        (conversation_id,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "id": row[0],
            "company_symbol": row[1],
            "content": json.loads(row[2]),
            "timestamp": row[3]
        }
    return None
