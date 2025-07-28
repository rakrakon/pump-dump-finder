import json

def add_conversation(conn, company_name, conversation_json):
    """
    conversation_json: dict or list representing the full conversation
    """
    content_str = json.dumps(conversation_json)
    conn.execute(
        'INSERT INTO chat_messages (company_name, content) VALUES (?, ?)',
        (company_name, content_str)
    )
    conn.commit()

def get_last_conversations(conn, company_name, limit=20):
    cursor = conn.execute(
        'SELECT content, timestamp FROM chat_messages WHERE company_name = ? ORDER BY id DESC LIMIT ?',
        (company_name, limit)
    )
    rows = cursor.fetchall()
    # Return parsed JSONs, newest last
    conversations = []
    for content_str, timestamp in reversed(rows):
        conversations.append({
            "content": json.loads(content_str),
            "timestamp": timestamp
        })
    return conversations

def update_conversation(conn, conversation_id, new_conversation_json):
    content_str = json.dumps(new_conversation_json)
    conn.execute(
        'UPDATE chat_messages SET content = ?, timestamp = CURRENT_TIMESTAMP WHERE id = ?',
        (content_str, conversation_id)
    )
    conn.commit()

def get_conversation_by_id(conn, conversation_id):
    cursor = conn.execute(
        'SELECT id, company_name, content, timestamp FROM chat_messages WHERE id = ?',
        (conversation_id,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "id": row[0],
            "company_name": row[1],
            "content": json.loads(row[2]),
            "timestamp": row[3]
        }
    return None