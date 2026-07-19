import sqlite3
conn = sqlite3.connect('D:/Play/Commodity/data/commodity_data.db')
c = conn.cursor()
print('Entities:', c.execute('SELECT COUNT(*) FROM entities').fetchone()[0])
print('By entity type:', dict(c.execute('SELECT entity_type, COUNT(*) FROM entities GROUP BY entity_type').fetchall()))
print('By state:', dict(c.execute('SELECT state, COUNT(*) FROM entities GROUP BY state').fetchall()))
print('Tasks:', c.execute('SELECT COUNT(*) FROM tasks').fetchone()[0])
print('Completed:', c.execute('SELECT COUNT(*) FROM tasks WHERE status="completed"').fetchone()[0])