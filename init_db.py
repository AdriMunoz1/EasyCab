import sqlite3

def create_database():
    connection = sqlite3.connect('taxis.db')
    cursor = connection.cursor()

    cursor.execute('''
            CREATE TABLE IF NOT EXISTS taxis (
                id INTEGER PRIMARY KEY,
                destination TEXT,
                estado TEXT
            )
     ''')

    taxis = [
        (1, '', 'KO'),
        (2, '', 'KO'),
        (3, '', 'KO'),
        (4, '', 'KO')
    ]
    cursor.executemany('''
        INSERT OR IGNORE INTO taxis (id, destination, estado) VALUES (?, ?, ?)
    ''', taxis)

    connection.commit()
    connection.close()

if __name__ == '__main__':
    create_database()
    print("Database created successfully")