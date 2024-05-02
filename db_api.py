from flask import Flask, jsonify, request
import sqlite3
import pandas as pd
from datetime import date


# Получение информации для дообучения модели
def get_info_from_db():
    # библиотеки
    import sqlite3
    
    # запрос к БД
    query = """
    SELECT metro, lon, lat, num_of_floors, type, num_of_ruums, area, floor
    FROM moscow
    """ # может меняться
    
    # Подключение к БД
    con = sqlite3.connect('db_api/dataset_db.db') # может меняться

    # чтение информации из БД
    df = pd.read_sql_query(query, con)
    
    # закрытие таблиццы
    con.close()
    
    # Переименование столбцов
    df.columns = ["Metro station", "lon", "lat", "Number of floors", "type", "Number of rooms", "Area", "floor"]
    
    # возвращение полученной информации
    return df


# Очистка БД от устаревшей информации
def clear_old_info(cur):
    cursor = cur
    
    cursor.execute('SELECT id, date FROM moscow')
    results = cursor.fetchall()

    for_delete = []

    for row in results:
        date_info = (str(row[1])).split('-')
        date_ready = date(int(date_info[0]), int(date_info[1]), int(date_info[2]))
        if (int((date.today() - date_ready).days)) > 31:
            for_delete.append(int(row[0]))

    for i in for_delete:
        cursor.execute('DELETE FROM dates WHERE id = ?', [i])


# Функция очистки полученой таблицы от повторений(которые уже есть в БД)
def clear_equals_info(data, cursor):
    df = data
    cur = cursor

    cur.execute('''SELECT id_num from moscow''')    
    results = [int(*i) for i in cursor.fetchall()]

    for i in df["id"]:
        if i in results:
            df = df.drop(df[df["id"] == i].index)

    return df


app = Flask(__name__)


# Запрос на обновление БД(запись новой информации)
@app.route('/update_db', methods=['GET'])
def update_db():
    try:
        # Получение инвормации с запроса 
        data = dict(link=request.args.get("link"))

        # Читаем таблицу с новыми данными
        df = pd.read_csv(data['link'], index_col=False, names=['n','underground', 'lon', 'lat', 'num_of_floors', 'type', 'num_of_ruums', 'id', 'Area', 'floor', "price", "date"], encoding='utf-8', header=0)
        
        # Подключаемся к БД
        connection = sqlite3.connect('dataset_db.db')
        cursor = connection.cursor()

        # Чистка от повторяющихся квартир
        df = clear_equals_info(df, cursor)


        # Добавляем новые квартиры        
        for i in range(df.shape[0]):
            cursor.execute('''INSERT INTO moscow (metro, lon, lat, num_of_floors, type, num_of_ruums, id_num, area, floor, price, date) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (df['underground'][i], df['lon'][i], df['lat'][i], int(df['num_of_floors'][i]), df['type'][i],
                                                            int(df['num_of_ruums'][i]), int(df['id'][i]), df['Area'][i], int(df['floor'][i]),
                                                            int(df['price'][i]), df["date"][i]))
            
        # Сохранение(на всякий случай)
        connection.commit()

        # Удаление устаревших предложений 
        clear_old_info(cursor)
        
        # Сохраняем изменения и закрываем таблицу
        connection.commit()
        connection.close()
        
        return  jsonify({'status': f"Successfully"})
    except ValueError:
        return  jsonify({'status': f"Error!",
                         "Error": "ValueError"})
    except KeyError:
        return  jsonify({'status': f"Error!", 
                         "Error": "KeyError"})
    

# Запрос на удаление устаревших данных
@app.route('/delete_old_db', methods=['GET'])
def delete_old():
    connection = sqlite3.connect('dataset_db.db')
    cursor = connection.cursor()
    
    clear_old_info(cursor)

    connection.commit()
    connection.close()

    return  jsonify({'status': f"Old data was successfully deleted"})

# Запрос на размер БД
@app.route('/len_db', methods=['GET'])
def len_db():

    # Подключение к БД
    con = sqlite3.connect('dataset_db.db')
    cursor = con.cursor()
    
    # чтение информации из БД
    cursor.execute('SELECT id FROM moscow')
    results = cursor.fetchall()
    
    # закрытие таблиццы
    con.close()
    
    # возвращение полученной информации
    return jsonify({'len': f"{len(results)}"})



# Запрос на создание таблицы
@app.route('/create_db', methods=['GET'])
def create_db():
    connection = sqlite3.connect('dataset_db.db')
    cursor = connection.cursor()

    # Создаем таблицу Users
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS moscow (
    id INTEGER PRIMARY KEY,
    metro TEXT,
    lon REAL,
    lat REAL,
    num_of_floors INTEGER,
    type TEXT,
    num_of_ruums INTEGER,
    id_num INTEGER,
    area REAL,
    floor INTEGER,
    price INTEGER,
    date TEXT
    )
    ''')

    # Сохраняем изменения и закрываем соединение
    connection.commit()
    connection.close()

    return jsonify({'result': f"New DB was created"})


if __name__ == '__main__':
    app.run(debug=True, port=7999)