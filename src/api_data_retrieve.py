import pandas as pd
import mysql.connector
from datetime import datetime

con = mysql.connector.connect(
    host='localhost',
    port=3305,
    user='',  
    password='',         
    database= ''
)
cursor = con.cursor()

# Insert Status
def insert_status():
    try:
        cursor.execute("""
            INSERT INTO Status (name)
            VALUES (%s), (%s), (%s), (%s), (%s), (%s)
        """, ("Released", "In Production", "Post Production", "Planned", "Rumored", "Canceled"))
    except Exception as e:
            print("Error inserting status")
    con.commit()
    

def insert_into_table(cursor, table_name, values):
    placeholders = ','.join(['%s'] * len(values))
    query = f"INSERT IGNORE INTO {table_name} VALUES ({placeholders})"
    try:
        cursor.execute(query, values)
        con.commit()
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")


def load_and_process_csv():
    df = pd.read_csv('TMDB_movies.csv')
    return df

def insert_data():
    cnt = 0

    df = load_and_process_csv()
    print(df)

    # Insert Movies
    for _, row in df.iterrows():
        cnt += 1
        movie_values = (
            row['id'],
            row['title'],
            datetime.strptime(row['release_date'], "%m/%d/%Y").strftime("%Y-%m-%d"),
            row['vote_average'],
            row['vote_count'],
            row['revenue'],
            row['budget'],
            row['runtime'],
            row['status'],
            bool(row['adult']),
            row['original_language'],
            row['overview']
        )
        insert_into_table(cursor, 'Movies', movie_values)
        

        # Insert data
        if type(row['genres']) == str and len(row['genres']) != 0:
            for genre in row['genres'].split(', '):
                insert_into_table(cursor, 'Genres', (genre,))
                insert_into_table(cursor, 'MovieGenre', (row['id'], genre))

        if type(row['production_companies']) == str and len(row['genres']) != 0:
            for company in row['production_companies'].split(', '):
                insert_into_table(cursor, 'Companies', (company,))
                insert_into_table(cursor, 'MovieCompany', (row['id'], company))

        if type(row['production_countries']) == str and len(row['genres']) != 0:
            for country in row['production_countries'].split(', '):
                insert_into_table(cursor, 'Countries', (country,))
                insert_into_table(cursor, 'MovieCountry', (row['id'], country))

        if type(row['keywords']) == str and len(row['genres']) != 0:
            for keyword in row['keywords'].split(', '):
                insert_into_table(cursor, 'Keywords', (keyword,))
                insert_into_table(cursor, 'MovieKeyword', (row['id'], keyword))

        if type(row['spoken_languages']) == str and len(row['genres']) != 0:
            for language in row['spoken_languages'].split(', '):
                insert_into_table(cursor, 'Languages', (language,))
                insert_into_table(cursor, 'MovieLanguage', (row['id'], language))
        print (cnt)

    con.commit()
    con.close()

if __name__ == "__main__":
    try:
        insert_status()
        insert_data()
    finally:
        cursor.close()

    
