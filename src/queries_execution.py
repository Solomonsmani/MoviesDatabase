import mysql.connector
from queries_db_script import query_1, query_2, query_3, query_4, query_5

def main():
    # Connection to the MySQL database    
    con = mysql.connector.connect(
        host='localhost',
        port=3305,
        user='chendamoze',
        password='chendamoz17818',
        database='chendamoze'
    )
    cursor = con.cursor()

    try:
        print("\nQuery 1: Movies containing 'New York' in their overview")
        results = query_1(cursor)
        for row in results:
            print(row)

        print("\nQuery 2: Movies with user-provided word in their title")
        word = input("Enter a word to search in movie titles: ")
        results = query_2(cursor, word)
        for row in results:
            print(row)

        print("\nQuery 3: Companies sorted by the number of movies released after 2020")
        results = query_3(cursor)
        for row in results:
            print(row)

        print("\nQuery 4: Most popular genre for each language")
        results = query_4(cursor)
        for row in results:
            print(row)

        print("\nQuery 5: Movies with rating and profit above the average")
        results = query_5(cursor)
        for row in results:
            print(row)

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        cursor.close()
        con.close()

if __name__ == "__main__":
    main()
