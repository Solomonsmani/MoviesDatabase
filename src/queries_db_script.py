

def query_1(cursor):
    "Full-text query: Search for movies containing the word 'New York' in their overview."
    sql = """
    SELECT title, overview
    FROM Movies
    WHERE MATCH(overview) AGAINST('"New York"');
    """
    cursor.execute(sql)
    return cursor.fetchall()

def query_2(cursor, word):
    "Full-text query: Search for movies with the user-provided word in their title."
    sql = """
    SELECT title
    FROM Movies
    WHERE MATCH(title) AGAINST(%s);
    """
    cursor.execute(sql, (word,))
    return cursor.fetchall()

def query_3(cursor):
    "Companies sorted by the number of movies they released after 2020."
    sql = """
    SELECT mc.name AS company, COUNT(*) AS movie_count
    FROM MovieCompany mc, Movies m
    WHERE m.movie_id = mc.movie_id
    AND m.status = "Released"
    AND m.release_date >= '2020-01-01'
    GROUP BY mc.name
    ORDER BY movie_count DESC;

    """
    cursor.execute(sql)
    return cursor.fetchall()


def query_4(cursor):
    "Find the most popular genre for each language."
    sql = """
    SELECT ml.name AS language, g.name AS genre, COUNT(*) AS genre_count
    FROM Movies m
    JOIN MovieGenre mg ON m.movie_id = mg.movie_id
    JOIN Genres g ON mg.name = g.name
    JOIN MovieLanguage ml ON m.movie_id = ml.movie_id
    GROUP BY ml.name, g.name
    HAVING COUNT(*) = (
        SELECT MAX(genre_count)
        FROM (
            SELECT ml1.name AS language, g1.name AS genre, COUNT(*) AS genre_count
            FROM Movies m1
            JOIN MovieGenre mg1 ON m1.movie_id = mg1.movie_id
            JOIN Genres g1 ON mg1.name = g1.name
            JOIN MovieLanguage ml1 ON m1.movie_id = ml1.movie_id
            GROUP BY ml1.name, g1.name
        ) AS genre_counts
        WHERE genre_counts.language = ml.name
    )
    ORDER BY genre_count DESC, language;
    """
    cursor.execute(sql)
    return cursor.fetchall()


def query_5(cursor):
    "Find movies with rating and profit above the average."
    sql = """ SELECT m.title, m.rating, (m.revenue - m.budget) AS profit
    FROM Movies m
    WHERE m.revenue > 0
    AND m.rating > (
        SELECT AVG(m1.rating)
        FROM Movies m1
        WHERE m1.revenue > 0)
    AND (m.revenue - m.budget) > (
        SELECT AVG(m2.revenue - m2.budget)
        FROM Movies m2
        WHERE m2.revenue > 0
    )
    ORDER BY m.rating DESC, profit DESC;

    """
    cursor.execute(sql)
    return cursor.fetchall()
    


