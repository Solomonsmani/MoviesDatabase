import mysql.connector


# Connection to the MySQL database
connection = mysql.connector.connect(
    host='localhost',
    port = 3305,  
    user='chendamoze',           
    password='chendamoz17818',
    database='chendamoze'       
)

cursor = connection.cursor()

# Dictionary to store SQL queries for table creation
tables = {}

tables['Status'] = """ 
    CREATE TABLE IF NOT EXISTS Status (
        name VARCHAR(255) PRIMARY KEY
    );
"""


tables['Movies'] = """
    CREATE TABLE IF NOT EXISTS Movies (
        movie_id INT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        release_date DATE,
        rating FLOAT,
        rating_count INT,
        revenue INT,
        budget INT,
        duration INT,
        status VARCHAR(255),
        adult BOOL,
        original_language VARCHAR(255),
        overview VARCHAR(255),
        FOREIGN KEY (status) REFERENCES Status(name)
    );
"""


tables['Genres'] = """ 
    CREATE TABLE IF NOT EXISTS Genres (
        name VARCHAR(255) PRIMARY KEY
    );
"""

# Many-to-many relationship between movies and genres
tables['MovieGenre'] = """
    CREATE TABLE IF NOT EXISTS MovieGenre (
        movie_id INT,
        name VARCHAR(255),
        PRIMARY KEY (movie_id, name),
        FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
        FOREIGN KEY (name) REFERENCES Genres(name)
    );
"""

tables['Companies'] = """
    CREATE TABLE IF NOT EXISTS Companies (
        name VARCHAR(255) PRIMARY KEY
    );
"""

# Many-to-many relationship between movies and companies
tables['MovieCompany'] = """
    CREATE TABLE IF NOT EXISTS MovieCompany (
        movie_id INT,
        name VARCHAR(255),
        PRIMARY KEY (movie_id, name),
        FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
        FOREIGN KEY (name) REFERENCES Companies(name)
    );
"""

tables['Countries'] = """
    CREATE TABLE IF NOT EXISTS Countries (
        name VARCHAR(255) PRIMARY KEY
    );
"""

# Many-to-many relationship between movies and countries
tables['MovieCountry'] = """
    CREATE TABLE IF NOT EXISTS MovieCountry (
        movie_id INT,
        name VARCHAR(255),
        PRIMARY KEY (movie_id, name),
        FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
        FOREIGN KEY (name) REFERENCES Countries(name)
    );
"""

tables['Keywords'] = """
    CREATE TABLE IF NOT EXISTS Keywords (
        name VARCHAR(255) PRIMARY KEY
    );
"""

# Many-to-many relationship between movies and keywords
tables['MovieKeyword'] = """
    CREATE TABLE IF NOT EXISTS MovieKeyword (
        movie_id INT,
        name VARCHAR(255),
        PRIMARY KEY (movie_id, name),
        FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
        FOREIGN KEY (name) REFERENCES Keywords(name)
    );
"""

tables['Languages'] = """
    CREATE TABLE IF NOT EXISTS Languages (
        name VARCHAR(255) PRIMARY KEY
    );
"""

# Many-to-many relationship between movies and languages
tables['MovieLanguage'] = """
    CREATE TABLE IF NOT EXISTS MovieLanguage (
        movie_id INT,
        name VARCHAR(255),
        PRIMARY KEY (movie_id, name),
        FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
        FOREIGN KEY (name) REFERENCES Languages(name)
    );
"""

# Order of table creation to ensure dependencies are met
tables_order = [
    'Status',
    'Movies',
    'Genres',
    'Companies',
    'Countries',
    'Keywords',
    'Languages',
    'MovieGenre',
    'MovieCompany',
    'MovieCountry',
    'MovieKeyword',
    'MovieLanguage'
]

# Iterating over tables and executing their creation
for table_name in tables_order:
    try:
        print("Creating table:", table_name)
        cursor.execute(tables[table_name])
    except mysql.connector.Error as err:
        print("Error while creating", table_name)
        raise Exception(str(err))

# Adding indices to improve query performance    
try:
    cursor.execute("ALTER TABLE Movies ADD FULLTEXT(overview);")
    cursor.execute("ALTER TABLE Movies ADD FULLTEXT(title);")
    cursor.execute("CREATE INDEX idx_movies_release_date ON Movies(release_date);")
    cursor.execute("CREATE INDEX idx_rating ON Movies(rating);")
    cursor.execute("CREATE INDEX idx_revenue_budget ON Movies(revenue, budget);")
    cursor.execute("CREATE INDEX idx_movie_genre ON MovieGenre(name);")
    cursor.execute("CREATE INDEX idx_movie_language ON MovieLanguage(name);")
except mysql.connector.Error as err:
    print("Error while adding indices")
    raise Exception(str(err))

connection.commit()
cursor.close()
connection.close()
