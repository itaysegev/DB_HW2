
from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor


# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        createCritics = "CREATE TABLE Critics(CriticId INTEGER PRIMARY KEY NOT NULL," \
                        "Name TEXT NOT NULL," \
                        "CHECK ( CriticId >0 ));"

        createMovies = "CREATE TABLE Movies(MovieName TEXT NOT NULL," \
                       "Year INTEGER NOT NULL," \
                       "Genre TEXT NOT NULL," \
                       "PRIMARY KEY (MovieName,Year)," \
                       "CHECK (Genre in ('Drama', 'Action', 'Comedy', 'Horror') AND Year>=1895));"

        viewMovieExists = "CREATE VIEW MovieExists AS " \
                          "SELECT MovieName, Year " \
                          "FROM Movies;"

        createActor = "CREATE TABLE Actors(ActorId INTEGER PRIMARY KEY NOT NULL," \
                      "Name TEXT NOT NULL," \
                      "Age INTEGER NOT NULL," \
                      "Height INTEGER NOT NULL," \
                      "CHECK ( ActorId >0 AND Age>0 AND Height>0 ));"

        createStudio = "CREATE TABLE Studios(StudioId INTEGER PRIMARY KEY NOT NULL," \
                       "Name TEXT NOT NULL," \
                       "CHECK ( StudioId >0 ));"

        createActorOnMovie = "CREATE TABLE ActorOnMovie(MovieName TEXT NOT NULL," \
                             "Year INTEGER NOT NULL," \
                             "ActorId INTEGER NOT NULL," \
                             "Salary INTEGER NOT NULL," \
                             "FOREIGN KEY (MovieName,Year) REFERENCES Movies(MovieName,Year) ON DELETE CASCADE," \
                             "FOREIGN KEY (ActorId) REFERENCES Actors(ActorId) ON DELETE CASCADE," \
                             "PRIMARY KEY(MovieName,Year,ActorId),"\
                             "CHECK (Salary>0));"

        viewActorOnMovie = "CREATE VIEW ActorPlayInMovie AS " \
                           "SELECT ActorId, MovieName, Year " \
                           "FROM ActorOnMovie;"

        createActorRolesOnMovie = "CREATE TABLE ActorRolesOnMovie(MovieName TEXT NOT NULL," \
                                  "Year INTEGER NOT NULL," \
                                  "ActorId INTEGER NOT NULL," \
                                  "Role TEXT NOT NULL," \
                                  "FOREIGN KEY (MovieName,Year,ActorId) REFERENCES ActorOnMovie(MovieName,Year,ActorId)" \
                                  " ON DELETE CASCADE);"

        createStudioProducedMovie = "CREATE TABLE StudioProducedMovie(MovieName TEXT NOT NULL," \
                                   "Year INTEGER NOT NULL," \
                                   "StudioId INTEGER NOT NULL," \
                                   "Budget INTEGER NOT NULL," \
                                   "Revenue INTEGER NOT NULL," \
                                   "FOREIGN KEY (MovieName,Year) REFERENCES Movies(MovieName,Year) ON DELETE CASCADE," \
                                   "FOREIGN KEY (StudioId) REFERENCES Studios(StudioId) ON DELETE CASCADE," \
                                   "CHECK (Budget>=0 AND Revenue>=0)," \
                                   "PRIMARY KEY(MovieName,Year,StudioId)," \
                                   "CONSTRAINT Movie UNIQUE (MovieName,Year));"

        viewIsStudioProducedMovie = "CREATE VIEW IsStudioProducedMovie AS " \
                                    "SELECT StudioId, MovieName, Year " \
                                    "FROM StudioProducedMovie;"

        viewMovieBudget = "CREATE VIEW MovieBudget AS " \
                          "SELECT Budget, MovieName, Year " \
                          "FROM StudioProducedMovie;"

        createCriticRatedMovie = "CREATE TABLE CriticRatedMovie(CriticId INTEGER NOT NULL," \
                                 "MovieName TEXT NOT NULL," \
                                 "Year INTEGER NOT NULL," \
                                 "Rating INTEGER NOT NULL," \
                                 "FOREIGN KEY (CriticId) REFERENCES Critics(CriticId) ON DELETE CASCADE," \
                                 "FOREIGN KEY (MovieName,Year) REFERENCES Movies(MovieName,Year) ON DELETE CASCADE," \
                                 "CHECK (Rating BETWEEN 1 AND 5)," \
                                 "PRIMARY KEY(CriticID,MovieName,Year));"

        viewCriticRatedMovie = "CREATE VIEW CriticFanOfMovie AS " \
                               "SELECT CriticId, MovieName, Year " \
                               "FROM CriticRatedMovie;"

        query = sql.SQL(f"BEGIN; "
                        f"{createCritics}"
                        f"{createMovies}"
                        f"{viewMovieExists}"
                        f"{createActor}"
                        f"{createStudio}"
                        f"{createActorOnMovie}"
                        f"{viewActorOnMovie}"
                        f"{createActorRolesOnMovie}"
                        f"{createStudioProducedMovie}"
                        f"{viewIsStudioProducedMovie}"
                        f"{viewMovieBudget}"
                        f"{createCriticRatedMovie}"
                        f"{viewCriticRatedMovie}"
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        clearActors = "DELETE FROM Actors CASCADE;"

        clearMovies = "DELETE FROM Movies CASCADE;"

        clearCritics = "DELETE FROM Critics CASCADE;"

        clearStudios = "DELETE FROM Studios CASCADE;"

        clearActorOnMovie = "DELETE FROM ActorOnMovie;"

        clearActorRolesOnMovie = "DELETE FROM ActorRolesOnMovie;"

        clearStudioProduceMovie = "DELETE FROM StudioProducedMovie;"

        clearCriticRatedMovie = "DELETE FROM CriticRatedMovie;"

        query = sql.SQL(f"BEGIN; "
                        f"{clearActors}"
                        f"{clearMovies}"
                        f"{clearCritics}"
                        f"{clearStudios}"
                        f"{clearActorOnMovie}"
                        f"{clearActorRolesOnMovie}"
                        f"{clearStudioProduceMovie}"
                        f"{clearCriticRatedMovie}"
                        f"COMMIT;")

        conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        dropActors = "DROP TABLE Actors CASCADE;"

        dropMovies = "DROP TABLE Movies CASCADE;"

        dropCritics = "DROP TABLE Critics CASCADE;"

        dropStudios = "DROP TABLE Studios CASCADE;"

        dropActorOnMovie = "DROP TABLE ActorOnMovie CASCADE;"

        dropActorRolesOnMovie = "DROP TABLE ActorRolesOnMovie;"

        dropStudioProduceMovie = "DROP TABLE StudioProducedMovie CASCADE;"

        dropCriticRatedMovie = "DROP TABLE CriticRatedMovie CASCADE;"

        query = sql.SQL(f"BEGIN; "
                        f"{dropActors}"
                        f"{dropMovies}"
                        f"{dropCritics}"
                        f"{dropStudios}"
                        f"{dropActorOnMovie}"
                        f"{dropActorRolesOnMovie}"
                        f"{dropStudioProduceMovie}"
                        f"{dropCriticRatedMovie}"
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Critics VALUES({criticId}, {Name})").format(
            criticId=sql.Literal(critic.getCriticID()),
            Name=sql.Literal(critic.getName()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def deleteCritic(critic_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Critics WHERE CriticID = {critic_id}").format(critic_id=sql.Literal(critic_id))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Critics WHERE CriticId = {critic_id}").format(
            critic_id=sql.Literal(critic_id))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Critic.badCritic()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Critic.badCritic()
    return Critic(result[0]['CriticId'], result[0]['Name'])


def addActor(actor: Actor) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Actors VALUES({ActorId}, {Name}, {Age}, {Height})").format(
            ActorId=sql.Literal(actor.getActorID()),
            Name=sql.Literal(actor.getActorName()),
            Age=sql.Literal(actor.getAge()),
            Height=sql.Literal(actor.getHeight()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def deleteActor(actor_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            f"DELETE FROM Actors WHERE ActorID = {actor_id}")
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"SELECT * FROM Actors WHERE ActorId = {actor_id}")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Actor.badActor()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Actor.badActor()
    return Actor(result[0]['ActorId'], result[0]['Name'], result[0]['Age'], result[0]['Height'])


def addMovie(movie: Movie) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Movies VALUES({MovieName}, {Year}, {Genre})").format(
            MovieName=sql.Literal(movie.getMovieName()),
            Year=sql.Literal(movie.getYear()),
            Genre=sql.Literal(movie.getGenre()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            f"DELETE FROM Movies WHERE MovieName = '{movie_name}' AND Year = {year}")
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Movies WHERE MovieName = {name} AND Year = {year}").format(
            name=sql.Literal(movie_name), year=sql.Literal(year))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Movie.badMovie()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Movie.badMovie()
    return Movie(result[0]['MovieName'], result[0]['Year'], result[0]['Genre'])


def addStudio(studio: Studio) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Studios VALUES({studioID}, {Name})").format(
            studioID=sql.Literal(studio.getStudioID()),
            Name=sql.Literal(studio.getStudioName()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def deleteStudio(studio_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            f"DELETE FROM Studios WHERE StudioId = {studio_id}")
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def getStudioProfile(studio_id: int) -> Studio:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Studios WHERE StudioId = {studio_id}").format(studio_id=sql.Literal(studio_id))
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Studio.badStudio()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Studio.badStudio()
    return Studio(result[0]['StudioId'], result[0]['Name'])


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO CriticRatedMovie VALUES({criticID}, {Name}, {Year}, {rating})").format(
            criticID=sql.Literal(criticID), Name=sql.Literal(movieName), Year=sql.Literal(movieYear),
            rating=sql.Literal(rating))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"DELETE FROM CriticRatedMovie WHERE CriticId={criticID} AND MovieName='{movieName}'"
                        f" AND Year={movieYear};")
        rows_effected, _ = conn.execute(query)

        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_actor = f"INSERT INTO ActorOnMovie VALUES ('{movieName}', {movieYear}, {actorID}, {salary});"
        values = "VALUES"
        for tup in ((movieName, movieYear, actorID, role) for role in roles):
            values += tup.__str__() + ','
        values = values[:-1] if values.endswith(',') else values + '(NULL,NULL,NULL,NULL)'
        add_role_to_actor = f"INSERT INTO ActorRolesOnMovie(MovieName, Year, ActorId, Role)  {values};"
        query = sql.SQL(f"BEGIN; "
                        f"{add_actor} "
                        f"{add_role_to_actor} "
                        f"COMMIT;")
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def actorDidntPlayInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"DELETE FROM ActorOnMovie WHERE MovieName='{movieName}' AND Year={movieYear} AND "
                        f"ActorId={actorID}")
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def getActorsRoleInMovie(actor_id: int, movie_name: str, movieYear:int) -> List[str]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"SELECT Role FROM ActorRolesOnMovie WHERE MovieName='{movie_name}' AND "
                        f"Year={movieYear} AND ActorId={actor_id} "
                        f"ORDER BY Role DESC")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return [tup[0] for tup in result.rows]


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO studioProducedMovie VALUES({Name}, {Year}, {ID}, {budget}, {revenue})").format(
            Name=sql.Literal(movieName), Year=sql.Literal(movieYear), ID=sql.Literal(studioID),
            budget=sql.Literal(budget), revenue=sql.Literal(revenue))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
    return ReturnValue.OK


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"DELETE FROM studioProducedMovie WHERE MovieName='{movieName}' AND Year={movieYear} AND "
                        f"StudioId={studioID}")
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException as e:
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(Rating) FROM CriticRatedMovie "
                        "WHERE MovieName={Name} AND Year={Year}").format(
            Name=sql.Literal(movieName), Year=sql.Literal(movieYear)
        )
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return 0
    finally:
        if conn:
            conn.close()
    if result.isEmpty() or result[0]['avg'] is None:
        return 0
    return float(result[0]['avg'])


def averageActorRating(actorID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        selectByActorID = f"SELECT MovieName, Year FROM ActorPlayInMovie WHERE ActorId={actorID}"
        joinMovieRating = f"SELECT A.MovieName AS MovieName, A.Year AS Year, AVG(COALESCE(C.Rating, 0)) AS Rating" \
                          f" FROM ({selectByActorID}) AS A " \
                          f"LEFT JOIN CriticRatedMovie AS C ON " \
                          f"A.MovieName=C.MovieName AND A.Year=C.Year " \
                          f"GROUP BY A.MovieName ,A.Year"
        orderByRating = f"SELECT AVG(Rating) AS AVG FROM ({joinMovieRating}) AS J " \
                        f"WHERE (MovieName, Year) IN ({selectByActorID})"
        query = sql.SQL(orderByRating)
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return 0
    finally:
        if conn:
            conn.close()
    if result.isEmpty() or not result[0]['AVG']:
        return 0
    return float(result[0]['AVG'])


def bestPerformance(actor_id: int) -> Movie:
    conn = None
    try:
        conn = Connector.DBConnector()
        selectByActorID = f"SELECT MovieName, Year FROM ActorPlayInMovie WHERE ActorID={actor_id}"
        joinActorRating = f"SELECT A.MovieName AS MovieName, A.Year AS Year, COALESCE(C.Rating, 0) AS Rating" \
                          f" FROM ({selectByActorID}) AS A " \
                          f"LEFT JOIN CriticRatedMovie AS C ON " \
                          f"A.MovieName=C.MovieName AND A.Year=C.Year"

        orderByRating = f"SELECT MovieName, Year FROM ({joinActorRating}) AS R " \
                        f"GROUP BY MovieName, Year "\
                        f"ORDER BY AVG(Rating) DESC, Year ASC, MovieName DESC " \
                        f"LIMIT 1"
        query = sql.SQL(f"SELECT * FROM Movies"
                        f" WHERE (MovieName, Year) IN ({orderByRating})")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Movie.badMovie()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Movie.badMovie()
    return Movie(result[0]['MovieName'], result[0]['Year'], result[0]['Genre'])


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        buildSalaryAmount = f"SELECT MovieName, Year, Sum(Salary) FROM ActorOnMovie " \
                            f"WHERE MovieName='{movieName}' AND Year={movieYear} " \
                            f"GROUP BY MovieName, Year"
        buildBudget = f"SELECT * FROM MovieBudget " \
                      f"WHERE MovieName='{movieName}' AND Year={movieYear}"
        buildAllMovies = f"SELECT * FROM MovieExists " \
                          f"WHERE MovieName='{movieName}' AND Year={movieYear}"

        joinMovieBudget = f"SELECT M.MovieName, M.Year, COALESCE(B.Budget, 0) AS Budget FROM ({buildAllMovies}) AS M " \
                          f"LEFT JOIN ({buildBudget}) AS B ON" \
                          f" M.MovieName=B.MovieName AND M.Year=B.Year"

        query = sql.SQL(f"SELECT (M.Budget - COALESCE(S.Sum, 0)) AS Diff FROM ({joinMovieBudget}) AS M "
                        f"LEFT JOIN ({buildSalaryAmount}) AS S ON "
                        f"M.MovieName=S.MovieName AND M.Year=S.Year")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return -1
    return int(result[0]['Diff'])


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        buildEachMovieRolesNumber = f"SELECT ActorID, MovieName, Year, COUNT(Role) FROM ActorRolesOnMovie " \
                           f"WHERE MovieName='{movie_name}' AND Year={movie_year} AND ActorID={actor_id} " \
                           f"GROUP BY ActorID, MovieName, Year"
        buildActorRolesNumber = f"SELECT MovieName, Year, COUNT(Role) AS TotalCount FROM ActorRolesOnMovie " \
                           f"WHERE MovieName='{movie_name}' AND Year={movie_year} " \
                           f"GROUP BY MovieName, Year"
        query = sql.SQL(f"SELECT ActorID FROM ({buildEachMovieRolesNumber}) AS R1 INNER JOIN"
                        f" ({buildActorRolesNumber}) AS R2 "
                        f"ON R1.MovieName=R2.MovieName AND R1.Year=R2.Year "
                        f"WHERE 2* COUNT >= TotalCount")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return False
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return False
    return True


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        buildRevenue = f"SELECT MovieName, Year, Revenue FROM StudioProducedMovie"

        joinMovieRevenue = f"SELECT M.MovieName, COALESCE(SUM(Revenue), 0) FROM ({buildRevenue}) AS R RIGHT OUTER JOIN" \
                           f" MovieExists AS M " \
                           f"ON R.MovieName=M.MovieName AND R.Year=M.Year " \
                           f"GROUP BY M.MovieName " \
                           f"ORDER BY M.MovieName DESC"

        query = sql.SQL(joinMovieRevenue)
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return result.rows


def studioRevenueByYear() -> List[Tuple[int, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()

        buildRevenue = f"SELECT StudioId, Year, SUM(Revenue) FROM StudioProducedMovie" \
                       f" GROUP BY StudioId, Year" \
                       f" ORDER BY StudioId DESC, Year DESC"
        query = sql.SQL(buildRevenue)
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return result.rows


def getFanCritics() -> List[Tuple[int, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        buildNumOfMovies = f"SELECT StudioId, COUNT((MovieName, Year)) AS Count FROM IsStudioProducedMovie " \
                           f"GROUP BY StudioId"
        buildNumOfCritics = f"SELECT CriticId, StudioId, COUNT((C.MovieName, C.Year)) AS Count FROM" \
                            f" CriticFanOfMovie AS C" \
                            f" INNER JOIN IsStudioProducedMovie AS S" \
                            f" ON C.MovieName=S.MovieName AND C.Year=S.Year" \
                            f" GROUP BY CriticId, StudioId"
        joinMoviesCritics = f"SELECT C.CriticId, C.StudioId FROM ({buildNumOfCritics}) AS C INNER JOIN" \
                            f" ({buildNumOfMovies}) AS M ON" \
                            f" C.StudioId=M.StudioId" \
                            f" WHERE C.Count=M.Count AND C.Count > 0" \
                            f" ORDER BY C.CriticId DESC, C.StudioId DESC"
        query = sql.SQL(joinMoviesCritics)
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return result.rows


def averageAgeByGenre() -> List[Tuple[str, float]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        buildActor = f"SELECT ActorId, Age FROM Actors"
        buildGenre = f"SELECT MovieName, Year, Genre FROM Movies"
        joinMovieActor = f"SELECT M.ActorId, M.MovieName, M.Year, A.Age FROM ActorPlayInMovie As M " \
                         f"INNER JOIN ({buildActor}) As A" \
                         " ON M.ActorId=A.ActorId"
        joinAgeGenre = f"SELECT DISTINCT M.Genre AS Genre, A.Age AS Age, A.ActorId FROM ({joinMovieActor}) AS A " \
                       f"INNER JOIN ({buildGenre}) AS M " \
                       f"ON A.MovieName=M.MovieName AND A.Year=M.Year"
        query = sql.SQL(f"SELECT Genre, AVG(Age) FROM ({joinAgeGenre}) AS A "
                        f"GROUP BY Genre "
                        f"ORDER BY Genre ASC")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return [(tup[0], float(tup[1])) for tup in result.rows]


def getExclusiveActors() -> List[Tuple[int, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        joinActorStudio = f"SELECT DISTINCT ActorID, StudioID FROM ActorPlayInMovie AS a" \
                          f" LEFT JOIN IsStudioProducedMovie AS s " \
                          "ON a.MovieName=s.MovieName AND a.Year=s.Year "
        buildIndActor = f"SELECT ActorID FROM ({joinActorStudio}) AS j " \
                        f"WHERE j.StudioID IS NULL"
        exclusiveActor = f"SELECT ActorID FROM ({joinActorStudio}) AS j "\
                         f"WHERE j.ActorID NOT IN ({buildIndActor}) "\
                         f"GROUP BY ActorID "\
                         f"HAVING COUNT(StudioID) = 1"

        query = sql.SQL(f"SELECT ActorID, StudioID FROM ({joinActorStudio}) AS a "
                        f"WHERE ActorID IN ({exclusiveActor}) "
                        f"ORDER BY ActorID DESC")

        rows_effected, result = conn.execute(query)

        conn.commit()
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return result.rows

# GOOD LUCK!
