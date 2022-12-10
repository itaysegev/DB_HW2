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

        createActor = "CREATE TABLE Actors(ActorId INTEGER PRIMARY KEY NOT NULL," \
                      "Name TEXT NOT NULL," \
                      "Age INTEGER NOT NULL," \
                      "Height INTEGER NOT NULL," \
                      "CHECK ( ActorId >0 AND Age>0 AND Height>0 ));"

        createStudio = "CREATE TABLE Studios(StudioId INTEGER PRIMARY KEY NOT NULL," \
                       "Name TEXT NOT NULL);"

        createActorOnMovie = "CREATE TABLE ActorOnMovie(MovieName TEXT NOT NULL," \
                             "Year INTEGER NOT NULL," \
                             "ActorId INTEGER NOT NULL," \
                             "Salary INTEGER NOT NULL," \
                             "FOREIGN KEY (MovieName,Year) REFERENCES Movies(MovieName,Year) ON DELETE CASCADE," \
                             "FOREIGN KEY (ActorId) REFERENCES Actors(ActorId) ON DELETE CASCADE," \
                             "PRIMARY KEY(MovieName,Year,ActorId),"\
                             "CHECK (Salary>0));"

        createActorRolesOnMovie = "CREATE TABLE ActorRolesOnMovie(MovieName TEXT NOT NULL," \
                                  "Year INTEGER NOT NULL," \
                                  "ActorId INTEGER NOT NULL," \
                                  "Role TEXT NOT NULL," \
                                  "FOREIGN KEY (MovieName,Year) REFERENCES Movies(MovieName,Year) ON DELETE CASCADE," \
                                  "FOREIGN KEY (ActorId) REFERENCES Actors(ActorId) ON DELETE CASCADE);"

        createStudioProducedMovie = "CREATE TABLE StudioProducedMovie(MovieName TEXT NOT NULL," \
                                   "Year INTEGER NOT NULL," \
                                   "StudioId INTEGER NOT NULL," \
                                   "Budget INTEGER NOT NULL," \
                                   "Revenue INTEGER NOT NULL," \
                                   "FOREIGN KEY (MovieName,Year) REFERENCES Movies(MovieName,Year) ON DELETE CASCADE," \
                                   "FOREIGN KEY (StudioId) REFERENCES Studios(StudioId) ON DELETE CASCADE," \
                                   "CHECK (Budget>0 AND Revenue>0)," \
                                   "PRIMARY KEY(MovieName,Year,StudioId));"

        createCriticRatedMovie = "CREATE TABLE CriticRatedMovie(CriticId INTEGER NOT NULL," \
                                 "MovieName TEXT NOT NULL," \
                                 "Year INTEGER NOT NULL," \
                                 "Rating INTEGER NOT NULL," \
                                 "FOREIGN KEY (CriticId) REFERENCES Critics(CriticId) ON DELETE CASCADE," \
                                 "FOREIGN KEY (MovieName,Year) REFERENCES Movies(MovieName,Year) ON DELETE CASCADE," \
                                 "CHECK (Rating>1 AND Rating<5)," \
                                 "PRIMARY KEY(CriticID,MovieName,Year));"

        query = sql.SQL(f"BEGIN; "
                        f"{createCritics}"
                        f"{createMovies}"
                        f"{createActor}"
                        f"{createStudio}"
                        f"{createActorOnMovie}"
                        f"{createActorRolesOnMovie}"
                        f"{createStudioProducedMovie}"
                        f"{createCriticRatedMovie}"
                        f"COMMIT;")
        conn.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
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

        dropActorOnMovie = "DROP TABLE ActorOnMovie;"

        dropActorRolesOnMovie = "DROP TABLE ActorRolesOnMovie;"

        dropStudioProduceMovie = "DROP TABLE StudioProducedMovie;"

        dropCriticRatedMovie = "DROP TABLE CriticRatedMovie;"

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
        print(e)
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
            f"DELETE FROM Critics WHERE CriticID = {critic_id}")
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
        query = sql.SQL(f"SELECT * FROM Critic WHERE CriticID = {critic_id}")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Critic.badCritic()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Critic.badCritic()
    return Movie(result[0]['CriticID'], result[0]['Name'])


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
        query = sql.SQL(f"SELECT * FROM Actor WHERE ActorID = {actor_id}")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Actor.badActor()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Actor.badActor()
    return Actor(result[0]['ActorID'], result[0]['Name'])


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
            f"DELETE FROM Movies WHERE MovieName = {movie_name} AMD Year = {year}")
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
        query = sql.SQL(f"SELECT * FROM Movies WHERE MovieName = {movie_name} AMD Year = {year}")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Movie.badMovie()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Movie.badMovie()
    return Movie(result[0]['MovieName'], result[0]['Year'])


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
            f"DELETE FROM Studios WHERE studioID = {studio_id} ")
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
        query = sql.SQL(f"SELECT * FROM Movies WHERE StudioID = {studio_id}")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return Studio.badStudio()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Studio.badStudio()
    return Studio(result[0]['StudioID'], result[0]['Name'])


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            f"INSERT INTO CriticRatedMovie VALUES({criticID}, {movieName}, {movieYear}, {rating})")
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
        query = sql.SQL(f"DELETE FROM CriticRatedMovie WHERE VALUES({criticID}, {movieName}, {movieYear};")
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
        insertActorOnMovie = f"INSERT INTO ActorOnMovie VALUES({movieName}, {movieYear}, {actorID}, {salary})"
        insertActorRolesOnMovie = f"INSERT INTO ActorRolesOnMovie(ActorID, MovieName, MovieYear, Rating)"\
        " VALUES " + ', '.join((actorID, movieName, movieYear, role).__str__() for role in roles)
        query = sql.SQL(f"{insertActorOnMovie};"
                        f"{insertActorRolesOnMovie}")
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


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(f"DELETE FROM ActorOnMovie WHERE VALUES({movieName}, {movieYear}, {actorID};"
                        f"DELETE FROM ActorRolesOnMovie(ActorID, MovieName, MovieYear) WHERE VALUES({movieName}, {movieYear}, {actorID};")
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


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            f"INSERT INTO studioProducedMovie VALUES({movieName}, {movieYear}, {studioID}, {budget}, {revenue}))")
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
        query = sql.SQL(f"DELETE FROM studioProducedMovie WHERE VALUES({movieName}, {movieYear}, {studioID};")
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
        query = sql.SQL(f"SELECT AVG(Rating) FROM CriticRatedMovie"
                        f" WHERE MovieName = {movieName} AND MovieYear = {movieYear}")
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return 0
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return 0
    return result[0]



def averageActorRating(actorID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        selectByActorID = f"SELECT MovieName, MovieYear FROM ActorOnMovie WHERE ActorID={actorID}"
        orderByRating = f"SELECT MovieName, MovieYear FROM CriticRatedMovie " \
                        f"WHERE MovieName, MovieYear IN ({selectByActorID}) " \

        query = sql.SQL(f"SELECT AVG(Rating) FROM CriticRatedMovie"
                        f" WHERE MovieName, MovieYear = (SELECT * FROM ({orderByRating})")

        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return 0
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return 0
    return result[0]


def bestPerformance(actor_id: int) -> Movie:
    conn = None
    try:
        conn = Connector.DBConnector()
        selectByActorID = f"SELECT MovieName, MovieYear FROM ActorOnMovie WHERE ActorID={actor_id}"
        orderByRating = f"SELECT MovieName, MovieYear FROM CriticRatedMovie " \
                        f"WHERE MovieName, MovieYear IN ({selectByActorID}) " \
                        f"ORDER BY AVG(Rating) ASC, Year DESC, MovieName ASC"

        query = sql.SQL(f"SELECT * FROM Movies"
                        f" WHERE MovieName, MovieYear = (SELECT * FROM ({orderByRating})"
                        f" ORDER BY 1)")
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return Movie.badMovie()
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return Movie.badMovie()
    return Movie(result[0]['MovieName'], result[0]['Year'])


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        buildEachMovieRolesNumber = f"SELECT ActorID, COUNT(Role) FROM ActorRolesOnMovie " \
                           f"WHERE MovieName={movie_name} AND Year={movie_year} " \
                           f"GROUP BY MovieName, Year"
        buildActorRolesNumber = f"SELECT COUNT(Role) FROM ActorRolesOnMovie " \
                           f"WHERE MovieName={movie_name} AND Year={movie_year} " \
                           f"GROUP BY MovieName, Year, ActorID"

        query = sql.SQL(f"SELECT R1.ActorID FROM ({buildEachMovieRolesNumber}) R1,"
                        f" ({buildActorRolesNumber}) R2"
                        f" WHERE R2.COUNT(Role) * 2 > R1.COUNT(Role)")
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
        buildMovies = f"SELECT MovieName, Year FROM Movies"
        buildRevenue = f"SELECT MovieName, Year, Revenue FROM StudioProducedMovie"
        joinMovieRevenue = f"SELECT MovieName, Revenue FROM ({buildRevenue}) RIGHT OUTER JOIN ({buildMovies})"
        query = sql.SQL(f"SELECT MovieName, SUM(Revenue) FROM ({joinMovieRevenue}),"
                        f"GROUP BY MovieName"
                        f" ORDER BY MovieName DESC")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return result


def studioRevenueByYear() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def getFanCritics() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def averageAgeByGenre() -> List[Tuple[str, float]]:
    # TODO: implement
    pass


def getExclusiveActors() -> List[Tuple[int, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        buildActorMovie = f"SELECT ActorID, MovieName, Year FROM ActorOnMovie"
        buildStudioMovie = f"SELECT StudioID, MovieName, Year FROM StudioProducedMovie"
        joinActorStudio = f"SELECT ActorID, StudioID FROM ({buildActorMovie}) INNER JOIN ({buildStudioMovie})"
        query = sql.SQL(f"SELECT ActorID, StudioID FROM ({joinActorStudio}),"
                        f"GROUP BY ActorID, StudioID"
                        f" HAVING COUNT(StudioID) = 1")
        rows_effected, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return None
    finally:
        if conn:
            conn.close()
    if result.isEmpty():
        return []
    return result

# GOOD LUCK!
