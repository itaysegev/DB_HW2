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
                       "CHECK (Genre IN (Drama, Action, Comedy, Horror) AND Year>=1895));"

        createActor = "CREATE TABLE Actors(ActorId INTEGER PRIMARY KEY NOT NULL," \
                      "Name TEXT NOT NULL," \
                      "Age INTEGER NOT NULL," \
                      "Height INTEGER NOT NULL," \
                      "CHECK ( ActorId >0 AND Age>0 AND Height>0 ));"

        createStudio = "CREATE TABLE Studios(StudioId INTEGER PRIMARY KEY NOT NULL," \
                       "Name TEXT NOT NULL);"

        createActorOnMovie = "CREATE TABLE ActorOnMovie(MovieId INTEGER NOT NULL," \
                             "ActorId INTEGER NOT NULL," \
                             "Salary INTEGER NOT NULL" \
                             "FOREIGN KEY (MovieId) REFERENCES Movies(MovieId) ON DELETE CASCADE," \
                             "FOREIGN KEY (ActorId) REFERENCES Actors(ActorId) ON DELETE CASCADE," \
                             "CHECK (Salary>0))" \
                             "PRIMARY KEY (MovieId,ActorId));"

        createActorRolesOnMovie = "CREATE TABLE ActorRolesOnMovie(MovieId INTEGER NOT NULL," \
                                  "ActorId INTEGER NOT NULL," \
                                  "Role INTEGER NOT NULL" \
                                  "FOREIGN KEY (MovieId) REFERENCES Movies(MovieId) ON DELETE CASCADE," \
                                  "FOREIGN KEY (ActorId) REFERENCES Actors(ActorId) ON DELETE CASCADE);"

        createStudioProduceMovie = "CREATE TABLE StudioProduceMovie(MovieId INTEGER NOT NULL," \
                                   "StudioId INTEGER NOT NULL," \
                                   "Budget INTEGER NOT NULL" \
                                   "Revenue INTEGER NOT NULL" \
                                   "FOREIGN KEY (MovieId) REFERENCES Movies(MovieId) ON DELETE CASCADE," \
                                   "FOREIGN KEY (StudioId) REFERENCES Studios(StudioId) ON DELETE CASCADE," \
                                   "CHECK (Budget>0 AND Revenue>0)," \
                                   "PRIMARY KEY(MovieId,StudioId));"

        createCriticRatedMovie = "CREATE TABLE CriticRatedMovie(CriticId INTEGER NOT NULL," \
                                 "MovieID INTEGER NOT NULL," \
                                 "Rating INTEGER NOT NULL" \
                                 "FOREIGN KEY (CriticId) REFERENCES Critics(CriticId) ON DELETE CASCADE," \
                                 "FOREIGN KEY (MovieId) REFERENCES Movies(MovieId) ON DELETE CASCADE," \
                                 "CHECK (Rating>1 AND Rating<5)," \
                                 "UNIQUE(CriticID,MovieId));"

        query = sql.SQL(f"BEGIN; "
                        f"{createCritics}"
                        f"{createMovies}"
                        f"{createActor}"
                        f"{createStudio}"
                        f"{createActorOnMovie}"
                        f"{createActorRolesOnMovie}"
                        f"{createStudioProduceMovie}"
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

        clearStudioProduceMovie = "DELETE FROM StudioProduceMovie;"

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
        dropActors = "DROP FROM Actors CASCADE;"

        dropMovies = "DROP FROM Movies CASCADE;"

        dropCritics = "DROP FROM Critics CASCADE;"

        dropStudios = "DROP FROM Studios CASCADE;"

        dropActorOnMovie = "DROP FROM ActorOnMovie;"

        dropActorRolesOnMovie = "DROP FROM ActorRolesOnMovie;"

        dropStudioProduceMovie = "DROP FROM StudioProduceMovie;"

        dropCriticRatedMovie = "DROP FROM CriticRatedMovie;"

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
    # TODO: implement
    pass


def deleteCritic(critic_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getCriticProfile(critic_id: int) -> Critic:
    # TODO: implement
    pass


def addActor(actor: Actor) -> ReturnValue:
    # TODO: implement
    pass


def deleteActor(actor_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getActorProfile(actor_id: int) -> Actor:
    # TODO: implement
    pass


def addMovie(movie: Movie) -> ReturnValue:
    # TODO: implement
    pass


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    # TODO: implement
    pass


def getMovieProfile(movie_name: str, year: int) -> Movie:
    # TODO: implement
    pass


def addStudio(studio: Studio) -> ReturnValue:
    # TODO: implement
    pass


def deleteStudio(studio_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getStudioProfile(studio_id: int) -> Studio:
    # TODO: implement
    pass


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    # TODO: implement
    pass


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    # TODO: implement
    pass


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    # TODO: implement
    pass


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    # TODO: implement
    pass


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    # TODO: implement
    pass


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    # TODO: implement
    pass


def averageActorRating(actorID: int) -> float:
    # TODO: implement
    pass


def bestPerformance(actor_id: int) -> Movie:
    # TODO: implement
    pass


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


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
    # TODO: implement
    pass

# GOOD LUCK!
