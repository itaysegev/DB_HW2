import unittest
import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest

from Business.Critic import Critic
from Business.Actor import Actor
from Business.Movie import Movie
from Business.Studio import Studio

'''
    Simple test, create one of your own
    make sure the tests' names start with test
'''


class Test(AbstractTest):

    def testCritic(self) -> None:
        invalid_critic = Critic(critic_id=1, critic_name=None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addCritic(invalid_critic), "invalid name")
        invalid_critic = Critic(critic_id=None, critic_name="John")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addCritic(invalid_critic), "invalid id")
        jhon = Critic(critic_id=1, critic_name="John")
        self.assertEqual(ReturnValue.OK, Solution.addCritic(jhon), "valid critic")
        bob = Critic(critic_id=1, critic_name="Bob")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addCritic(bob), "existing id")

    def testActor(self) -> None:
        invalid_actor = Actor(actor_id=None, actor_name=None, age=-500, height=0)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addActor(invalid_actor), "invalid parameters")
        leonardo = Actor(actor_id=1, actor_name="Leonardo DiCaprio", age=48, height=183)
        self.assertEqual(ReturnValue.OK, Solution.addActor(leonardo), "should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addActor(leonardo), "already exists")

    def testMovie(self) -> None:
        invalid_movie = Movie(movie_name="Mission Impossible", year="343", genre="Action")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addMovie(invalid_movie), "invalid year")
        mission_impossible = Movie(movie_name="Mission Impossible", year="1996", genre="Action")  #note that postgreSQL will convert this string to an int
        self.assertEqual(ReturnValue.OK, Solution.addMovie(mission_impossible), "should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addMovie(mission_impossible), "already exists")

    def testStudio(self) -> None:
        invalid_studio = Studio(studio_id=None, studio_name="Warner Bros")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addStudio(invalid_studio), "invalid id")
        warner_bros = Studio(studio_id=1, studio_name="Warner Bros")
        self.assertEqual(ReturnValue.OK, Solution.addStudio(warner_bros), "should work")
        paramount = Studio(studio_id=1, studio_name="Paramount")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addStudio(paramount), "ID 1 already exists")

    def testBuildTables(self) -> None:
        names = ['Jhon Travolta', 'Tom Cruise', 'Leonardo DiCaprio', 'Stan Lee', 'Uma Thurman']
        studios = ['Paramount', 'Universal']
        for idx, name in enumerate(names):
            actor = Actor(actor_id=idx + 1, actor_name=name, age=(idx * 10 + 8), height=(180 + idx))
            self.assertEqual(ReturnValue.OK, Solution.addActor(actor), "should work")

        paramount = Studio(studio_id=2, studio_name="Paramount")
        universal = Studio(studio_id=1, studio_name="Universal")
        self.assertEqual(ReturnValue.OK, Solution.addStudio(paramount), "should work")
        self.assertEqual(ReturnValue.OK, Solution.addStudio(universal), "should work")

        mi = Movie(movie_name="Mission Impossible", year="1996", genre="Action")
        grease = Movie(movie_name="Grease", year="1978", genre="Drama")
        topgun = Movie(movie_name="Top Gun", year="1986", genre="Drama")
        topgun2 = Movie(movie_name="Top Gun", year="2022", genre="Drama")
        titanic = Movie(movie_name="Titanic", year="1997", genre="Drama")
        av = Movie(movie_name="The aviator", year="2004", genre="Action")
        hulk = Movie(movie_name="Hulk", year="2003", genre="Action")
        pulp = Movie(movie_name="Pulp Fiction", year="1994", genre="Drama")

        paramount_lst = [grease, topgun, topgun2, titanic]
        universal = [av, hulk]
        all_movies = universal + paramount_lst + [pulp, mi]

        for movie in all_movies:
            self.assertEqual(ReturnValue.OK, Solution.addMovie(movie), "should work")
        for idx, movie in enumerate(universal):
            studio_id = 1
            # Solution.studioProducedMovie(studio_id,
            #                              movie.getMovieName(), movie.getYear(), studio_id + idx, studio_id + idx)
            self.assertEqual(ReturnValue.OK, Solution.studioProducedMovie(studio_id,
                movie.getMovieName(), movie.getYear(), studio_id + idx, studio_id + idx), "should work")
            self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.studioProducedMovie(studio_id + 1, movie.getMovieName(),
                                                                          movie.getYear(), studio_id + idx,
                                                                          studio_id + idx), "should work")

        for idx, movie in enumerate(paramount_lst):
            studio_id = 2
            self.assertEqual(ReturnValue.OK, Solution.studioProducedMovie(studio_id,
                movie.getMovieName(), movie.getYear(), studio_id + idx, studio_id + idx), "should work")

        Solution.actorPlayedInMovie("Grease", 1978, 1, 20, ['Danny'])
        self.assertEqual(ReturnValue.OK, Solution.actorPlayedInMovie("Pulp Fiction", 1994, 1, 200, ['Vincent Vega'])
                         , "should work")
        Solution.actorPlayedInMovie("Top Gun", 1986, 2, 200, ['L.T Pitt Maverick'])
        Solution.actorPlayedInMovie("Top Gun", 2022, 2, 2000, ['L.T Pitt Maverick'])
        Solution.actorPlayedInMovie("Titanic", 1997, 3, 300, ['Jack', 'Bob'])
        Solution.actorPlayedInMovie("The aviator", 2004, 3, 1000, ['Howard Hughes'])
        Solution.actorPlayedInMovie("Hulk", 2003, 4, 4, ['security guard #2'])
        Solution.actorPlayedInMovie("Pulp Fiction", 1994, 5, 150, ['Mia Wallace'])

        # print(Solution.averageActorRating(10))

        names = ['Jhon Travolta', 'Tom Cruise', 'Leonardo DiCaprio', 'Stan Lee', 'Uma Thurman']
        for idx, name in enumerate(names):
            critic = Critic(idx + 1, name)
            self.assertEqual(ReturnValue.OK, Solution.addCritic(critic), "should work")

        Solution.criticRatedMovie("Grease", 1978, 1, 2)
        self.assertEqual(ReturnValue.OK, Solution.criticRatedMovie("Pulp Fiction", 1994, 1, 4)
                         , "should work")
        Solution.criticRatedMovie("Top Gun", 1986, 2, 2)
        Solution.criticRatedMovie("Top Gun", 2022, 2, 2)
        Solution.criticRatedMovie("Titanic", 1997, 3, 3)
        Solution.criticRatedMovie("The aviator", 2004, 3, 1)
        Solution.criticRatedMovie("Hulk", 2003, 4, 4)
        Solution.criticRatedMovie("Pulp Fiction", 1994, 5, 5)


        Solution.averageRating("Pulp Fiction", 1994)
        Solution.bestPerformance(1)


        Solution.addMovie(Movie(movie_name="Austin Powers", year="2002", genre="Comedy"))
        Solution.addActor(Actor(actor_id=1234, actor_name='Mike Myers', age=30, height=178))
        # print(Solution.getActorProfile(1234))
        Solution.actorPlayedInMovie("Austin Powers", 2002, 1234, 200, ['Austin Powers', 'Dr Evil',
                                                                       'Goldmember'])

        # print(Solution.actorDidntPlayInMovie("Austin Powers", 2002, 1234))

        Solution.actorPlayedInMovie("Austin Powers", 2002, 2, 20, ['Himself', 'Famous Austin'])
        Solution.addActor(Actor(actor_id=123, actor_name='Beyonce', age=27, height=174))
        Solution.actorPlayedInMovie("Austin Powers", 2002, 123, 25, ['Foxy'])
        # Solution.deleteActor(1234)
        Solution.criticRatedMovie("Grease", 1978, 3, 4)
        # Solution.printActorsInMovies()
        # print(Solution.averageRating("Grease", 1978))

        # print(Solution.averageActorRating(1))
        # Solution.printActorsInMovies()
        # print(Solution.averageAgeByGenre())
        Solution.stageCrewBudget("Austin Powers", 2002)
        Solution.overlyInvestedInMovie("Grease", 1978, 1234)
        Solution.franchiseRevenue()

        Solution.printActorsInMovies()
        print(Solution.getActorsRoleInMovie(1234, "Austin Powers", 2002))
        # print(Solution.getFanCritics())







# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)


