import unittest
from leaderboard_test_case import LeaderboardTestCase

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(LeaderboardTestCase))
    return suite
