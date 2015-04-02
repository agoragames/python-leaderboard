from redis import Redis, ConnectionPool
import leaderboard
import unittest

class LeaderboardTestCase(unittest.TestCase):
    def setUp(self):
        self.leaderboard = leaderboard.Leaderboard('name')

    def tearDown(self):
        self.leaderboard.redis_connection.flushdb()

    def test_init_with_defaults(self):
        self.assertEquals('name', self.leaderboard.leaderboard_name)
        self.assertEquals( 1, len(self.leaderboard.options) )
        self.assertTrue( isinstance(self.leaderboard.options['connection_pool'],ConnectionPool) )
        self.assertTrue( isinstance(self.leaderboard.redis_connection,Redis) )
        self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE, self.leaderboard.page_size)

    def test_init_sets_page_size_to_default_if_set_to_invalid_value(self):
        self.leaderboard = leaderboard.Leaderboard('name', page_size=0)
        self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE, self.leaderboard.page_size)

    def test_init_uses_connection_pooling(self):
        lb0 = leaderboard.Leaderboard('lb0', db=0)
        lb1 = leaderboard.Leaderboard('lb1', db=0)
        lb2 = leaderboard.Leaderboard('lb2', db=1)

        self.assertEquals( lb0.redis_connection.connection_pool,
            lb1.redis_connection.connection_pool )
        self.assertNotEquals( lb0.redis_connection.connection_pool,
            lb2.redis_connection.connection_pool )

    def test_rank_member_and_total_members(self):
        self.leaderboard.rank_member('member', 1)
        self.assertEquals(1, self.leaderboard.total_members())

    def test_remove_member(self):
        self._add_members_to_leaderboard(self.leaderboard.DEFAULT_PAGE_SIZE)

        self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE, self.leaderboard.total_members())

        self.leaderboard.remove_member('member_1')
        self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE - 1, self.leaderboard.total_members())
        self.assertEquals(None, self.leaderboard.rank_for('member_1'))

    def test_clear(self):
        self._add_members_to_leaderboard(3)
        self.assertEquals(3, self.leaderboard.total_members() )
        self.leaderboard.clear()
        self.assertFalse( self.leaderboard.redis_connection.exists(
            self.leaderboard.leaderboard_name) )

    def test_total_members_in_score_range(self):
        self._add_members_to_leaderboard(5)
        self.assertEquals(3, self.leaderboard.total_members_in_score_range(2, 4))

    def test_rank_for(self):
        self._add_members_to_leaderboard(5)
        self.assertEquals(2, self.leaderboard.rank_for('member_4'))
        self.assertEquals(1, self.leaderboard.rank_for('member_4', True))

    def test_score_for(self):
        self._add_members_to_leaderboard(5)
        self.assertEquals(4, self.leaderboard.score_for('member_4'))

    def test_total_pages(self):
        self._add_members_to_leaderboard(10)
        self.assertEquals(1, self.leaderboard.total_pages())
        self.leaderboard.redis_connection.flushdb()
        self._add_members_to_leaderboard(self.leaderboard.DEFAULT_PAGE_SIZE + 1)
        self.assertEquals(2, self.leaderboard.total_pages())

    def test_leaders(self):
        self._add_members_to_leaderboard(25)
        self.assertEquals(25, self.leaderboard.total_members())

        leaders = self.leaderboard.leaders(1)

        self.assertEquals(25, len(leaders))
        self.assertEquals('member_25', leaders[0]['member'])
        self.assertEquals('member_2', leaders[-2]['member'])
        self.assertEquals('member_1', leaders[-1]['member'])
        self.assertEquals(1, leaders[-1]['score'])

    def test_leaders_ascending(self):
        lb = leaderboard.Leaderboard('asc',order='asc')

        for index in range(1, 11):
            lb.rank_member("member_%d" % index, index)

        leaders = lb.leaders(1)
        self.assertEquals(10, len(leaders))
        self.assertEquals('member_1', leaders[0]['member'])
        self.assertEquals('member_10', leaders[-1]['member'])

    def test_leaders_with_multiple_pages(self):
        self._add_members_to_leaderboard(self.leaderboard.DEFAULT_PAGE_SIZE * 3 + 1)
        self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE * 3 + 1, self.leaderboard.total_members())

        leaders = self.leaderboard.leaders(1)
        self.assertEquals(self.leaderboard.page_size, len(leaders))

        leaders = self.leaderboard.leaders(2)
        self.assertEquals(self.leaderboard.page_size, len(leaders))

        leaders = self.leaderboard.leaders(3)
        self.assertEquals(self.leaderboard.page_size, len(leaders))

        leaders = self.leaderboard.leaders(4)
        self.assertEquals(1, len(leaders))

        leaders = self.leaderboard.leaders(-5)
        self.assertEquals(self.leaderboard.page_size, len(leaders))

    # assert we get an empty page back
        leaders = self.leaderboard.leaders(10)
        self.assertEquals(None, leaders)

    def test_around_me(self):
        self._add_members_to_leaderboard(self.leaderboard.DEFAULT_PAGE_SIZE * 3 + 1)
        self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE * 3 + 1, self.leaderboard.total_members())

        leaders_around_me = self.leaderboard.around_me('member_30')
        self.assertEquals(self.leaderboard.page_size / 2, len(leaders_around_me) / 2)

        leaders_around_me = self.leaderboard.around_me('member_1')
        self.assertEquals(self.leaderboard.page_size / 2 + 1, len(leaders_around_me))

        leaders_around_me = self.leaderboard.around_me('member_76')
        self.assertEquals(self.leaderboard.page_size / 2, len(leaders_around_me) / 2)

    def test_ranked_in_list(self):
        self._add_members_to_leaderboard(self.leaderboard.DEFAULT_PAGE_SIZE)
        self.assertEquals(self.leaderboard.DEFAULT_PAGE_SIZE, self.leaderboard.total_members())

        members = ['member_1', 'member_5', 'member_10']
        ranked_members = self.leaderboard.ranked_in_list(members, True)

        self.assertEquals(3, len(ranked_members))

        self.assertEquals(25, ranked_members[0]['rank'])
        self.assertEquals(1, ranked_members[0]['score'])

        self.assertEquals(21, ranked_members[1]['rank'])
        self.assertEquals(5, ranked_members[1]['score'])

        self.assertEquals(16, ranked_members[2]['rank'])
        self.assertEquals(10, ranked_members[2]['score'])

    def test_change_score_for(self):
        self.leaderboard.rank_member('member_1', 5)
        self.assertEquals(5, self.leaderboard.score_for('member_1'))

        self.leaderboard.change_score_for('member_1', 5)
        self.assertEquals(10, self.leaderboard.score_for('member_1'))

        self.leaderboard.change_score_for('member_1', -5)
        self.assertEquals(5, self.leaderboard.score_for('member_1'))

    def test_check_member(self):
        self.leaderboard.rank_member('member_1', 10)

        self.assertEquals(True, self.leaderboard.check_member('member_1'))
        self.assertEquals(False, self.leaderboard.check_member('member_2'))

    def test_can_change_page_size_and_have_it_reflected_in_size_of_result_set(self):
        self._add_members_to_leaderboard(self.leaderboard.DEFAULT_PAGE_SIZE)

        self.leaderboard.page_size = 5
        self.assertEquals(5, self.leaderboard.total_pages())

        self.assertEquals(5, len(self.leaderboard.leaders(1)))

    def test_score_and_rank_for(self):
        self._add_members_to_leaderboard()

        data = self.leaderboard.score_and_rank_for('member_1')
        self.assertEquals('member_1', data['member'])
        self.assertEquals(1, data['score'])
        self.assertEquals(5, data['rank'])

    def test_remove_members_in_score_range(self):
        self._add_members_to_leaderboard()

        self.assertEquals(5, self.leaderboard.total_members())

        self.leaderboard.rank_member('cheater_1', 100)
        self.leaderboard.rank_member('cheater_2', 101)
        self.leaderboard.rank_member('cheater_3', 102)

        self.assertEquals(8, self.leaderboard.total_members())

        self.leaderboard.remove_members_in_score_range(100, 102)

        self.assertEquals(5, self.leaderboard.total_members())

        for leader in self.leaderboard.leaders(1):
            self.assertTrue(leader['score'] < 100)

    def test_pipeline(self):
        pipeline = self.leaderboard.pipeline()

        self._add_members_to_leaderboard(3, pipeline)

        self.assertEquals(0, self.leaderboard.total_members())
        self.leaderboard.commit(pipeline)
        self.assertEquals(3, self.leaderboard.total_members())

    def _add_members_to_leaderboard(self, members_to_add = 5, pipeline = None):
        for index in range(1, members_to_add + 1):
            self.leaderboard.rank_member("member_%d" % index, index, pipeline = pipeline)


    def test_redis_connection(self):
      lb = leaderboard.Leaderboard('name', connection=Redis())
      self.assertEquals( 0, len(lb.options) )