from redis import Redis, ConnectionPool
from copy import deepcopy
from math import ceil


class Leaderboard(object):
    VERSION = '1.1.6'
    DEFAULT_PAGE_SIZE = 25
    DEFAULT_REDIS_HOST = 'localhost'
    DEFAULT_REDIS_PORT = 6379
    DEFAULT_REDIS_DB = 0
    ASC = 'asc'
    DESC = 'desc'

    @classmethod
    def pool(self, host, port, db, pools={}):
        '''
        Fetch a redis conenction pool for the unique combination of host
        and port. Will create a new one if there isn't one already.
        '''
        key = (host,port,db)
        rval = pools.get( key )
        if not isinstance(rval,ConnectionPool):
            rval = ConnectionPool(host=host, port=port, db=db)
            pools[ key ] = rval
        return rval

    def __init__(self, leaderboard_name, **options):
        '''
        Initialize a connection to a specific leaderboard. By default, will use a
        redis connection pool for any unique host:port:db pairing.

        The options and their default values (if any) are:

        host : the host to connect to if creating a new handle ('localhost')
        port : the port to connect to if creating a new handle (6379)
        db : the redis database to connect to if creating a new handle (0)
        page_size : the default number of items to return in each page (25)
        connection : an existing redis handle if re-using for this leaderboard
        connection_pool : redis connection pool to use if creating a new handle
        '''
        self.leaderboard_name = leaderboard_name
        self.options = deepcopy(options)

        self.page_size = self.options.pop('page_size', self.DEFAULT_PAGE_SIZE)
        if self.page_size < 1:
            self.page_size = self.DEFAULT_PAGE_SIZE

        self.order = self.options.pop('order', self.DESC).lower()
        if not self.order in [self.ASC, self.DESC]:
            raise ValueError("%s is not one of [%s]" % (self.order,  ",".join([self.ASC, self.DESC])))

        self.redis_connection = self.options.pop('connection',None)
        if not isinstance(self.redis_connection,Redis):
            if 'connection_pool' not in self.options:
                self.options['connection_pool'] = self.pool(
                    self.options.pop('host', self.DEFAULT_REDIS_HOST),
                    self.options.pop('port', self.DEFAULT_REDIS_PORT),
                    self.options.pop('db', self.DEFAULT_REDIS_DB)
                )
            self.redis_connection = Redis(**self.options)

    @classmethod
    def get_connection(self, **options):
        redis_options = {}
        redis_options['connection_pool'] = self.pool(
            options.pop('host', self.DEFAULT_REDIS_HOST),
            options.pop('port', self.DEFAULT_REDIS_PORT),
            options.pop('db', self.DEFAULT_REDIS_DB)
        )
        return Redis(**redis_options)

    def _get(self, **options):
        if 'pipeline' in options and options['pipeline'] != None:
            return options['pipeline']
        return self.redis_connection

    def pipeline(self):
        return self.redis_connection.pipeline()

    def commit(self, pipeline):
        pipeline.execute()

    def rank_member(self, member, score, **options):
        # redis-py deprecated the non-kwarg form of zadd
        return self._get(**options).zadd(self.leaderboard_name, **{str(member):score})

    def remove_member(self, member, **options):
        return self._get( **options).zrem(self.leaderboard_name, str(member))

    def clear(self, **options):
        '''Remove all rankings for this leaderboard.'''
        self._get(**options).delete(self.leaderboard_name)

    def total_members(self, **options):
        return self._get(**options).zcard(self.leaderboard_name)

    def total_pages(self, **options):
        return ceil(float(self.total_members(**options)) / options.get('page_size',self.page_size))

    def total_members_in_score_range(self, min_score, max_score, **options):
        return self._get(**options).zcount(self.leaderboard_name, min_score, max_score)

    def change_score_for(self, member, delta, **options):
        return self._get(**options).zincrby(self.leaderboard_name, str(member), delta)

    def rank_for(self, member, use_zero_index_for_rank = False, **options):
        try:
            return self._rank_method(self._get(**options), self.leaderboard_name, str(member))\
                + (0 if use_zero_index_for_rank else 1)
        except: return None

    def score_for(self, member, **options):
        return self._get(**options).zscore(self.leaderboard_name, str(member))

    def check_member(self, member, **options):
        return not None == self._get(**options).zscore(self.leaderboard_name, str(member))

    def score_and_rank_for(self, member, use_zero_index_for_rank = False, **options):
        return {
            'member' : member,
            'score' : self.score_for(member, **options),
            'rank' : self.rank_for(member, use_zero_index_for_rank, **options)
        }

    def remove_members_in_score_range(self, min_score, max_score, **options):
        return self._get(**options).zremrangebyscore(self.leaderboard_name, min_score, max_score)

    def leaders(self, current_page, with_scores = True, with_rank = True, use_zero_index_for_rank = False, **options):
        if current_page < 1:
            current_page = 1

        page_size = options.get('page_size',self.page_size)
        tpages = self.total_pages(page_size=page_size)

        index_for_redis = current_page - 1

        starting_offset = (index_for_redis * page_size)
        if starting_offset < 0:
            starting_offset = 0

        ending_offset = (starting_offset + page_size) - 1

        raw_leader_data = self._range_method(self._get(**options), self.leaderboard_name, int(starting_offset), int(ending_offset), with_scores)
        if raw_leader_data:
            return self._massage_leader_data(raw_leader_data, with_rank, use_zero_index_for_rank)
        else:
            return None

    def around_me(self, member, with_scores = True, with_rank = True, use_zero_index_for_rank = False, **options):
        reverse_rank_for_member = \
            self._rank_method(self._get(**options), self.leaderboard_name, str(member))

        if not reverse_rank_for_member is None:
            page_size = options.get('page_size',self.page_size)
            starting_offset = reverse_rank_for_member - (page_size / 2)
            if starting_offset < 0:
                starting_offset = 0

            ending_offset = (starting_offset + page_size) - 1

            raw_leader_data = self._range_method(self._get(**options), self.leaderboard_name, starting_offset, ending_offset, with_scores)
            if raw_leader_data:
                return self._massage_leader_data(raw_leader_data, with_rank, use_zero_index_for_rank)

        return None

    def ranked_in_list(self, members, with_scores = True, use_zero_index_for_rank = False):
        ranks_for_members = []

        for member in members:
            data = {}
            data['member'] = member
            data['rank'] = self.rank_for(member, use_zero_index_for_rank)
            if with_scores:
                data['score'] = self.score_for(member)

            ranks_for_members.append(data)

        return ranks_for_members

    def _range_method(self, connection, *args, **kwargs):
        if self.order == self.DESC:
            return connection.zrevrange(*args, **kwargs)
        else:
            return connection.zrange(*args, **kwargs)

    def _rank_method(self, connection, *args, **kwargs):
        if self.order == self.DESC:
            return connection.zrevrank(*args, **kwargs)
        else:
            return connection.zrank(*args, **kwargs)

    def _massage_leader_data(self, leaders, with_rank, use_zero_index_for_rank):
        member_attribute = True
        leader_data = []

        for leader_data_item in leaders:
            data = {}
            data['member'] = leader_data_item[0]
            data['score'] = leader_data_item[1]
            if with_rank:
                data['rank'] = self.rank_for(data['member'], use_zero_index_for_rank)

            leader_data.append(data)

        return leader_data
