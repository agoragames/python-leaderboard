from redis import Redis
from copy import deepcopy
from math import ceil

class Leaderboard(object):
	VERSION = '1.0.1'
	DEFAULT_PAGE_SIZE = 25
	DEFAULT_REDIS_HOST = 'localhost'
	DEFAULT_REDIS_PORT = 6379

	def __init__(self, leaderboard_name, host = DEFAULT_REDIS_HOST, port = DEFAULT_REDIS_PORT, page_size = DEFAULT_PAGE_SIZE, **redis_options):
		self.leaderboard_name = leaderboard_name
		self.host = host
		self.port = port

		if page_size < 1:
			page_size = self.DEFAULT_PAGE_SIZE

		self.page_size = page_size

		self.redis_options = deepcopy(redis_options)
		self.redis_options.setdefault('host', self.host)
		self.redis_options.setdefault('port', self.port)

		self.redis_connection = Redis(self.redis_options['host'], self.redis_options['port'], self.redis_options.get('db',0))

	def add_member(self, member, score):
		return self.redis_connection.zadd(self.leaderboard_name, member, score)

	def remove_member(self, member):
		return self.redis_connection.zrem(self.leaderboard_name, member)

	def total_members(self):
		return self.redis_connection.zcard(self.leaderboard_name)

	def total_pages(self):
		return ceil(float(self.total_members()) / self.page_size)

	def total_members_in_score_range(self, min_score, max_score):
		return self.redis_connection.zcount(self.leaderboard_name, min_score, max_score)

	def change_score_for(self, member, delta):
		return self.redis_connection.zincrby(self.leaderboard_name, member, delta)

	def rank_for(self, member, use_zero_index_for_rank = False):
		if use_zero_index_for_rank:
			return self.redis_connection.zrevrank(self.leaderboard_name, member)
		else:
			try: return self.redis_connection.zrevrank(self.leaderboard_name, member) + 1
			except: return None

	def score_for(self, member):
		return self.redis_connection.zscore(self.leaderboard_name, member)

	def check_member(self, member):
		return not None == self.redis_connection.zscore(self.leaderboard_name, member)

	def score_and_rank_for(self, member, use_zero_index_for_rank = False):
		return {'member' : member, 'score' : self.score_for(member), 'rank' : self.rank_for(member, use_zero_index_for_rank)}

	def remove_members_in_score_range(self, min_score, max_score):
		return self.redis_connection.zremrangebyscore(self.leaderboard_name, min_score, max_score)

	def leaders(self, current_page, with_scores = True, with_rank = True, use_zero_index_for_rank = False):
		if current_page < 1:
			current_page = 1
			
		tpages = self.total_pages()
			
		if current_page > tpages:
			current_page = tpages

		index_for_redis = current_page - 1

		starting_offset = (index_for_redis * self.page_size)
		if starting_offset < 0:
			starting_offset = 0

		ending_offset = (starting_offset + self.page_size) - 1

		raw_leader_data = self.redis_connection.zrevrange(self.leaderboard_name, int(starting_offset), int(ending_offset), with_scores)
		if raw_leader_data:
			return self._massage_leader_data(raw_leader_data, with_rank, use_zero_index_for_rank)
		else:
			return None

	def around_me(self, member, with_scores = True, with_rank = True, use_zero_index_for_rank = False):
		reverse_rank_for_member = self.redis_connection.zrevrank(self.leaderboard_name, member)

		starting_offset = reverse_rank_for_member - (self.page_size / 2)
		if starting_offset < 0:
			starting_offset = 0

		ending_offset = (starting_offset + self.page_size) - 1

		raw_leader_data = self.redis_connection.zrevrange(self.leaderboard_name, starting_offset, ending_offset, with_scores)
		if raw_leader_data:
			return self._massage_leader_data(raw_leader_data, with_rank, use_zero_index_for_rank)
		else:
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
