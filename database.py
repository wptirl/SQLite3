import sqlite3

class data_base(object):
	"""Class to handle generic sqlite3 database in Python"""

	def __init__(self, path):

		self.conn = sqlite3.connect(path)
		self.c = self.conn.cursor()

	def create_table(self, sym = ''):
		"""Creates a new table in database

		:param sym: String of table name
		:return: Truth of table creation
		"""

		# Check if table exists
		if not self.check_table(sym):
			self.c.execute('''CREATE TABLE {}'''.format(sym))
			return True
		else:
			return False

	def check_table(self, sym = ''):
		"""Check if table with given name exists

		:param sym:
		:return:
		"""

		cmd = ''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{}' '''.format(sym)
		res = self.c.execute(cmd)
		if res.fetchone()[0] == 1:
			return True

		return False


	def get_vers(self):
		"""Returns version of SQLITE database

		:return:
		"""
		self.c.execute("SELECT VERSION()")

		data = self.c.fetchone()
		if data:
			print('Version retrieved: ', data)
		else:
			print('Version not retrieved.')

	def search_for_value(self, sym_table, column_search = None, value_search = None):
		"""

		:param sym_table:
		:param column_search:
		:param value_search:
		:return:
		"""
		if value_search:
			sql = "SELECT * FROM {} WHERE '{}' = '{}'".format(sym_table, column_search, value_search)
			self.c.execute(sql)
			res = self.c.fetchall()

		else:
			sql = "SELECT * FROM {} WHERE {} IS NULL".format(sym_table, column_search)
			self.c.execute(sql)
			res = self.c.fetchall()
		return res

	def add_data_point(self, sym_table, data):
		"""
		Data format

		:param sym:
		:param data:
		:return:
		"""

		if len(self.get_column_names(sym_table)) == 0:
			return False

		sqlite_wild = '(?'
		for _ in range(len(self.get_column_names(sym_table))-1):
			sqlite_wild = sqlite_wild+', ?'
		sqlite_wild = sqlite_wild+')'

		sql = '''INSERT INTO '{}' VALUES {}'''.format(sym_table, sqlite_wild)
		self.c.execute(sql, data)
		self.conn.commit()
		return self.c.lastrowid


	def select_unmarked_data(self, sym_table):
		"""

		:param sym_table:
		:return:
		"""
		return False

	def update_where(self, sym_table, search_col = None, search_id = None, replace_column = None, replace_value = None):
		"""

		:param sym_table:
		:param search_col:
		:param search_id:
		:param replace_column:
		:param replace_value:
		:return:
		"""

		sql = "UPDATE {} SET '{}' = '{}' WHERE '{}' = '{}'".format(sym_table, replace_column, replace_value, search_col, search_id)
		print(sql)
		try:
			self.c.execute(sql)
			self.conn.commit()
		except:
			print('Could not commit.')

		return True



	def add_column(self, sym_table, column_name):
		"""

		:param sym_table:
		:param column_name:
		:return:
		"""
		if column_name not in self.get_column_names(sym_table):
			print(f'Adding {column_name}')
			sql = '''ALTER TABLE '{}' ADD COLUMN '{}' text;'''.format(sym_table, column_name)
			self.c.execute(sql)
			self.conn.commit()
			return True

		else:
			print(f'{column_name} already exists in {sym_table}')
			return False

	def get_column_names(self, sym_table):
		"""

		:return:
		"""
		columns = []
		sql = '''PRAGMA table_info({});'''.format(sym_table)
		self.c.execute(sql)
		res = self.c.fetchall()
		for result in res:
			columns.append(result[1])

		return columns

	def update_full_column(self, sym_table, column, value):
		"""


		:param sym_table:
		:param column:
		:param value:
		:return:
		"""
		return False

	def access_values_table(self, sym_table):
		"""

		:param sym_table:
		:return:
		"""
		if sym_table:
			sql = "SELECT * FROM {}".format(sym_table)
			self.c.execute(sql)
			res = self.c.fetchall()
			return res

		else:
			return []