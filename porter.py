# provide an interface to makeshift databases
# made with/as csv and json files

import csv
import json
import os
import time

stash = {}
LAST_ID = 0

DB_ROOT = "./porter-db/"
DB_PATH = "./porter-db/db/"
TEMP_PATH = "./porter-db/temp/"
LOG_PATH = "./porter-db/log/"
DB_DEP = ["db", "temp", "log"]

if not os.path.isdir(DB_ROOT):
	os.mkdir(DB_ROOT)
	for dep in DB_DEP:
		os.mkdir(DB_ROOT + dep)

class DB(dict):
	def __init__(self):
		super().__init__(self)
	
	def new_db(self, target_db, meta={}):
		# there are two places a db can be
		# in memory and an exernal file
		
		if target_db in self.keys():
			return KeyError("database already exist")
		
		record_name = target_db + ".db"
		records = os.listdir(DB_PATH)
		if record_name in records:
			return KeyError("database record already exists")
		
		# this in essence, is so that overwriting an
		# existing database is avoided
		
		try:
			os.mknod(DB_PATH + record_name)
			os.mknod(TEMP_PATH + target_db+".temp")
			os.mknod(LOG_PATH + target_db+".log")
		except:
			raise
		
		with open(DB_PATH+record_name, "w") as blank:
			p_meta = json.dumps(meta)
			foundation = """{"meta": %s, "content": {}, "edit_history": 0}""" %(p_meta)
			blank.write(foundation + "\n")
		
		# log new db event
		# log template build event
		
		# load the new db
		self.load_db(target_db)
	
	def load_db(self, target_db):
		# target db should exist
		records = os.listdir(DB_PATH)
		record_name = target_db + ".db"
		
		if record_name not in records:
			raise KeyError("database record does not exist")
		
		record_db = open(DB_PATH + record_name, "r")
		record = json.loads(record_db.read())
		
		self[target_db] = record
	
	def rebuild_db(self, target_db):
		pass

# maintain a reference to all DataBase() objects
db = DB()

class DataBase():
	def __init__(self, target_db, new=False, index_by="_id"):
		if new:
			# create a new db
			meta = {"index_by": index_by}
			if index_by == "_id":
				meta["insert_id"] = 0
			
			db.new_db(target_db, meta=meta)
		else:
			try:
				# check if its already loaded
				if target_db in db.keys():
					pass
				else:
					# load from file
					db.load_db(target_db)
			except:
				self.__init__(target_db, new=True, index_by=index_by)
		
		self.events = EventHandler(target_db)
		self.meta = db[target_db].get("meta")
		self.content = db[target_db].get("content")
	
	def insert(self, entry):
		if self.meta["index_by"] == "_id":
			indexer = self.meta["insert_id"]
			self.meta["insert_id"] += 1
		else:
			indexer = entry.get(self.meta.get("index_by"))
			if not indexer:
				return KeyError(f"""index key "{self.meta['index_by']}" not present""")
		
		self.content[indexer] = entry
		self.events.insert(indexer, entry)
	
	def update(self, id, entry):
		if self.content.get(id):
			self.content[id].update(entry)
			self.events.update(id, entry)
		else:
			return KeyError(f"no entry at {id}")
	
	def delete(self, id):
		if self.content.get(id):
			_ = self.content.pop(id)
			self.events.delete(id)
		else:
			return KeyError(f"{id} does not exist")
	
	def fetch(self, id):
		return self.content.get(id)
	
	def save(self):
		pass
	
	def rollback(self, to=-1):
		pass

class EventHandler():
	def __init__(self, target_log):
		
		# opening an event handler opens a connection to its log file
		self.log_file = LOG_PATH + target_log + ".log"
		
		if not os.path.isfile(self.log_file):
			raise FileNotFoundError("log file not found")
	
	def get_time(self):
		yr = str(time.localtime()[0])[2:]
		record_time = time.strftime(f"{yr}%m%d:%H%M%S")
		return record_time
	
	def transaction(self, id, entry, action="pass"):
		with open(self.log_file, "a") as logger:
			log = {
				"transaction": entry,
				"transaction_id": id,
				"time": self.get_time(),
				"action": action
			}
			log = json.dumps(log)
			logger.write(log + "\n")
	
	def insert(self, id, entry):
		self.transaction(id, entry, action="insert")
	
	def update(self, id, entry):
		self.transaction(id, entry, action="update")
	
	def delete(self, id):
		self.transaction(id, None, action="delete")
