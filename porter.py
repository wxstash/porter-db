# provide an interface to makeshift databases
# made with/as csv and json files

import csv
import json
import os

stash = {}
LAST_ID = 0

DB_ROOT = "./porter-db"
DB_PATH = "./porter-db/db"
DB_DEP = ["db", "temp", "log"]

if not os.path.isdir(DB_ROOT):
	os.mkdir(DB_ROOT)
	for dep in DB_DEP:
		os.mkdir(DB_ROOT + "/" + dep)

class DB(dict):
	def __init__(self):
		super().__init__(self)
	
	def new_db(self, target_db):
		# there are two places a db can be
		# in memory and an exernal file
		
		
		memory = self.keys()
		if target_db in memory:
			raise KeyError("database already exist")
		
		db_name = target_db + ".db"
		records = os.listdir("assets/db")
		if db_name in records:
			raise KeyError("database record already exists")
		
		# this in essence, is so that overwriting an
		# existing database is avoided
		
		try:
			file = open("assets/db/"+target_db+".db", "w")
			file.close()
		except:
			raise
		
		try:
			temp = open("assets/db/temp/"+target_db+".db",
			"w")
			file.close()
		except:
			raise
		
		self[target_db] = {}
	
	def load_db(self, target_db):
		# target db should exist
		records = os.listdir("assets/db")
		if target_db+".db" not in records:
			raise KeyError("database record does not exist")
		
		record = open("assets/db/"+target_db+".db", "r")
		content = json.loads(record.read())
		
		self[target_db] = content

# maintain a reference to all DataBase() objects
db = DB()

class DataBase():
	def __init__(self, target,
					new=False,
					index_id="_id",
					):
		
		if new:
			# create a new db
			db.new_db(target)
			self.content = db.get(target, None)
		else:
			# check if its already loaded
			loaded = db.keys()
			if target in loaded:
				self.content = db.get(target, None)
			else:
				# attempt loading from file
				db.load_db(target)
				self.content = db.get(target, None)
	
	def insert(self, entry):
		# write entry to DB() and temp db
		self.content[hash(entry)] = entry
	
	def update(self):
		pass
	
	def delete(self):
		pass
	
	def fetch(self):
		pass

def load_db():
	stacks = ["users.db", "requests.db", "housings.db"]
	for db in stacks:
		file = open("assets/db/"+db, "r")
		entries = file.read().splitlines()
		content = [json.loads(entry) for entry in entries]
		stash[db] = content

def reload_db():
	load_db()

def save_db():
	stacks = stash.keys()
	
	for db in stacks:
		with open("assets/db/"+db, "w") as file:
			entries = stash.get(db, "")
			for entry in entries:
				entry = json.dumps(entry)
				file.write(entry + "\n")

def save_now(entry, target_db="dump", plunge=False):
	if plunge:
		entry = json.dumps(entry)
		db = open("assets/db/"+target_db+".db", "a")
		
		db.write(entry + "\n")
		db.close()

def make_entry(entry, target_db="dump"):
	db = DataBase()
