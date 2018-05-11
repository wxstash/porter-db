# misc things

def isconnected():
	status = False
	try:
		# if requests is available, make use of it
		# no need making it a dependency
		import requests
		try:
			# attempt to reach google.com
			r = requests.get("https://google.com")
			if r.ok:
				status = True
				return status
		except:
			# whatever happens..
			# timeouts, connection errors, ssl..
			# no reliable internet connection
			return status
	except:
		# sans requests, make use of urllib3
		import urllib3
		c = urllib3.connection.HTTPConnection("google.com")
		try:
			c.connect()
			# without errors, a connection was created
			# succesfully. connect() would return None
			status = True
			return status
		except:
			# coudnt create a reliable internet connection
			return status
	
	# should all else fail
	# assume there is no internet connection
	return status
	# how reliable is this function, however

def connected(func=None, *args, **kwargs):
	def action(*args, **kwargs):
		if isconnected():
			return func(*args, **kwargs)
		else:
			pass
	return action
