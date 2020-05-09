import socket 
import threading
import os
import time
import hashlib
import json

class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (host, port)
		self.predecessor = (host, port)
		# additional state variables
		self.alreadyLooked = False
		self.pingTime = 0.5
		threading.Thread(target=self.ping).start()

	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		msg = json.loads(client.recv(1024).decode("utf-8"))
		if msg["type"] == "lookup":
			node = self.lookUp(msg["key"])
			client.send(
				json.dumps({"type": "lookup", "successor": node}).encode("utf-8")
			)
		elif msg["type"] == "update_p":
			self.predecessor = (msg["predecessor"][0], msg["predecessor"][1])
			print("Predecessor: ", self.predecessor)
		elif msg["type"] == "ping":
			reqNode = (msg["req"][0], msg["req"][1])
			if reqNode == self.predecessor:
				client.send(
					json.dumps({"type": "ping", "ans": "yes"}).encode("utf-8")
				)
			else:
				client.send(
					json.dumps({"type": "ping", "ans": "yes", "res": self.predecessor}).encode("utf-8")
				)

	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def ping(self):
		# Checks whether I am your predecessor or not
		startTime = time.time()
		while not self.stop:
			while time.time() >= (startTime + self.pingTime):
				res = json.loads(self.send(
					self.successor,
					json.dumps({"type": "ping", "req": (self.host, self.port)}).encode("utf-8"),
					recv=True
				))
				if res["ans"] == "no":
					self.successor = (res["res"][0], res["res"][1])
				startTime = time.time()

	def send(self, to, msg, recv=False):
		res = None
		soc = socket.socket()
		soc.connect(to)
		soc.send(msg)
		if recv:
			res = soc.recv(1024).decode("utf-8")
		soc.close()
		return res

	def lookUp(self, key):
		# print(self.key, key)
		node = (self.host, self.port)
		# Finds the server that is responsible for the "key"
		if not self.alreadyLooked: 		# Not Looked in Cycle
			self.alreadyLooked = True 	# Mark It
			if key > self.key: 			# Not Present Here
				res = json.loads(self.send(
					self.successor,
					json.dumps({"type":"lookup", "key": key}).encode("utf-8"),
					recv=True
				))
				node = (res["successor"][0], res["successor"][1])
			# Looked Here
			self.alreadyLooked = False
		# Found from next Peers OR Present on Self
		return node 

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		# Corner Case 1: 1 Node (empty string)
		if joiningAddr:
			print((self.host, self.port), "joining:", joiningAddr)
			# Message joiningAdd to lookup for my successor
			res = json.loads(
				self.send(
					joiningAddr,
					json.dumps({"type":"lookup", "key": self.hasher(self.host+str(self.port))}).encode("utf-8"),
					recv=True
				)
			)
			# Update Successor
			self.successor = (res["successor"][0], res["successor"][1])
			print("Successor: ", self.successor)
			# Message Successor to update predecessor
			self.send(
				self.successor,
				json.dumps({"type":"update_p", "predecessor": (self.host, self.port)}).encode("utf-8")
			)
			# Get Files
			# Back Up Files

	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		pass
		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''

	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''

	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True
