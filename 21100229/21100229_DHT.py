'''
	I have stored backup files at the predecessor node so that at pinging the successor it
	could know whether it is killed or not. And on killing it could send the request to the 
	next successor to update the predecessor to self and send the backup files to new successor
	because they hash to it's space of ring now
'''

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
		self.pingTime = 0.5
		self.path = self.host+"_"+str(self.port)
		self.nextSuccessor = (host, port)
		# print("Self: ", self.port, self.key)

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
		elif msg["type"] == "getSucc":
			client.send(
				json.dumps({"type": "getSucc", "successor": self.successor}).encode("utf-8")
			)
		elif msg["type"] == "update_p":
			self.predecessor = (msg["predecessor"][0], msg["predecessor"][1])
			client.send(("ok").encode("utf-8"))
		elif msg["type"] == "update_s":
			self.successor = (msg["successor"][0], msg["successor"][1])
			client.send(("ok").encode("utf-8"))
		elif msg["type"] == "update_n":
			self.nextSuccessor = (msg["res"][0], msg["res"][1])
		elif msg["type"] == "ping":
			res = {"type":"ping", "ans":True, "nextAns":True}
			reqNode = (msg["req"][0], msg["req"][1])
			nextNode = (msg["next"][0], msg["next"][1])
			if nextNode != self.successor:
				res["nextAns"] = False
				res["next"] = self.successor
			if reqNode != self.predecessor:
				res["ans"] = False
				res["res"] = self.predecessor
			client.send(
				json.dumps(res).encode("utf-8")
			)
			# Join Corner Case 2
			if self.predecessor != self.successor and self.successor == (self.host, self.port):
				self.successor = self.predecessor
				self.send(
					self.predecessor,
					json.dumps({"type":"update_n", "res":self.successor}).encode("utf-8")
				)
			elif self.predecessor != self.successor and self.predecessor == (self.host, self.port):
				self.predecessor = self.successor
			# print("Self: ", self.port, "---S: ", self.successor, "---P: ", self.predecessor)
		elif msg["type"] == "put":
			# Recieve File
			self.files.append(msg["file"])
			client.send(("ok").encode("utf-8"))
			self.recieveFile(client, os.path.join(self.host+"_"+str(self.port), msg["file"]))
			# Send Back Up File
			soc = socket.socket()
			soc.connect(self.predecessor)
			soc.send(
				json.dumps({"type":"put_b", "file": msg["file"]}).encode("utf-8")
			)
			soc.recv(1024).decode("utf-8")
			self.sendFile(soc, os.path.join(self.host+"_"+str(self.port), msg["file"]))
			soc.close()
		elif msg["type"] == "put_b":
			self.backUpFiles.append(msg["file"])
			client.send(("ok").encode("utf-8"))
			self.recieveFile(client, os.path.join(self.host+"_"+str(self.port), msg["file"]))
		elif msg["type"] == "get":
			for f in self.files:
				if msg["file"] == f:
					client.send(json.dumps({"type":"get", "file":f}).encode("utf-8"))
					client.recv(1024).decode("utf-8")
					self.sendFile(client, os.path.join(self.host+"_"+str(self.port), f))
					return
			client.send(json.dumps({"type":"get", "file":None}).encode("utf-8"))
		elif msg["type"] == "rehash":
			temp = list()
			for f in self.files:
				# print(self.port, f)
				if self.hasher(f) <= self.hasher(msg["hash"][0]+str(msg["hash"][1])):
					# print("yes")
					client.send(json.dumps(
						{"found": True, "file": f}
					).encode("utf-8"))
					client.recv(1024).decode("utf-8")
					self.sendFile(client, os.path.join(self.path, f))
					client.recv(1024).decode("utf-8")
					temp.append(f)
			for f in temp:
				self.files.remove(f)
			client.send(json.dumps(
				{"found": False}
			).encode("utf-8"))
		elif msg["type"] == "rehashBackup":
			temp = list()
			for f in self.backUpFiles:
				# print(self.port, f)
				if self.hasher(f) > self.hasher(msg["hash"][0]+str(msg["hash"][1])):
					# print("yes")
					client.send(json.dumps(
						{"found": True, "file": f}
					).encode("utf-8"))
					client.recv(1024).decode("utf-8")
					self.sendFile(client, os.path.join(self.path, f))
					client.recv(1024).decode("utf-8")
					temp.append(f)
			for f in temp:
				self.backUpFiles.remove(f)
			client.send(json.dumps(
				{"found": False}
			).encode("utf-8"))
		elif msg["type"] == "leave":
			num = msg["num"]
			client.send(("ok").encode("utf-8"))
			for _ in range(num):
				fileName = client.recv(1024).decode("utf-8")
				if msg["backup"]:
					self.backUpFiles.append(fileName)
				else:
					self.files.append(fileName)
				client.send(("ok").encode("utf-8"))
				self.recieveFile(client, os.path.join(self.path, fileName))
		elif msg["type"] == "kill":
			self.predecessor = (msg["sender"][0], msg["sender"][1])
			client.send(("ok").encode("utf-8"))
			while True:
				res = json.loads(client.recv(1024).decode("utf-8"))
				if not res["found"]:
					break
				self.files.append(res["file"])
				client.send(("ok").encode("utf-8"))
				self.recieveFile(client, os.path.join(self.path, res["file"]))
				client.send(("ok").encode("utf-8"))

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
		'''
			Checks whether I am your predecessor or not
		'''
		startTime = time.time()
		while not self.stop:
			while time.time() >= (startTime + self.pingTime):
				numPings = 3
				pinged = False
				while numPings:
					try:
						res = json.loads(self.send(
							self.successor,
							json.dumps({"type": "ping", "req": (self.host, self.port), "next":self.nextSuccessor}).encode("utf-8"),
							recv=True
						))
						# print(res)
						if not res["ans"]:
							self.successor = (res["res"][0], res["res"][1])
							self.send(
								self.predecessor,
								json.dumps({"type":"update_n", "res":self.successor}).encode("utf-8")
							)
							# Message Successor to update predecessor
							# print("Self: ", self.port, "---", self.successor)
							self.send(
								self.successor,
								json.dumps({"type":"update_p", "predecessor": (self.host, self.port)}).encode("utf-8"),
								recv=True
							)
						if not res["nextAns"]:
							self.nextSuccessor = (res["next"][0], res["next"][1])
						pinged = True
						break
					except:
						numPings -= 1
					# print("Self: ", self.port, "-- S: ", self.successor, "-- P: ", self.predecessor)
					if not pinged:
						self.successor = self.nextSuccessor
						soc = socket.socket()
						soc.connect(self.successor)
						soc.send(
							json.dumps({"type":"kill", "sender":(self.host, self.port)}).encode("utf-8")
						)
						soc.recv(1024).decode("utf-8")
						for f in self.backUpFiles:
							soc.send(json.dumps(
								{"found": True, "file": f}
							).encode("utf-8"))
							soc.recv(1024).decode("utf-8")
							self.sendFile(soc, os.path.join(self.path, f))
							soc.recv(1024).decode("utf-8")
						soc.send(json.dumps(
							{"found": False}
						).encode("utf-8"))
						soc.close()
				startTime = time.time()

	def send(self, to, msg, recv=False):
		'''
			Sends msg to the given node
		'''
		res = None
		soc = socket.socket()
		soc.connect(to)
		soc.send(msg)
		if recv:
			res = soc.recv(1024).decode("utf-8")
		soc.close()
		return res

	def lookUp(self, key):
		'''
			Finds the node responsible for the key
		'''
		# Finds the server that is responsible for the "key"
		succ = self.hasher(self.successor[0]+str(self.successor[1]))
		# print("Self: ", self.key, "key: ", key, "succ: ", succ)
		# Ideal Condition
		if self.key < key <= succ:
			return self.successor
		# Last Part of ring
		elif self.key >= succ:
			if key < self.key and key > succ:
				return (self.host, self.port)
			return self.successor
		else:
			res = json.loads(self.send(
				self.successor,
				json.dumps({"type":"lookup", "key": key}).encode("utf-8"),
				recv=True
			))
			return (res["successor"][0], res["successor"][1])

	def join(self, joiningAddr):
		'''
			Joins Network
		'''
		# Corner Case 1: 1 Node (empty string)
		threading.Thread(target=self.ping).start()
		if joiningAddr:
			# print((self.host, self.port), "joining:", joiningAddr)
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

			# Message Successor to update predecessor
			self.send(
				self.successor,
				json.dumps({"type":"update_p", "predecessor": (self.host, self.port)}).encode("utf-8"),
				recv=True
			)
			# Get Files (Provide me my share of files)
			soc = socket.socket()
			soc.connect(self.successor)
			soc.send(
				json.dumps({"type":"rehash", "hash": (self.host, self.port)}).encode("utf-8")
			)
			while True:
				msg = soc.recv(1024).decode("utf-8")
				# print(msg)
				res = json.loads(msg)
				if not res["found"]:
					break
				# print("Self: ", self.port, self.key, res["file"], self.hasher(res["file"]))
				self.files.append(res["file"])
				soc.send(("ok").encode("utf-8"))
				self.recieveFile(soc, os.path.join(self.path, res["file"]))
				soc.send(("ok").encode("utf-8"))
			soc.close()

			# Update Next Successor
			res = json.loads(
				self.send(
					self.successor,
					json.dumps({"type":"getSucc"}).encode("utf-8"),
					recv=True
				)
			)
			self.nextSuccessor = (res["successor"][0], res["successor"][1])
			time.sleep(1)
			# Back Up Files
			soc = socket.socket()
			soc.connect(self.predecessor)
			soc.send(
				json.dumps({"type":"rehashBackup", "hash": (self.host, self.port)}).encode("utf-8")
			)
			while True:
				msg = soc.recv(1024).decode("utf-8")
				# print(msg)
				res = json.loads(msg)
				if not res["found"]:
					break
				# print("Self: ", self.port, self.key, res["file"], self.hasher(res["file"]))
				self.backUpFiles.append(res["file"])
				soc.send(("ok").encode("utf-8"))
				self.recieveFile(soc, os.path.join(self.path, res["file"]))
				soc.send(("ok").encode("utf-8"))
			soc.close()

	def put(self, fileName):
		'''
			Puts File
		'''
		responsibleNode = self.lookUp(self.hasher(fileName))
		soc = socket.socket()
		soc.connect(responsibleNode)
		soc.send(
			json.dumps({"type": "put", "file": fileName, "ins":"add to your files"}).encode("utf-8")
		)
		soc.recv(1024).decode("utf-8")
		self.sendFile(soc, os.path.join(fileName))
		soc.close()
		
	def get(self, fileName):
		'''
			Gets File
		'''
		responsibleNode = self.lookUp(self.hasher(fileName))
		soc = socket.socket()
		soc.connect(responsibleNode)
		soc.send(
			json.dumps({"type":"get", "file": fileName}).encode("utf-8")
		)
		res = json.loads(soc.recv(1024).decode("utf-8"))
		if res["file"]:
			soc.send(("ok").encode("utf-8"))
			self.recieveFile(soc, os.path.join(res["file"]))
		return res["file"]

	def leave(self):
		'''
			Leaves the network
		'''
		self.kill()
		threading.Thread(target=self.send, args=(
			self.successor,
			json.dumps({"type": "update_p", "predecessor": self.predecessor}).encode("utf-8"),
			True
		)).start() 
		self.send(
			self.predecessor,
			json.dumps({"type": "update_s", "successor": self.successor}).encode("utf-8"),
			recv=True
		)
		# Sending Files
		soc = socket.socket()
		soc.connect(self.successor)
		soc.send(
			json.dumps({"type": "leave", "backup":False, "num": len(self.files)}).encode("utf-8")
		)
		soc.recv(1024).decode("utf-8")
		for f in self.files:
			soc.send((f).encode("utf-8"))
			soc.recv(1024).decode("utf-8")
			self.sendFile(soc, os.path.join(self.path, f))
		soc.close()
		# Sending BackUp Files
		soc = socket.socket()
		soc.connect(self.predecessor)
		soc.send(
			json.dumps({"type": "leave", "backup":True, "num": len(self.backUpFiles)}).encode("utf-8")
		)
		soc.recv(1024).decode("utf-8")
		for f in self.backUpFiles:
			soc.send((f).encode("utf-8"))
			soc.recv(1024).decode("utf-8")
			self.sendFile(soc, os.path.join(self.path, f))
		soc.close()

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
		'''
			Kill Node
		'''
		# DO NOT EDIT THIS, used for code testing
		self.stop = True
