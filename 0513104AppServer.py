import socket
import json
import stomp

MQconn = stomp.Connection10([('54.234.177.235',61613)])
MQconn.start()
MQconn.connect()


DBip = '54.234.177.235'
DBport = 15000



AppServer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ip = "0.0.0.0"
port = 11000
AppServer_sock.bind((ip, port))
AppServer_sock.listen(5)


while True:

	db_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	db_sock.connect((DBip, DBport))

	dbctl = {}

	# list_cmd = []
	# list_cmd.append(input('cmd='))
	# list_cmd.append(input('cmd='))

	"""
	dbctl['cmd'] = list_cmd
	dbctl['attr'] = input("arrtibute:")
	db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
	resp = db_sock.recv(1024).decode('utf-8')

	print(resp)
	"""


	client_sock, client_address = AppServer_sock.accept()

	client_req = client_sock.recv(1024).decode('utf-8')

	req = client_req.split();
	reply = {}
	
	attr_list = []

	if(req[0] == 'invite'):

		if( len(req) >= 1 ):
			if len(req)==1 :
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[0]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				#cursor.execute("SELECT * from T1 where token = ?", (req[0],))
				#r = cursor.fetchall()
			else:
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
				# r = cursor.fetchall()
			if( resp != 1 ):
				reply["status"] = 1
				reply["message"] = "Not login yet"
			elif( resp > 3 ):
				reply["status"] = 1 
				reply["message"] = "Usage: invite <user> <id>"
			else:
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
				# r = cursor.fetchall()
				if( resp==1 ):
					dbctl['cmd'] = 2
					dbctl['attr'] = req[2]
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					resp = int(resp)
					# cursor.execute("SELECT * from T4 where id=? and inviteID=?",(req[2],r[0][0],))
					# s = cursor.fetchall()
					if( resp > 0 ):
						reply["status"] = 1
						reply["message"] = "Already invited"

					dbctl['cmd'] = 3
					dbctl['attr'] = req[2]
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					resp = int(resp)
					# cursor.execute("SELECT * from T4 where id=? and inviteID=?",(r[0][0],req[2],))
					# s = cursor.fetchall()
					if( resp >0 ):
						reply["status"] = 1
						reply["message"] = req[2] + " has invited you"

				dbctl['cmd'] = 1
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)	
				# cursor.execute("SELECT * from T1 where token = ?", (req[1],))  #先去看自己登入了沒有
				# r = cursor.fetchall()
				if( resp == 1 ): #如果登入了
					dbctl['cmd'] = 4
					dbctl['attr'] = req[2]
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					resp = int(resp)
					# cursor.execute("SELECT * from T3 where id=? and friend=? ", (r[0][0],req[2])) #去看自己的朋友表裡面有沒有那個人
					# r = cursor.fetchall()
					if( resp == 1 ): #如果自己的朋友表裡面有那個人
						reply["status"] = 1 ;
						reply["message"] = req[2] + " is already your friend"
					else: #如果自己的朋友表裡面沒有那個人
						dbctl['cmd'] = 5
						attr_list.append(req[1])
						attr_list.append(req[2])
						dbctl['attr'] = attr_list
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						resp = int(resp)
						# cursor.execute("SELECT * from T1 where token= ? and id= ?",(req[1],req[2],)) #去比對輸入的id 跟 token 是不是同一個人
						# r = cursor.fetchall()
						if( resp == 1 ): #如果是同一個人
							reply["status"] = 1
							reply["message"] = "You cannot invite yourself"
				else:
					reply["status"] = 1
					reply["message"] = "Not login yet"
				dbctl['cmd'] =6
				dbctl['attr'] = req[2]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where id=? ", (req[2],)) #去看那個人有沒有註冊過
				# r = cursor.fetchall()
				if( resp != 1 ): #如果沒有註冊過
					reply["status"] = 1
					reply["message"] = req[2] + " does not exist"

				
				# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
				# r = cursor.fetchall()
				if( "status" not in reply ):
					dbctl['cmd'] =7
					dbctl['attr'] = req[2]
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					reply["status"] = 0
					reply["message"] = "Success!"
					# cursor.execute("INSERT INTO T4 (id, inviteID) VALUES(?,?)", (req[2],r[0][0],))
					# db.commit()
		


	elif(req[0] == 'list-invite' ):
		if( len(req) > 2):
			reply["status"] = 1
			reply["message"] = "Usage: list-invite <user>​"
		elif( len(req) == 1 ):
			reply["status"] = 1
			reply["message"] = "Not login yet"
		else:
			dbctl['cmd'] = 1
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token=?",(req[1],))
			# r = cursor.fetchall()
			if( resp == 1):
				dbctl['cmd']=8
				dbctl['req']= req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				# list_invite = []
				# cursor.execute("SELECT inviteID from T4 where id=?" , (r[0][0],))
				# r = cursor.fetchall()
				# for row in r:
				# 	list_invite.append(row[0])
				reply["invite"] = resp
				reply["status"] = 0
			else:
				reply["status"] = 1
				reply["message"] = "Not login yet"

	elif(req[0] == 'accept-invite' ):

		if( len(req) >= 1 ):
			if len(req)==1 :
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[0]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
				# r = cursor.fetchall()
			else:
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
				# r = cursor.fetchall()
			if( resp != 1 ):
				reply["status"] = 1
				reply["message"] = "Not login yet"
			elif( len(req) != 3 ):
				reply["status"] = 1 
				reply["message"] = "Usage: accept-invite <user> <id>"
			else:
				dbctl['cmd'] = 1
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token=?" , (req[1],))
				# r = cursor.fetchall()
				if( resp == 1 ):
					dbctl['cmd'] = 3
					dbctl['attr'] = req[2]
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					resp = int(resp)
					# cursor.execute("SELECT * from T4 where id=? and inviteID=? ",(r[0][0],req[2],))
					# s = cursor.fetchall()
					if( len(s) == 0 ):
						reply["status"] = 1
						reply["message"] = req[2] + " did not invite you"
					else:
						reply["status"] = 0
						reply["message"] = "Success!"

						dbctl['cmd'] = 9
						dbctl['attr'] = req[2]
						dbctl['req'] = req[1]
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						# cursor.execute("DELETE from T4 where id =? and inviteID=? ",(req[2],r[0][0],))

						dbctl['cmd'] = 10
						dbctl['attr'] = req[2]
						dbctl['req'] = req[1]
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						# cursor.execute("DELETE from T4 where id =? and inviteID=? ",(r[0][0],req[2],))

						dbctl['cmd'] = 11
						dbctl['attr'] = req[2]
						dbctl['req'] = req[1]
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						# cursor.execute("INSERT INTO T3 (id, friend) VALUES (?,?)", (r[0][0],req[2],))

						dbctl['cmd'] = 12
						dbctl['attr'] = req[2]
						dbctl['req'] = req[1]
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						# cursor.execute("INSERT INTO T3 (id, friend) VALUES (?,?)", (req[2],r[0][0],))
						# db.commit()
				else:
					reply["status"] = 1
					reply["message"] = "Not login yet"



	elif(req[0] == 'list-friend' ):
		if( len(req) > 2):
			reply["status"] = 1
			reply["message"] = "Usage: list-friend <user>"
		elif( len(req) == 1):
			reply["status"] = 1
			reply["message"] = "Not login yet"
		else:
			dbctl['cmd'] = 1
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
			# r = cursor.fetchall()
			if( len(r) == 1 ):
				dbctl['cmd']=13
				dbctl['req']= req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')

				# cursor.execute("SELECT friend from T3 where id=?" , (r[0][0],))
				# r = cursor.fetchall()
				# for row in r:
				# 	list_friend.append(row[0])
				reply["friend"] = resp
				reply["status"] = 0
			else:
				reply["status"] = 1
				reply["message"] = "Not login yet"

	elif(req[0] == 'post' ):

		print( len(req) )
		if( len(req) >= 1 ):
			if len(req)==1 :
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[0]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
				# r = cursor.fetchall()
			else:
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
				# r = cursor.fetchall()
			if( len(r) != 1 ):
				reply["status"] = 1
				reply["message"] = "Not login yet"
			elif( len(req) <= 2 ):
				reply["status"] = 1 
				reply["message"] = "Usage: post <user> <message>"
			else:
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?" ,(req[1],))
				# r = cursor.fetchall()
				if( len(r) == 1 ):
					del req[0:2]
					post = " ".join(req)
					dbctl['cmd'] = 14
					dbctl['req'] = req[1]
					dbctl['post']=post
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					# cursor.execute("INSERT INTO T5 (id,post) VALUES (?,?)",( r[0][0],post ))
					# db.commit()
					reply["status"] = 0
					reply["message"] = "Success!"


	elif(req[0] == 'receive-post' ):

		print( len(req) )
		if( len(req) >= 1 ):
			if len(req)==1 :
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[0]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
				# r = cursor.fetchall()
			else:
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
				# r = cursor.fetchall()

			if( resp != 1 ):
				reply["status"] = 1
				reply["message"] = "Not login yet"
			elif( resp > 2 ):
				reply["status"] = 1 
				reply["message"] = "Usage: receive-post ​<user>"
			elif( resp == 2):
				dbctl['cmd'] = 1;
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where token =?", (req[1],))
				# r = cursor.fetchall()
				if( resp ==1 ):
					dbctl['cmd'] = 15;
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					# cursor.execute("SELECT * from T5 where id in ( SELECT friend from T3 where id=?)",(r[0][0],))
					# post = []
					# q = cursor.fetchall()
					# for row in q:
					# 	temp = {}
					# 	temp["id"] = row[0]
					# 	temp["message"] = row[1]
					# 	post.append(temp)
					reply["status"] = 0
					reply["post"] = resp

	elif(req[0] == 'send'):
		if len(req)==1 :
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[0]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
			# r = cursor.fetchall()
		else:
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
			# r = cursor.fetchall()
		if( resp != 1):
			reply["status"] = "Fail-A"
			reply["message"] = "Not login yet"
		else:
			if resp <4:
				reply["status"] = "Fail-B"
				reply["message"] = "Usage: send <user> <friend> <message>"
			else:
				dbctl['cmd'] = 6;
				dbctl['attr'] = req[2]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T1 where id = ?", (req[2],))
				# r = cursor.fetchall()
				if( resp != 1):
					reply["status"] = "Fail-C"
					reply["message"] = "No such user exist"
				else:
					dbctl['cmd'] = 16
					attr_list.append(req[1])
					attr_list.append(req[2])
					dbctl['attr'] = attr_list
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					resp = int(resp)
					# cursor.execute("SELECT * from T3 where id in (SELECT id from T1 where token=?) and friend=?",(req[1],req[2],))
					# r = cursor.fetchall()
					if( resp != 1):
						reply["status"] = "Fail-D"
						reply["message"] = req[2] + " is not your friend"
					else:
						dbctl['cmd'] = 5
						attr_list.append("")
						attr_list.append(req[2])
						dbctl['attr'] = attr_list
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						resp = int(resp)
						# cursor.execute("SELECT * from T1 where id=? and token =?" , (req[2],"",))
						# r = cursor.fetchall()
						if resp > 0:
							reply["status"] = "Fail-E"
							reply["message"] = req[2] + " is not online"
						else:
							reply["status"] = 0
							reply["message"] = "Success!"
							channel = req[2]

							dbctl['cmd'] = 17;
							dbctl['attr'] = req[1]
							db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
							resp = db_sock.recv(1024).decode('utf-8')
							# cursor.execute("SELECT id from T1 where token=?" ,(req[1],))
							# r = cursor.fetchall()
							user = resp
							del req[0:3]
							MQsend = "<<<" + user + "->" + channel + ":" + " ".join(req) + ">>>"
							MQconn.send(channel,MQsend)

	elif(req[0] == 'create-group'):
		if len(req)==1 :
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[0]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
			# r = cursor.fetchall()
		else:
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
			# r = cursor.fetchall()
		if( resp != 1):
			reply["status"] = "Fail-A"
			reply["message"] = "Not login yet"
		else:
			if resp !=3:
				reply["status"] = "Fail-B"
				reply["message"] = "Usage: create-group <user> <group>"
			else:
				dbctl['cmd'] = 18
				dbctl['attr'] = req[2]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T6 where groupMQ =?", (req[2],))
				# r = cursor.fetchall()
				if( resp >= 1 ):
					reply["status"] = "Fail-C"
					reply["message"] = req[2] + " already exist"
				else:
					dbctl['cmd'] = 19;
					dbctl['attr'] = req[2]
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					# cursor.execute("SELECT id from T1 where token=?", (req[1],))
					# r = cursor.fetchall()
					# cursor.execute("INSERT INTO T6(groupMQ,id) VALUES (?,?)", (req[2],r[0][0],))
					# db.commit()
					reply["status"] = 0
					reply["message"] = "Success!"
					reply["topic"] = { "group":[req[2]], "id": resp}


	elif(req[0] == 'list-group'):
		if len(req)==1 :
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[0]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
			# r = cursor.fetchall()
		else:
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
			# r = cursor.fetchall()
		if( resp != 1):
			reply["status"] = "Fail-A"
			reply["message"] = "Not login yet"
		else:
			if len(req) !=2:
				reply["status"] = "Fail-B"
				reply["message"] = "Usage: list-group <user>"
			else:
				dbctl['cmd'] = 20
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				# list_group = []
				# cursor.execute("SELECT DISTINCT groupMQ from T6")
				# r = cursor.fetchall()
				# for row in r:
				# 	list_group.append(row[0])
				reply["listGroup"] = resp
				reply["status"] = 0


	elif(req[0] == 'list-joined'):
		if len(req)==1 :
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[0]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
			# r = cursor.fetchall()
		else:
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
			# r = cursor.fetchall()
		if( resp != 1):
			reply["status"] = "Fail-A"
			reply["message"] = "Not login yet"
		else:
			if len(req) !=2:
				reply["status"] = "Fail-B"
				reply["message"] = "Usage: list-joined <user>"
			else:
				dbctl['cmd'] = 21
				dbctl['attr'] = req[1]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				# list_joined = []
				# cursor.execute("SELECT groupMQ from T6 where id in (SELECT id from T1 where token=?) ",(req[1],))
				# r = cursor.fetchall()
				# for row in r:
				# 	list_joined.append(row[0])
				reply["listJoined"] = resp
				reply["status"] = 0


	elif(req[0] == 'join-group'):
		if len(req)==1 :
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[0]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
			# r = cursor.fetchall()
		else:
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
			# r = cursor.fetchall()
		if( resp != 1):
			reply["status"] = "Fail-A"
			reply["message"] = "Not login yet"
		else:
			if len(req) !=3:
				reply["status"] = "Fail-B"
				reply["message"] = "Usage: join-group <user> <group>"
			else:
				dbctl['cmd'] = 18
				dbctl['attr'] = req[2]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T6 where groupMQ=?" , (req[2],))
				# r = cursor.fetchall()
				if( resp==0 ):
					reply["status"] = "Fail-C"
					reply["message"] = req[2] + " does not exist"
				else:
					dbctl['cmd'] = 22
					attr_list.append(req[2])
					attr_list.append(req[1])
					dbctl['attr'] = attr_list
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					resp = int(resp)
					# cursor.execute("SELECT * from T6 where groupMQ=? and id=?", (req[2],req[1],))
					# r = cursor.fetchall()
					if( resp == 1 ):
						reply["status"] = "Fail-D"
						reply["message"] = "Already a member of " + req[2]
					else:
						reply["status"] = 0
						reply["message"] = "Success!"

						dbctl['cmd'] = 19;
						dbctl['attr'] = req[2]
						dbctl['req'] = req[1]
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						# cursor.execute("SELECT id from T1 where token=?", (req[1],))
						# r = cursor.fetchall()
						# cursor.execute("INSERT INTO T6(groupMQ,id) VALUES (?,?)", (req[2],r[0][0],))
						# db.commit()
						reply["topic"] = { "group":[req[2]], "id": resp}


	elif( req[0] == 'send-group'):
		if len(req)==1 :
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[0]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[0],))
			# r = cursor.fetchall()
		else:
			dbctl['cmd'] = 1;
			dbctl['attr'] = req[1]
			db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
			resp = db_sock.recv(1024).decode('utf-8')
			resp = int(resp)
			# cursor.execute("SELECT * from T1 where token = ?", (req[1],))
			# r = cursor.fetchall()
		if( resp != 1):
			reply["status"] = "Fail-A"
			reply["message"] = "Not login yet"
		else:
			if len(req) <4:
				reply["status"] = "Fail-B"
				reply["message"] = "Usage: send-group <user> <group> <message>"
			else:
				dbctl['cmd'] = 18
				dbctl['attr'] = req[2]
				db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
				resp = db_sock.recv(1024).decode('utf-8')
				resp = int(resp)
				# cursor.execute("SELECT * from T6 where groupMQ =?",(req[2],))
				# r = cursor.fetchall()
				if( resp == 0 ):
					reply["status"] = "Fail-C"
					reply["message"] = "No such group exist"
				else:
					dbctl['cmd'] = 23
					dbctl['attr'] = req[2]
					dbctl['req'] = req[1]
					db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
					resp = db_sock.recv(1024).decode('utf-8')
					# cursor.execute("SELECT id from T1 where token=?",(req[1],))
					# r = cursor.fetchall()
					# cursor.execute("SELECT * from T6 where groupMQ=? and id=?",(req[2],r[0][0],))
					# r = cursor.fetchall()
					if( resp == 0):
						reply["status"] = "Fail-D"
						reply["message"] = "You are not the member of " + req[2]
					else:
						reply["status"] = 0
						reply["message"] = "Success!"
						channel = req[2]

						dbctl['cmd']=17
						dbctl['attr'] = req[1]
						db_sock.send(bytes(json.dumps(dbctl),'utf-8'))
						resp = db_sock.recv(1024).decode('utf-8')
						# cursor.execute("SELECT id from T1 where token=?" ,(req[1],))
						# r = cursor.fetchall()
						user = resp
						del req[0:3]
						MQsend = "<<<" + user + "->" + "GROUP<" + channel + ">:" + " ".join(req) + ">>>"
						MQconn.send("/topic/"+channel,MQsend)

	else:
		reply["status"] = 1
		reply["message"] = "Unknown command " + req[0]

	client_sock.send(bytes(json.dumps(reply),'utf-8'))

client_sock.close()

MQconn.disconnect()
