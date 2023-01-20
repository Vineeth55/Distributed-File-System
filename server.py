from fuse import FUSE, FuseOSError, Operations, fuse_get_context
import socket
import os
import threading
import errno



# This function is used to handle different clients simultaneously
def threaded_client(c,n):

	while True:

		command = c.recv(2048).decode('utf-8').split(",")
		# print(command)
		message = command[0]
		print("Message from Client " + str(n) + ": " + str(message))
		reply = 0
			

		### Done
		if message == 'mkdir':
			path = command[1] #c.recv(2048).decode('utf-8')
			# print(path)
			mode = int(command[2])
			# isdir = os.path.isdir(path)
			# print(isdir)
			# if isdir:
			# 	reply = "There already exits a directory with the same name in the path you want to create, Please request a different name for the directory"
			os.mkdir(path,mode)
			reply = "Directory created succefully! at " + path 

		
		### Done
		elif message == 'rmdir':
			path = command[1] #c.recv(2048).decode('utf-8')
			# isdir = os.path.isdir(path)
			# if not isdir:
			# 	reply = "There does not exist the requested directory at the path you provided, look over your path again"
			# else:
			os.rmdir(path)
			reply = "Directory removed succefully! at " + path

		### Done
		elif message == 'mkfile':

			path = command[1]
			mode = int(command[2])
			# uid, gid, pid = fuse_get_context()
			uid = int(command[3])
			gid = int(command[4])
			pid = int(command[5])
			fd = os.open(path, os.O_RDWR | os.O_CREAT, mode)
			os.chown(path,uid,gid)
			# isfile = os.path.isfile(path)
		
			# # if isdir:
			# # 	reply = "There already exits a file with the same name in the path you want to create, Please request a different name for the directory"
			# # else:
			reply = str(fd)

		
		### Done
		elif message == 'delfile':
			path = command[1]
			# isfile = os.path.isfile(path)
			# if not isfile:
			# 	reply = "There does not exist the requested file at the path you provided, look over your path again"
			# else:
			uid = int(command[2])
			gid = int(command[3])
			pid = int(command[4])
			st = os.lstat(path)
			if uid == getattr(st, 'st_uid'):
				os.unlink(path)
				reply = "File removed succefully! at " + path
			else:
				reply = "Permission Denied: Only owner can Delete"

		### Done
		elif message == 'readfile':
			path = command[1]
			length = int(command[2])
			offset = int(command[3])
			fd = int(command[4]) 
			os.lseek(fd, offset, os.SEEK_SET)
			reply = os.read(fd,length).decode('utf-8')

		### Done
		elif message == 'writefile':
			path = command[1]
			buf = command[2].encode('utf-8')
			offset = int(command[3])
			fd = int(command[4])
			os.lseek(fd, offset, os.SEEK_SET)
			numBytes = os.write(fd, buf)
			reply = str(numBytes)



		elif message == 'truncatefile':
			path = command[1]
			length = int(command[2])
			isfile = os.path.isfile(path)
			with open(path, 'r+') as f:
				f.truncate(length)
			reply = "Succesfully truncated"


		### Done
		elif message == 'closefile':
			path = command[1]
			fh = int(command[2])
			# print(fh)
			os.close(fh)
			reply = "File Closed at " + path

		
		
		### Done		
		elif message == 'openfile':
			path = command[1]
			flags = int(command[2])
			istrue1 = os.path.isdir(path)
			istrue2 = os.path.isfile(path)
			if istrue1 or istrue2:
				fd = os.open(path, flags)
				reply = "True" + "," + str(fd)
			else:
				reply = "False" + "," + str(errno.EACCES)

		### Done
		elif message == "access":
			path = command[1]
			mode = int(command[2])
			isaccess = os.access(path,mode)
			if isaccess:
				reply = str(isaccess) + "," + str(errno.EACCES) + "," + "Access Successful at " + path
			else:
				reply = str(isaccess) + "," + str(errno.EACCES) + "," + "Access not Successful at " + path


		### Done
		elif message == 'getattr':
			path = command[1]
			istrue1 = os.path.isdir(path)
			istrue2 = os.path.isfile(path)
			if istrue1 or istrue2:
				st = os.lstat(path)
				atime = getattr(st, 'st_atime')
				ctime = getattr(st, 'st_ctime')
				gid = getattr(st, 'st_gid')
				mode = getattr(st, 'st_mode')
				mtime = getattr(st, 'st_mtime')
				nlink = getattr(st, 'st_nlink')
				size = getattr(st, 'st_size')
				uid = getattr(st, 'st_uid')
				reply = "True" + "," + str(atime) + "," + str(ctime) + "," + str(gid) + "," + str(mode) + "," + str(mtime) + "," + str(nlink) + "," + str(size) + "," + str(uid)
			else:
				reply = "False" + "," + str(errno.ENOENT)

		### Done
		elif message == 'readdir':
			dirents = ['.', '..']
			path = command[1]
			if os.path.isdir(path):
				dirents.extend(os.listdir(path))
			reply = str(dirents)
			# print("Readdir")

		elif message == 'chmod':
			path = command[1]
			mode = int(command[2])
			uid = int(command[3])
			gid = int(command[4])
			pid = int(command[5])
			st = os.lstat(path)
			if uid == getattr(st, 'st_uid'):
				os.chmod(path, mode)
				reply = "Permissions modified successfully! at " + path
			else:
				reply = "Permission Denied: Only owner can change the permission of the file"

		### Done
		elif message == 'chown':
			path = command[1]
			uid = int(command[2])
			gid = int(command[3])
			os.chown(path, uid, gid)
			reply = 'Permissions modified successfully at ' + path
		
		### Done
		elif message == 'fsync':
			path = command[1]
			fh = int(command[2])
			os.fsync(fh)
			reply = "Fsync function called"


		else:
			reply = "Invalid operation requested!"

		c.send(reply.encode('utf-8'))
		# print(reply)




s = socket.socket()
clients = []
host = socket.gethostname()
port = 1234
nClients = 0
s.bind((host,port))
print("Server Created, waiting for clients...")
s.listen(5)

while True:
	c, address = s.accept()
	nClients += 1
	print('Connection is establishd with Client ' + str(nClients) + ' with address ' + str(address))
	# c.send(str(nClients).encode('utf-8'))
	clients.append(c)
	thread = threading.Thread(target=threaded_client, args = (c, nClients, ))
	thread.start()
	







