from __future__ import with_statement
import socket
import threading
import os
from fuse import FUSE, FuseOSError, Operations, fuse_get_context
import sys
import errno
 
 
class Passthrough(Operations):
    def __init__(self, root):
        self.root = root
        self.s = socket.socket()
        self.port = 1234
        self.host = socket.gethostname()
        self.s.connect((self.host,self.port))
        print("Connected to server successfully")

    ### Done
    def recieve(self):
        message = self.s.recv(2048).decode('utf-8')
        return message
 
     # Helpers
     # =======
    
    ### Done
    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path
 
     # Filesystem methods
     # ==================
    
    ### Done
    def access(self, path, mode):
        print("Access function called")
        fullpath = self._full_path(path)
        request = "access"
        command = request + "," + fullpath + "," + str(mode)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve().split(",")
        # print(reply)
        if reply[0] == "False":
            raise FuseOSError(int(reply[1]))

    def chmod(self, path, mode):
        fullpath = self._full_path(path)
        request = "chmod"
        uid,gid,pid = fuse_get_context()
        # print("uid = " + str(uid))
        command = request + "," + fullpath + "," + str(mode) + "," + str(uid) + "," + str(gid) + "," + str(pid)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve().split(",")
        print(reply)
    #     full_path = self._full_path(path)
    #     return os.chmod(full_path, mode)
 
    def chown(self, path, uid, gid):
        fullpath = self._full_path(path)
        request = "chown"
        command = request + ',' + fullpath + ',' + str(uid) + ',' + str(gid)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve().split(',')
        print(reply)
    #     full_path = self._full_path(path)
    #     return os.chown(full_path, uid, gid)
    
    ### Done
    def getattr(self, path, fh=None):
        print("Getattr function called")
        fullpath = self._full_path(path)
        request = "getattr"
        command = request + "," + fullpath 
        self.s.send(command.encode('utf-8'))
        reply = self.recieve().split(",")
        # print(reply)

        if reply[0] == "True":
            return {'st_atime':float(reply[1]), 'st_ctime':float(reply[2]), 'st_gid':int(reply[3]), 'st_mode':int(reply[4]), 'st_mtime':float(reply[5]),'st_nlink':int(reply[6]),
            'st_size':int(reply[7]), 'st_uid':int(reply[8])}
        else:
            raise FuseOSError(int(reply[1]))

    ### Done
    def readdir(self, path, fh):
        print("Readdir function called")
        fullpath = self._full_path(path)
        request = "readdir"
        command = request + "," + fullpath + "," + str(fh)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve()
        dirents = eval(reply)
        # print(reply)
        for r in dirents:
            yield r
 
    # def readlink(self, path):
    #     pathname = os.readlink(self._full_path(path))
    #     if pathname.startswith("/"):
    #      # Path name is absolute, sanitize it.
    #         return os.path.relpath(pathname, self.root)
    #     else:
    #         return pathname
 
    # def mknod(self, path, mode, dev=0):
    #     print("mknod")
    #     fullpath = self._full_path(path)
    #     request = "mkfile"
    #     command = request + "," + fullpath + "," + str(mode) + "," + str(dev)
    #     self.s.send(command.encode('utf-8'))
    #     # self.s.send(fullpath.encode('utf-8'))
    #     # self.s.send(mode.encode('utf-8'))
    #     reply = self.recieve()
    #     print(reply)
    #     return None
 
    ### Done
    def mkdir(self, path, mode):
        fullpath = self._full_path(path)
        request = "mkdir"
        command = request + "," + fullpath + "," + str(mode)
        # print(command)
        self.s.send(command.encode('utf-8'))
        # self.s.send(",".encode('utf-8'))
        # self.s.send(fullpath.encode('utf-8'))
        # self.s.send(",".encode('utf-8'))
        # self.s.send(str(mode).encode('utf-8'))
        reply = self.recieve()
        print(reply)
        return None

    ### Done
    def rmdir(self, path):
        fullpath = self._full_path(path)
        request = "rmdir"
        command = request + "," + fullpath
        self.s.send(command.encode('utf-8'))
        # self.s.send(",".encode('utf-8'))
        # self.s.send(fullpath.encode('utf-8'))
        reply = self.recieve()
        print(reply)
        return None
 
    # def statfs(self, path):
    #     full_path = self._full_path(path)
    #     stv = os.statvfs(full_path)
    #     return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
    #      'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
    #      'f_frsize', 'f_namemax'))
 
    ### Done
    def unlink(self, path):
        fullpath = self._full_path(path)
        request = "delfile"
        uid,gid,pid = fuse_get_context()
        # print("uid = " + str(uid))
        # print("gid = " + str(gid))
        command = request + "," + fullpath + "," + str(uid) + "," + str(gid) + "," + str(pid) 
        self.s.send(command.encode('utf-8'))
        # self.s.send(",".encode('utf-8'))
        # self.s.send(fullpath.encode('utf-8'))
        reply = self.recieve()
        print(reply)
        return None
 
    # def symlink(self, name, target):
    #     return os.symlink(name, self._full_path(target))
 
    # def rename(self, old, new):
    #     return os.rename(self._full_path(old), self._full_path(new))
 
    # def link(self, target, name):
    #     return os.link(self._full_path(target), self._full_path(name))
 
    # def utimens(self, path, times=None):
    #     return os.utime(self._full_path(path), times)
 
     # File methods
     # ============
    
    ### Done
    def open(self, path, flags):
        fullpath = self._full_path(path)
        request = "openfile"
        command = request + "," + fullpath + "," + str(flags)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve().split(",")
        print("Open function call")
        # print(reply)
        if reply[0] == "True":
            print("File Opened at " + fullpath)
            fd = int(reply[1])
            return fd
        else:
            raise FuseOSError(int(reply[1]))

        # full_path = self._full_path(path)
        # return os.open(full_path, flags)


    ### Done
    def create(self, path, mode, fi=None):
        print("mkfile function called")
        fullpath = self._full_path(path)
        request = "mkfile"
        uid,gid,pid = fuse_get_context()
        command = request + "," + fullpath + "," + str(mode) + "," + str(uid) + "," + str(gid) + "," + str(pid)
        self.s.send(command.encode('utf-8'))
        # self.s.send(fullpath.encode('utf-8'))
        # self.s.send(mode.encode('utf-8'))
        reply = int(self.recieve())
        return reply

    ### Done
    def read(self, path, length, offset, fh):
        print("Readfile function called")
        fullpath = self._full_path(path)
        request = "readfile"
        command = request + "," + fullpath + "," + str(length) + "," + str(offset) + "," + str(fh)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve()
        # print(reply)      
        return bytes(reply, 'utf-8')
        # os.lseek(fh, offset, os.SEEK_SET)
        # return os.read(fh, length)

    ### Done
    def write(self, path, buf, offset, fh):
        print("Writefile function called")
        fullpath = self._full_path(path)
        request = "writefile"
        command = request + "," + fullpath + "," + str(buf) + "," + str(offset) + "," + str(fh)
        self.s.send(command.encode('utf-8'))
        reply = int(self.recieve())
        print(str(reply) + "Bytes written to the file")
        return reply
        # os.lseek(fh, offset, os.SEEK_SET)
        # return os.write(fh, buf)

    ### Done
    def truncate(self, path, length, fh=None):
        fullpath = self._full_path(path)
        request = "truncatefile"
        command = request + "," + fullpath + "," + str(length)
        self.s.send(command.encode('utf-8'))
        # self.s.send(fullpath.encode('utf-8'))
        # self.s.send(str(length).encode('utf-8'))
        reply = self.recieve()
        print(reply)
        return None
        # full_path = self._full_path(path)
        # with open(full_path, 'r+') as f:
        #     f.truncate(length)


    def flush(self, path, fh):
        request = "fsync"
        fullpath = self._full_path(path)
        command = request + "," + fullpath + "," + str(fh)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve()
        print("Flush function called")
        return None
    ### Done
    def release(self, path, fh):  
        fullpath = self._full_path(path)
        request = "closefile"
        command = request + "," + fullpath + "," + str(fh)
        print(command)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve()
        print(reply)
        return None
        # return os.close(fh)

    ### Done
    def fsync(self, path, fdatasync, fh):
        request = "fsync"
        fullpath = self._full_path(path)
        command = request + "," + fullpath + "," + str(fh)
        self.s.send(command.encode('utf-8'))
        reply = self.recieve()
        print(reply)
        return None


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)
    

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
