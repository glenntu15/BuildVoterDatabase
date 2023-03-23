


class Block_File_Interface(object):
    BLOCKSIZE = 4096
    def __init__(self):
        self.blocks_written = 0
        self.blocks_read = 0
        self.blocks_replaced = 0
        self.outfile = None
        self.infile = None
        self.isopen_read = False
        self.isopen_write = False
        self.buffer_pos = -1
        self.directory_block_offset = 0 # number of directory blocks so any 
                                        # block request is added to this number to
                                        # get the actual block number
#----------------------------------------------------------------------------------

    def open_for_write(self,filename):
        print(" Opening file: ",filename,)
        try:
            self.outfile = open(filename, 'wb')
        except IOError:
            print(" could not open file")
            exit(1)
        self.isopen_write = True
        self.blocks_written = 0
#----------------------------------------------------------------------------------

    def open_for_read(self,filename):
        print(" Opening file: ",filename,)
        try:
            self.infile = open(filename, 'rb')
        except IOError:
            print(" could not open file")
            exit(1)
        self.isopen_read = True
        print(" seekable: ",self.infile.seekable())
#----------------------------------------------------------------------------------

    def close_file(self):
        if (self.isopen_read):
            self.infile.close()
            return(0)
        if (self.isopen_write):
            self.outfile.close()
            nb = self.blocks_written
        return(nb)
#----------------------------------------------------------------------------------

    def write_block(self,buffer):
        self.outfile.write(buffer)
        self.blocks_written = self.blocks_written + 1
        # progress report
        #if (self.blocks_written % 500) == 0:
        #    print(" so far...",self.blocks_written)

    def read_block(self):
        try:
            buffer = self.infile.read(self.BLOCKSIZE)
        except IOError:
            print(" could not read from file")
        self.blocks_read = self.blocks_read + 1
        return buffer

    def read_required_block(self,blocknum):
        file_offset = (self.directory_block_offset + blocknum) * self.BLOCKSIZE # offset in bytes
        self.infile.seek(file_offset,0)
        print(" reading block: ",(self.directory_block_offset + blocknum)," offset in file ",file_offset," tell: ",self.infile.tell())
        buffer = self.infile.read()
        print(" after seek and read buffer: ",buffer[0:64])
        return buffer

#----------------------------------------------------------------------------------
    




