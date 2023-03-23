
from Block_File_Interface import Block_File_Interface 
import sys

BLOCKSIZE=4096
DICTIONARY_RECL = 16

class Block_IO():
    buffer = bytearray(BLOCKSIZE)
    bs = Block_File_Interface()

    def __init__(self):

        self.have_opened_storage = False
        #self.blocks_written = 0
        self.bufferpos = BLOCKSIZE # force the initial reading of a block
    #----------------------------------------------------------------------------------

    def return_block_count(self):
        if self.bs.isopen_read:
            return self.bs.blocks_read
        elif self.bs.isopen_write:
            return self.bs.blocks_written
        else:
            return 0

    def set_output_file(self,filename):
        self.bs.open_for_write(filename)
        self.bufferpos = 0

    def close_output_file(self):
        nb = self.bs.close_file()
        return(nb)
    #----------------------------------------------------------------------------------
    def clear_last_block(self):
        # write out last block and reset bufferpos
        self.bs.write_block(self.buffer)
        self.bufferpos = 0

    #----------------------------------------------------------------------------------
    def write_record_to_block(self, record, length):
        if (self.bufferpos+length > BLOCKSIZE):
            self.bs.write_block(self.buffer)
            print(" block written ",self.buffer[0:64])
            self.bufferpos = 0

        self.buffer[self.bufferpos:self.bufferpos+length] = record
        self.bufferpos = self.bufferpos + length

    def read_record_from_block(self,length):
        if (self.bufferpos + length) > BLOCKSIZE:
            self.buffer = self.bs.read_block()
            #print(" block read ",self.buffer[0:16])
            #print(" block read ",self.buffer[16:32])
            self.bufferpos = 0;
        record = bytearray(length)
        record = self.buffer[self.bufferpos:self.bufferpos+length]
        #print(" record as read",record)
        self.bufferpos = self.bufferpos + length
        return record

    def read_spcific_record(self,blocknum,offset,length):
        self.buffer = self.bs.read_required_block(blocknum)
        print(" block read ",self.buffer[0:32])
        self.bufferpos = offset
        record = self.buffer[self.bufferpos:self.bufferpos+length]
        return record
   #----------------------------------------------------------------------------------


    def write_dictionary(self,entry_dictionary):

        entry_length = DICTIONARY_RECL # 16 bytes per dictionary entry into the buffer
        #buffer = bytearray(4096)  # Use BLOCKSIZE here
       
        byte_order = sys.byteorder
        print(" byte order: ",sys.byteorder)
        n = len(entry_dictionary)
        print(" number of dictionary entries = ",n)

        # Sanity check: see if actual number of blocks is the expected number
        records_per_buffer = BLOCKSIZE / entry_length
        calculated_n = n // records_per_buffer
        if (n % records_per_buffer) > 0:
            calculated_n = int(calculated_n + 1)
        print(" based on this the calculated number of blocks is: ",calculated_n)

        # The first three words of the buffer (fourth is not used to make entryies the same length)
        # are: number of dictionary entries, size of each entry, number of blocks used for dictionary
        bufferpos = 0
        bytes_val = n.to_bytes(4,sys.byteorder)
        #print(" number of entries as bytes: ",bytes_val)
        self.buffer[bufferpos:bufferpos+4] = bytes_val;

        bytes_val = entry_length.to_bytes(4,sys.byteorder)
        self.buffer[bufferpos+4:bufferpos+8] = bytes_val;
        
        bytes_val = calculated_n.to_bytes(4,sys.byteorder)
        self.buffer[bufferpos+8:bufferpos+12] = bytes_val;

        bufferpos = bufferpos + entry_length

       
        #print(" retrieved number of entries as bytes: ",bytes_val)
        
        
        # now for each dictionary entry we write 4 words to the buffer. They are
        # key, and a tuple (sosid -- sanity check, block number, offset -- where the record will be found)
        debug_count = 0;
        for vid, result in entry_dictionary.items():
            #sosid = result[0]
            #block = result[1]
            #offset = result[2]
            #print("vid: ",vid," sosid: ",sosid," block ",block," offset ",offset)
             
            bytes_val = vid.to_bytes(4,sys.byteorder);
            self.buffer[bufferpos:bufferpos+4] = bytes_val;
           

            
            bytes_val = result[0].to_bytes(4,sys.byteorder);
            self.buffer[bufferpos+4:bufferpos+8] = bytes_val;

            bytes_val = result[1].to_bytes(4,sys.byteorder)
            self.buffer[bufferpos+8:bufferpos+12] = bytes_val;

            bytes_val = result[2].to_bytes(4,sys.byteorder);
            self.buffer[bufferpos+12:bufferpos+16] = bytes_val;
           
            #if (debug_count < 10):
                 #print(" buffer record written: ",self.buffer[bufferpos:bufferpos+16])
                 #print("key: ",vid," result ",result[0]," [1] ", result[1], "[2] ",result[2])
            #    bytes_val = buffer[bufferpos:bufferpos+4]
            #    key = int.from_bytes(bytes_val,sys.byteorder)
            #    print("vid = ",vid, "key = ",key)
 
            
            debug_count = debug_count + 1
            #if (debug_count == 1):
            #    record = buffer[bufferpos:bufferpos+entry_length]
            #    print(" as written ",record)
            #    bytes_val = buffer[bufferpos:bufferpos+4]
            #    key = int.from_bytes(bytes_val,sys.byteorder)
            #    print("key = ",key)
            bufferpos = bufferpos + entry_length
                                         
           #buffer[bufferpos:bufferpos+lenint] = offset.to_bytes(lint,sys.byteorder)
            
            if (bufferpos >= BLOCKSIZE):
                self.bs.write_block(self.buffer)
                bufferpos = 0
        
        # Buffer may be partly full... need to write it
        if bufferpos > 0:
            self.clear_last_block()
        print(" blocks written: ",self.bs.blocks_written)
        #print("**** debug extract entries")
        #bytes_val = buffer[bufferpos:bufferpos+4]
        #n.from_bytes(bytes_val,sys.byteorder)
        #print(" n(retrieved) = ",n)

#----------------------------------------------------------------------------------

    def build_dictionary_from_file(self, infile_name, registered_voters):

        record_buffer = bytearray(DICTIONARY_RECL) # record length

        self.bs.open_for_read(infile_name)
        record_buffer = self.read_record_from_block(DICTIONARY_RECL)
       
        bufferpos = 0
        n = 0
        #  number of dictionary entries, size of each entry, number of blocks used for dictionary
        bytes_val = record_buffer[bufferpos:bufferpos+4]
        ll = len(bytes_val)
        #print("bytes: ",bytes_val)
        n = int.from_bytes(bytes_val,sys.byteorder)
        
        bytes_val = record_buffer[bufferpos+4:bufferpos+8]
        entry_length = int.from_bytes(bytes_val,sys.byteorder)
        bytes_val = record_buffer[bufferpos+8:bufferpos+12]
        nblocks_to_expect = int.from_bytes(bytes_val,sys.byteorder)
        print(" n(retrieved) = ",n," entry_length ",entry_length," nblocks ",nblocks_to_expect)
        # set this for later
        self.bs.directory_block_offset = nblocks_to_expect
        
        value = [0,0,0]
        for i in range(n):
            record_buffer = self.read_record_from_block(DICTIONARY_RECL)
            #print(record_buffer)

            bytes_val = record_buffer[bufferpos:bufferpos+4]
            key = int.from_bytes(bytes_val,sys.byteorder)
            
            bytes_val = record_buffer[bufferpos+4:bufferpos+8]
            value[0] = int.from_bytes(bytes_val,sys.byteorder)

            bytes_val = record_buffer[bufferpos+8:bufferpos+12]
            value[1] = int.from_bytes(bytes_val,sys.byteorder)

            bytes_val = record_buffer[bufferpos+12:bufferpos+16]
            value[2] = int.from_bytes(bytes_val,sys.byteorder)

            #if (i < 9):
            #    print("key",key," value stored: ",value[0], value[1], value[2])
            #    print(" buffer record written: ",self.buffer[bufferpos:bufferpos+16])
                
            # for debugging:
            if (key in registered_voters):
                print("*** ERROR duplicate key: ",key,"for i = ",i," nblocks = ",self.return_block_count())
                print(" block entry ",value[1],' offset: ',value[2])
            registered_voters[key] = (value[0],value[1],value[2])
        
  