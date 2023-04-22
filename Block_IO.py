
from Block_File_Interface import Block_File_Interface 
import sys
import math
from Config import Configurations

BLOCKSIZE = Configurations.BLOCKSIZE
DICTIONARY_RECL = Configurations.DRECL
optimize_reads = Configurations.optimize_reads
# for some reason is True and optimize_reads is false after this statement

class Block_IO():
    #
    # This class maintains the buffer and decides if it is current or another one needs to be written
    #
    buffer = bytearray(BLOCKSIZE)
    bs = Block_File_Interface()
    

    def __init__(self):

        self.have_opened_storage = False
        #self.blocks_written = 0
        self.bufferpos = BLOCKSIZE # force the initial reading of a block
        self.current_block = -1    # this is the block num (not counting directory blocks) held in the buffer
        self.optimize_reads = Configurations.optimize_reads

#----------------------------------------------------------------------------------
    ## 3 ref: `build_dictionary_from_file()`, BuildVoterDatabase, ReadFromDatabase
    def return_block_count(self):
        if self.bs.isopen_read:
            return self.bs.blocks_read
        elif self.bs.isopen_write:
            return self.bs.blocks_written
        return 0

#----------------------------------------------------------------------------------
    ## 1 ref: BuildVoterDatabase
    def set_output_file(self,filename):
        self.bs.open_for_write(filename)
        self.bufferpos = 0

    ## 1 ref: BuildVoterDatabase
    def close_output_file(self):
        nb = self.bs.close_file()
        return(nb)
    
    ## 1 ref: BuildVoterDatabase
    def write_record_to_block(self, record:bytearray, length:int):
        if (self.bufferpos+length > BLOCKSIZE):
            self.bs.write_block(self.buffer)
            # print(" block written ", self.buffer[0:64])
            self.bufferpos = 0

        self.buffer[self.bufferpos : self.bufferpos+length] = record
        self.bufferpos += length
    
    #----------------------------------------------------------------------------------
    ## 1 ref: `write_dictionary()`
    def clear_last_block(self):
        # write out last block and reset bufferpos
        # print(" clear last block: len is ", len(self.buffer))
        self.bs.write_block(self.buffer)
        self.bufferpos = 0
    #----------------------------------------------------------------------------------
    ## 1 ref: BuildVoterDatabase
    def write_dictionary(self, entry_dictionary):

        entry_length = DICTIONARY_RECL # 16 bytes per dictionary entry into the buffer
       
        print(" byte order: ", sys.byteorder)
        n = len(entry_dictionary)
        print(" number of dictionary entries = ", n)

        # Sanity check: see if actual number of blocks is the expected number
        records_per_buffer = BLOCKSIZE / entry_length
        calculated_n = n / records_per_buffer          # DF: Where is the sanity check? -- see if blocks written is same as calculated
        calculated_n = math.ceil(calculated_n)
        print(" based on this the calculated number of blocks is: ", calculated_n)

        # DF: Should these 3 words be part of Configurations somehow ?
        # DF:   - So that this (4, +4, +8, etc.) are not hardcoded below
        # The first three words of the buffer (fourth is not used to make entryies the same length)
        # are: number of dictionary entries, size of each entry, number of blocks used for dictionary
        bufferpos = 0
        bytes_val = n.to_bytes(4, sys.byteorder)   # number of dictionary entries
        # print(" number of entries as bytes: ", bytes_val)
        self.buffer[bufferpos : bufferpos+4] = bytes_val

        bytes_val = entry_length.to_bytes(4, sys.byteorder) # dict entry length
        self.buffer[bufferpos+4 : bufferpos+8] = bytes_val
        
        bytes_val = calculated_n.to_bytes(4, sys.byteorder) # number of blocks
        self.buffer[bufferpos+8 : bufferpos+12] = bytes_val

        bytes_val = BLOCKSIZE.to_bytes(4, sys.byteorder) # when reading check to see if blocksize is same as when written
        self.buffer[bufferpos+12 : bufferpos+16] = bytes_val

        bufferpos += entry_length

       
        # print(" retrieved number of entries as bytes: ", bytes_val)

        
        # now for each dictionary entry we write 4 words to the buffer. They are
        # key, and a tuple (sosid -- sanity check, block number, offset -- where the record will be found)
        debug_count = 0
        for vid, result in entry_dictionary.items():
            # sosid = result[0]
            # block = result[1]
            # offset = result[2]
            # print("vid: ", vid, " sosid: ", sosid, " block ", block, " offset ", offset)
             
            bytes_val = vid.to_bytes(4, sys.byteorder)
            self.buffer[bufferpos : bufferpos+4] = bytes_val
           
            bytes_val = result[0].to_bytes(4, sys.byteorder)
            self.buffer[bufferpos+4 : bufferpos+8] = bytes_val

            bytes_val = result[1].to_bytes(4, sys.byteorder)
            self.buffer[bufferpos+8 : bufferpos+12] = bytes_val

            bytes_val = result[2].to_bytes(4, sys.byteorder)
            self.buffer[bufferpos+12 : bufferpos+16] = bytes_val

            # if (debug_count < 10):
            #      print(" buffer record written: ", self.buffer[bufferpos : bufferpos+16])
            #      print("key: ", vid, " result ", result[0], " [1] ", result[1], "[2] ", result[2])
            #    bytes_val = buffer[bufferpos : bufferpos+4]
            #    key = int.from_bytes(bytes_val, sys.byteorder)
            #    print("vid = ", vid, "key = ", key)
 
            
            # debug_count += 1
            # if (debug_count == 1):
            #    record = buffer[bufferpos : bufferpos+entry_length]
            #    print(" as written ", record)
            #    bytes_val = buffer[bufferpos : bufferpos+4]
            #    key = int.from_bytes(bytes_val, sys.byteorder)
            #    print("key = ", key)
            bufferpos += entry_length
                                         
            # buffer[bufferpos:bufferpos+lenint] = offset.to_bytes(lint,sys.byteorder)
            
            if (bufferpos >= BLOCKSIZE):
                self.bs.write_block(self.buffer)
                bufferpos = 0
        
        # Buffer may be partly full... need to write it
        if bufferpos > 0:
            self.clear_last_block()
        # print(" blocks written: ", self.bs.blocks_written)
        # print("**** debug extract entries")
        # bytes_val = buffer[bufferpos : bufferpos+4]
        # n.from_bytes(bytes_val, sys.byteorder)
        # print(" n(retrieved) = ", n)

#----------------------------------------------------------------------------------
    ## 1 ref: ReadFromDatabase
    def get_block_file_stats(self):
        nbread = self.bs.blocks_read
        nseeks = self.bs.number_of_seeks
        nseekblocks = self.bs.total_seek_length_blocks
        return (nbread, nseeks, nseekblocks)
    
    ## 1 ref: ReadFromDatabase
    def read_specific_record(self, blocknum, offset, length):
        if not self.optimize_reads or self.current_block != blocknum:
            self.buffer = self.bs.read_required_block(blocknum)
        # debugging-----
        # if Configurations.optimize_reads and blocknum == self.current_block:
        #     print(" no need to seek")
        #----- end debug

        #self.buffer = self.bs.read_required_block(blocknum)
        self.current_block = blocknum
        #print(" block read ",self.buffer[0:32])
        self.bufferpos = offset
        record = self.buffer[self.bufferpos : self.bufferpos+length]
        return record

    # ## 1 ref: ReadFromDatabase
    # def debug_read_records(self):
    #     self.bs.open_for_read("database.bin")
    #     self.buffer = self.bs.read_block()
    #     print(" block: 1 ", self.buffer[0:36])
    #     self.buffer = self.bs.read_block()
    #     print(" block: 2 ", self.buffer[0:36])
    #     self.buffer = self.bs.read_block()
    #     print(" block: 3 ", self.buffer[0:36])
    #     self.bs.close_file()

    #----------------------------------------------------------------------------------
    ## 1 ref: `build_dictionary_from_file()`
    # Used to Read Next Record
    def read_record_from_block(self, length:int):
        if self.bufferpos + length > BLOCKSIZE:
            self.buffer = self.bs.read_block()  # note this does not set current block
            # print(" block read ",self.buffer[0:16])
            # print(" block read ",self.buffer[16:32])
            self.bufferpos = 0
        record = self.buffer[self.bufferpos : self.bufferpos+length]
        # print(" record as read",record)
        self.bufferpos += length
        return record
    #----------------------------------------------------------------------------------
    ## 1 ref: ReadFromDatabase
    def build_dictionary_from_file(self, infile_name, registered_voters):
        record_buffer = bytearray(DICTIONARY_RECL) # record length

        self.bs.open_for_read(infile_name)
        record_buffer = self.read_record_from_block(DICTIONARY_RECL)

        bufferpos = 0
        n = 0
        # number of dictionary entries, size of each entry, number of blocks used for dictionary
        bytes_val = record_buffer[bufferpos : bufferpos+4]
        n = int.from_bytes(bytes_val, sys.byteorder) 
        
        bytes_val = record_buffer[bufferpos+4 : bufferpos+8]
        entry_length = int.from_bytes(bytes_val, sys.byteorder)

        bytes_val = record_buffer[bufferpos+8 : bufferpos+12]
        nblocks_to_expect = int.from_bytes(bytes_val, sys.byteorder)

        bytes_val = record_buffer[bufferpos+12 : bufferpos+16]
        block_size_as_built = int.from_bytes(bytes_val, sys.byteorder)

        print(" number of dictionary entries = ", n, " entry_length ", entry_length,
              " nblocks ", nblocks_to_expect, " Blocksize of DB: ", block_size_as_built)
        # set this for later
        self.bs.directory_block_offset = nblocks_to_expect
        
        value = [0,0,0]
        for i in range(n):
            record_buffer = self.read_record_from_block(DICTIONARY_RECL)
            # print(record_buffer)

            bytes_val = record_buffer[bufferpos : bufferpos+4]
            key = int.from_bytes(bytes_val, sys.byteorder)
            
            bytes_val = record_buffer[bufferpos+4 : bufferpos+8]
            value[0] = int.from_bytes(bytes_val, sys.byteorder)

            bytes_val = record_buffer[bufferpos+8 : bufferpos+12]
            value[1] = int.from_bytes(bytes_val, sys.byteorder)

            bytes_val = record_buffer[bufferpos+12: bufferpos+16]
            value[2] = int.from_bytes(bytes_val, sys.byteorder)
            # if key == 80799695:
            #     print(" debug key in dict: ", key, " val1 ", value[1])
            # if (i < 9):
            #     print("key", key, " value stored: ", (value[0], value[1], value[2]))
            #     print(" buffer record written: ", self.buffer[bufferpos : bufferpos+16])
            
            # just checking for dupes 
            if (key in registered_voters):
                print("*** ERROR duplicate key: ", key, "for i = ", i, " nblocks = ", self.return_block_count())
                print(" block entry ", value[1], ' offset: ', value[2])

            registered_voters[key] = (value[0], value[1], value[2])
        
  