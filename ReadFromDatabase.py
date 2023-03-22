from Block_IO import Block_IO
import sys

BLOCKSIZE=4096
LRECL = 256

buffer = bytearray(4096) 

#----------------------------------------------------------------------------------
# Main starts here
#----------------------------------------------------------------------------------

bio = Block_IO()

dbfile = "database.bin"

registered_voters = {}

bio.build_dictionary_from_file(dbfile, registered_voters)


print(" dictionary of registered voters length = ",len(registered_voters))