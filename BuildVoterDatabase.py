import time
import sys
from Block_IO import Block_IO
from Config import Configurations
from UsefulFunctions import error_print
#from Block_File_Interface import Block_File_Interface

BLOCKSIZE = Configurations.BLOCKSIZE
LRECL = Configurations.LRECL   # Logical RECord Length (as opposed to physical records)
DICTIONARY_RECL = Configurations.DRECL
REGISTERED_VOTERS_TO_PROCESS = Configurations.VOTERS_TO_PROC

#----------------------------------------------------------------------------------
def write_record(record):
    print(" record:", record)

#----------------------------------------------------------------------------------
# Build a dictionary of voters by ID,
def build_dictionary(file_path_name, voter_dictionary):

    # adjust these for different data files
    splitchar = ',' if Configurations.DATA_FORMAT == "csv" else " "
    fld_voterid = 1
    fld_sosid = 2
    # DF: Can be determined by the index of `voterid` & `sosid` in Configurations.DATA_ATTRIBUTES
    # DF: Also: Configurations.DATA_ATTRIBUTES should correspond to the resulting split/decode of data

    # initialize counters
    nrecs = 0
    maxreclen = 0
    # we precalulate the block number and offset where the record will be stored the next pass through
    buffer_num = 0
    offset = 0
    
 
    print(" Opening file: ",file_path_name)
    try:
        infile = open(file_path_name, 'r')
    except IOError:
        error_print(f"could not open {file_path_name}")

    # skip the header line on the original file -- not on these
    line = infile.readline()
    while True:

        ## debug 500000 records
        #if (nrecs > REGISTERED_VOTERS_TO_PROCESS ):
        #    break
        ## end debug
        line = infile.readline()
        if not line:
            #print(" end of file, nrecs ",nrecs,"last record ",string_parts0)
            break
        l = len(line)
        if (l > maxreclen):
            maxreclen = l
        #if (l > LRECL):
        #    print(" record is too long, record is: ",line)

        nrecs += 1
        
        parts = line.split(splitchar)
        
        string_parts0 = parts[fld_voterid].replace('"','')
        string_parts1 = parts[fld_sosid].replace('"','')


        if (len(string_parts1) < 2):    # DF: What is this for ?
            sosid = 0
            print(" no sOS ID for voter id: ",string_parts0)
        else:
            sosid = int(string_parts1)            #Sectretery of state ID
        idnumber = int(string_parts0)

        if idnumber in voter_dictionary:
            print("--------------------------------------------------------------")
            print(" Duplicate key: ",idnumber)
            print("--------------------------------------------------------------")
        else:
            
            voter_dictionary[idnumber] = (sosid, buffer_num, offset)
            #if (buffer_num > 0):
                #print("***************** sosid ", sosid,"buffer ", buffer_num,"offset ", offset)
            offset += LRECL
            if (offset >= BLOCKSIZE):
                buffer_num += 1
                offset = 0
                
    #print("voter_dict:", voter_dictionary)

    infile.close()
    return (nrecs,maxreclen)

#----------------------------------------------------------------------------------
# This function is called to build the database of registered voters
def build_database(file_path_name,blbuilder):

    buffer = bytearray(LRECL)
    
    print(" Building database...Opening file: ",file_path_name)
    try:
        infile = open(file_path_name, 'r')
    except IOError:
        error_print(f"could not open {file_path_name}")

    # skip the header line -- no header file in split data
    line = infile.readline()
    nrecs = 0
    while True:

        line = infile.readline()
        if not line:
            break
        
        nrecs += 1
        # We just save the first 100 chars, the rest is padding (could be used)
        l = len(line)
        if (l > LRECL):
            l = LRECL
      
        savestring = line[0:l]
        if (l < LRECL):
            savestring = line.ljust(LRECL,' ')
        #print(" length of new string = ",len(savstring))
        #barray = bytes(savstring, 'utf-8')
        barray = savestring.encode('utf-8')
        buffer[0:LRECL] = barray
        
        #if nrecs < 2:
        #    print(" debug buffer: ",buffer[0:20])
        #    newstring = buffer.decode('utf-8')
        #    print(" decoded string: ",newstring)

        blbuilder.write_record_to_block(buffer,LRECL)

#----------------------------------------------------------------------------------
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# These next two functions are only called to build a list of keys to be searched for, they are actual voters on election day 2021
# This first function actually does the work of reading the input file and writing appending to an output file.
# This file will be used to query the database and assure the keys are retrieved in the same order each time.
# This funciton is passed two open file handles and the dictionary.  Voters not in the dictionary of registered voters are not saved
def process_this_file(outfile, infile, registered_voters):
    print("processing file")
    nrecs = 0

    # skip the header line
    line = infile.readline()
    while True:

        ## debug 10 records
        #if (nrecs >= 19):
        #    break
        ## end debug
        line = infile.readline()
        if not line:
            break
        l = len(line)
        val = 0
        val = line.find(",") # first comma
        val2 = line.find(",",val+1,l) # second comma
        vidstr = line[val+1:val2]
        idnumber = int(vidstr)
        if idnumber in registered_voters:
        # look for the third comma

            val3 = line.find(",",val2+1,l)
            outstr = line[0:val3]
            outfile.write(outstr)
            outfile.write("\n")
            nrecs += 1
        else:
            print(" id not found: ",vidstr)
            
    infile.close()
    # The Sosid is preserved so as to check that the record retireved is the one searched for
    return nrecs

#..................................................................................
#  This is the driver function called to build a list of keys to search for in the database
#  Several files may be used to build a big list of keys
def process_voters(datalocation, registered_voters):

    outfile = "searchkeys.txt"
    outfile_path_name = datalocation + outfile
    file = "VoterRoster - ELECTION DAY ONLY_1121_1.csv"
    file_path_name = datalocation + file
    
    nrecs = 0

    # open output file
    print(" Opening file: ",outfile_path_name)
    try:
        outfile = open(outfile_path_name, 'w')
    except IOError:
        error_print(f"could not open `{outfile_path_name}`")


    # we may use several input files

    print(" Opening file: ",file_path_name)
    try:
        infile = open(file_path_name, 'r')
    except IOError:
        error_print(f"could not open `{file_path_name}`")
    nrecs = process_this_file(outfile, infile, registered_voters)
    infile.close()

    file = "VoterRoster - ELECTION DAY ONLY_1121_2.csv"
    file_path_name = datalocation + file

    try:
        infile = open(file_path_name, 'r')
    except IOError:
        error_print(f"could not open `{file_path_name}`")
    nrecs += process_this_file(outfile, infile, registered_voters)
    infile.close()

    outfile.close()
    return (nrecs)
#----------------------------------------------------------------------------------
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

#----------------------------------------------------------------------------------
#  *** Main Starts Here ***
#----------------------------------------------------------------------------------

# Only create one instance of the block IO class -- its not a singleton
def main(args):
    Configurations.initialize_default()
    # Congurations.initialize( ... args )

    blbuilder = Block_IO()

    registered_voters = {}

    nrecs = 0
    maxlrecl = 0

    #registered_filename = "HARRIS COUNTY.csv"
    dataLocation = Configurations.DATA_PATH
    #file_path_name = dataLocation + registered_filename

    # DF: Should this range number be in the Configuration file ?
    # range may be up to 5, the first 4 files are 500000 records each
    for i in range(1):
        file_path_name = dataLocation + "registered_voters" + str(i) + ".csv"
        #file_path_name = dataLocation + "test_file" + str(i) + ".csv"
        _nrecs, _maxrecl = build_dictionary(file_path_name, registered_voters)
        nrecs += _nrecs
        if (_maxrecl > maxlrecl):
            maxrecl = _maxrecl

    print("finished, nrecs, lrecs: ",nrecs,maxrecl)
    #print(" sanity check")

    print(" blocks so far... ",blbuilder.return_block_count())

    ##########
    # Uncomment this section to rebuild the key file
    #
    # This section is just to build a list of keys in a file
    #nrecs = process_voters(dataLocation, registered_voters)
    #print(" nrecs = ",nrecs)
    #########

    blbuilder.set_output_file("database.bin")
    t1 = time.process_time()
    blbuilder.write_dictionary(registered_voters)
    t2 = time.process_time()
    print(" print time to write dictionary: ",(t2-t1))
    t1 = time.process_time()

    for i in range(1):              
        #file_path_name = dataLocation + "registered_voters" + str(i) + ".csv"
        file_path_name = dataLocation + "test_file" + str(i) + ".csv"

        build_database(file_path_name,blbuilder)

    t2 = time.process_time()
    print(" print time to build database: ",(t2-t1))

    print(" Total blocks written: ",blbuilder.close_output_file())


if __name__ == "__main__":
    main(sys.argv[1:])