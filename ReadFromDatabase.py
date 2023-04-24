from logging import exception   # DF: What's this?
import time
import sys
from Block_IO import Block_IO
from Config import Configurations
from UsefulFunctions import error_print

BLOCKSIZE = Configurations.BLOCKSIZE
LRECL = Configurations.LRECL

buffer = bytearray(BLOCKSIZE)

def Find_Records_From_Keys(bio, registered_voters, key_file_path_name, key_list = None):

    splitchar = ',' if Configurations.DATA_FORMAT == "csv" else " "
    fld_voterid = 1
    fld_sosid = 2
    # DF: Can be determined by the index of `voterid` & `sosid` in Configurations.DATA_ATTRIBUTES:
    # DF: Also: Configurations.DATA_ATTRIBUTES should correspond to the resulting split/decode of data
   
    ## Try reading the first data record
    # record = bio.read_specific_record(0, 0, LRECL)
    # result = registered_voters[idnumber]
    # print(" found dictionary entry for id ", idnumber, " sosid", result[0], " block ", result[1], " offset", result[2])
    # record = bio.read_specific_record(result[1], result[2], LRECL)
    # testdata = record[0:100]
    # sdata = testdata.decode("'utf-8'")
    # print(" debug first entry: ", sdata)
    ## Now try the 20 th record
   
    # testdata = record[0:100]
    # print(" debug testdata: ", testdata[0:20])
    # sdata = testdata.decode("'utf-8'")
    # print(sdata)

    if key_list is None:
        print(" Opening file: ", key_file_path_name)
        try:
            infile = open(key_file_path_name, 'r')
        except IOError:
            error_print(f"could not open {key_file_path_name}")
    """
    idnumber = 87015467
    result = registered_voters[idnumber]
    record = bio.read_specific_record(result[1],result[2],LRECL)
    print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
    print(" record: ", record)

    idnumber = 66651464
    result = registered_voters[idnumber]
    record = bio.read_specific_record(result[1],result[2],LRECL)
    print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
    print(" record: ", record)

    idnumber = 66938754
    result = registered_voters[idnumber]
    record = bio.read_specific_record(result[1],result[2],LRECL)
    print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
    print(" record second: ", record)

    idnumber = 84610229
    result = registered_voters[idnumber]
    record = bio.read_specific_record(result[1],result[2],LRECL)
    print(" found dictionary entry for id ",idnumber, " sosid",result[0]," block ",result[1]," offset",result[2])
    print(" record : ", record)
    """
   
    nrecs = 0

    while True:
        if key_list is None:
            line = infile.readline()
            if not line:
                # print(" end of file, nrecs ", nrecs, "last record ", string_parts0)
                break
            # if (l > LRECL):
            #    print(" record is too long, record is: ", line)           
        
            parts = line.split(splitchar)
        
            string_parts0 = parts[fld_voterid].replace('"','')
            # string_parts1 = parts[fld_sosid].replace('"','')
            idnumber = int(string_parts0)
            #if idnumber in registered_voters.keys():
             #   result = registered_voters[idnumber]
        else:
            if nrecs >= len(key_list):
                break
            idnumber = key_list[nrecs]

        # now process this id

        nrecs += 1

        if idnumber in registered_voters.keys():
            result = registered_voters[idnumber]
        else:
            print(" voter ID ",idnumber," not in dictionary...skipping")
            continue
        
        
        record = bio.read_specific_record(result[1], result[2], LRECL)
        if (nrecs % 1000 == 0):
            ## Check for correct record
            # print(" found dictionary entry for id ", idnumber, " sosid", result[0], " block ", result[1], " offset", result[2])
            # print(" record from key: ", record)

            testdata = record[0:100]
            sdata = testdata.decode("'utf-8'")
            parts = sdata.split(splitchar)
            string_parts0 = parts[fld_voterid].replace('"','')
            idtocheck = int(string_parts0)
            if idnumber != idtocheck:
                print(" error retrieving record with id ",idnumber)


        ## DF: Should this 100 be part of the Configurations ?
        # testdata = record[0:100]
        # sdata = testdata.decode("'utf-8'")
        # print(sdata)

        ## DF: Should this 3 be in Configurations somehow, maybe as the len() of a list in Configurations ?
        # if (nrecs > 4):
        #    break
#----------------------------------------------------------------------------------

def build_lists_from_keys(key_file_path_name):

    splitchar = ',' if Configurations.DATA_FORMAT == "csv" else " "
    fld_voterid = 1

    key_list = []
    print(" Opening file: ", key_file_path_name)
    try:
        infile = open(key_file_path_name, 'r')
    except IOError:
        error_print(f"could not open {key_file_path_name}")
    
    while True:
        line = infile.readline()
        if not line:
            # print(" end of file, nrecs ", nrecs, "last record ", string_parts0)
            break
        # if (l > LRECL):
        #    print(" record is too long, record is: ", line)           
        
        parts = line.split(splitchar)
        
        string_parts0 = parts[fld_voterid].replace('"','')
        # string_parts1 = parts[fld_sosid].replace('"','')
        idnumber = int(string_parts0)
        key_list.append(idnumber)
    infile.close()
    return key_list
#----------------------------------------------------------------------------------
# Main starts here
#----------------------------------------------------------------------------------
def main(args):
    Configurations.initialize_default()
    if len(args) > 0:
        if args[0] == "-or":
            Configurations.initialize(opt_reads = True)
            print(" ** Read optimization is turned on (don't re-read blocks)")
    
    bio = Block_IO()            # can only be one instance of bio

    dbfile = Configurations.DATABASE_PATH + Configurations.DATABASE_NAME
    keyfile = "searchkeys.txt"
    key_file_path_name = Configurations.DATA_PATH + keyfile

    ## DEBUG only
    # bio.debug_read_records()

    registered_voters = {}

    t1 = time.process_time()
    bio.build_dictionary_from_file(dbfile, registered_voters)
    t2 = time.process_time()
    print(" ====> time to build dictionary from file: ", (t2-t1))


    print(" dictionary of registered voters length = ", len(registered_voters))
    print(" blocks read for dictionary: ", bio.return_block_count())

    # count = 0
    # for vid, result in registered_voters.items():
    #    print(" vid: ", vid, " result", result)
    #    count = count + 1
    #    if (count > 10):
    #        break

    t1 = time.process_time()
    Find_Records_From_Keys(bio, registered_voters, key_file_path_name)
    t2 = time.process_time()
    print(" ====> time to process records: ", (t2-t1))
    print("")
    result = bio.get_block_file_stats()
    print(" blocks read: ", result[0], " number of seeks ", result[1], " total blocks in seek: ", result[2])
    print(" average seek length ", (result[2]/result[1]), " blocks ", (result[2]/result[1]) * BLOCKSIZE, "bytes")
    print("")
    # nbread = bs.blocks_read
    # nseeks = bs.number_of_seeks

    key_list = build_lists_from_keys(key_file_path_name)
    print(" number of keys: ",len(key_list))
    print("\n now process records based on list...")
    t1 = time.process_time()
    Find_Records_From_Keys(bio, registered_voters, key_file_path_name,key_list)
    t2 = time.process_time()
    print(" ====> time to process records: ", (t2-t1))
    print()
    bio.set_nobuffer_read()
    t1 = time.process_time()
    Find_Records_From_Keys(bio, registered_voters, key_file_path_name,key_list)
    t2 = time.process_time()
    print(" ====> time to process records: ", (t2-t1))

if __name__ == "__main__":
    main(sys.argv[1:])