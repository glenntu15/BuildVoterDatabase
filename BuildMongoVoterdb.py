import pymongo
import time
import sys
from Config import Configurations

LRECL = Configurations.LRECL 
def debug_check_record(mycol):
    x = mycol.find_one()
    print("returned: ",x)
    str = x["record"]
    print(str)

    vid = "84121144"
    myquery = { "_id": vid }
    mydoc = mycol.find_one(myquery)
    str = mydoc["record"]
    print(str)

def build_database(file_path_name,mycol, nrecs):
    splitchar = ',' if Configurations.DATA_FORMAT == "csv" else " "
    fld_voterid = 1
    fld_sosid = 2
    
    print(" Building database...Opening file: ", file_path_name)
    # print(" blocks so far... ", blbuilder.return_block_count())
    try:
        infile = open(file_path_name, 'r')
    except IOError:
        print("could not open ",file_path_name)

    # skip the header line -- no header file in split data
    line = infile.readline()
    
    while True:
        ## debug 5 records
        #if (nrecs > 5):
            #break
        ## end debug
        line = infile.readline()
        if not line:
            break
        
        nrecs += 1
        #if nrecs % 5000 == 0:
          #  print(" nrecs: ",nrecs)

        parts = line.split(splitchar)

        vid = parts[fld_voterid].replace('"', '')
        # We just save the first 100 chars, the rest is padding (could be used)
        l = len(line)
        if (l > LRECL):
            l = LRECL
        if (vid == "1072388"):
            print("saving ")
            break
        savestring = line[0:l]
        if (l < LRECL):
            savestring = line.ljust(LRECL,' ')

        mydict = { "_id": vid, "record": savestring}

        x = mycol.insert_one(mydict)
        # print(" length of new string = ", len(savstring))
        # barray = bytes(savstring, 'utf-8')
        #barray = savestring.encode('utf-8')
        #buffer[0:LRECL] = barray

    infile.close()
    return nrecs

#----------------------------------------------------------------------------
def main(args):

    myclient = pymongo.MongoClient()
    mydb = myclient["voterdatabase"]
    mycol = mydb["Voters"]
    if mycol.drop():
        print("deleted collection")
    else:
        print("collection not present")

    nrecs = 0
    t1 = time.process_time()
    for i in range(5):              
        file_path_name = Configurations.DATA_PATH + "registered_voters" + str(i) + ".csv"
        #file_path_name = dataLocation + "test_file" + str(i) + ".csv"

        nrecs = build_database(file_path_name,mycol, nrecs)
    t2 = time.process_time()
    print(" =======> database process time ",(t2-t1))
    debug_check_record(mycol)

if __name__ == "__main__":
    main(sys.argv[1:])