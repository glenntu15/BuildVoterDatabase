from UsefulFunctions import *
from Config import Configurations
import time
import math

nrecs_write = 0
nrecs_read = 0

min_rec = math.inf
max_rec = 0
avg_rec = 0

split_chr = ","
# test_dict = dict()

def keep_rec_summary_stats(rec:int, l:str):
    global nrecs_write
    global min_rec
    global max_rec
    global avg_rec

    nrecs_write += 1
    if rec < min_rec:
        min_rec = rec
    elif rec > max_rec:
        max_rec = rec
    
    a = 1 / nrecs_write
    avg_rec = a*rec + (1-a)*avg_rec


def build_s3(file:str):
    global test_dict

    with open(file, "r") as f:
        l = f.readline()
        l = f.readline()
        while l:
            l = l[:Configurations.LRECL]

            keep_rec_summary_stats(len(l), l)

            record = l.split(split_chr)

            if nrecs_write > 2448773:
                ## Upload String to Bucket
                upload_string(record[1], Configurations.S3_BUCKET_NAME, l)
            
            ## Test:
            # test_dict[int(record[1])] = l
            
            l = f.readline()

def build_voter_database_s3():
    t1 = time.process_time()
    file = Configurations.DATA_PATH + Configurations.DATA_FILE_NAME_ROOT
    for i in range(5):
        build_s3(file + str(i) + ".csv")
    t2 = time.process_time()
    print(" nrecs: ", nrecs_write, " (min, avg, max) record length: ", (min_rec, avg_rec, max_rec))
    print(" ====> time to build dictionary: ", (t2-t1))


def read_s3(file:str):
    global nrecs_read

    with open(file, "r") as f:
        l = f.readline()
        while l:
            record = l.split(split_chr)

            # s3_file_name = record[1].rjust(8, "0")
            # print("File:", s3_file_name)
            # if nrecs_read >= 5:
            #     break

            ## Download File from Bucket
            download_file(record[1].rjust(8, "0"), Configurations.S3_BUCKET_NAME)
            
            ## Test:
            # test = test_dict[int(record[1])]
            # if nrecs_read % 10000 == 0:
            #     print( nrecs_read, test)
            
            if nrecs_read % 50000 == 0:
                print("Read thus far:", nrecs_read)
            
            nrecs_read += 1
            l = f.readline()
    
    print(" nrecs read: ", nrecs_read)

def read_voter_database_s3():
    keyfile = "searchkeys.txt"
    keyfile = 'data/' + keyfile
    keyfile = os.path.join(os.getcwd(), keyfile)

    t1 = time.process_time()
    read_s3(keyfile)
    t2 = time.process_time()
    print(" ====> time to read records: ", (t2-t1))


def main():
    Configurations.initialize_default()

    ## MUST `init_boto3()` Prior to All AWS S3 Functions
    init_boto3()
    ## Create Bucket:
    # create_bucket()

    # build_voter_database_s3()
    # print()
    read_voter_database_s3()


if __name__ == "__main__":
    main()