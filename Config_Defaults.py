class DefaultConfigurations:
    BLOCKSIZE = 4096
    LRECL = 128
    DRECL = 16

    VOTERS_TO_PROC = 500000
    
    #DATA_PATH = "C:\\Users\\dfern\\OneDrive\\Documents\\UH\\Senior\\Spring2023\\COSC6376\\code\\tmp\\"
    DATA_PATH = "C:/tmp/"
    DATA_FORMAT = "csv"
    DATA_ATTRIBUTES = ["placeholder"]   # DF: Add header here
    DATA_FILE_NAME_ROOT = "registered_voters" # Might also be "test_file"
    optimize_reads = False
    DATABASE_PATH = "./"
    DATABASE_NAME ="database.bin"

    ## S3:
    S3_REGION = "us-east-2"
