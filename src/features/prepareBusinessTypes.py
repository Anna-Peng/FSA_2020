#!/usr/bin/python
import pandas as pd
import numpy as np
import warnings
import sys, getopt
from src.data.FHRSWrapper import FHRSWrapper
import json

def main(argv):
    out_businessTypes = ''
    try:
        opts, args = getopt.getopt(argv,"ho:",["out="])
    except getopt.GetoptError:
        print('prepareBusinessTypes.py -o <output.csv>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('prepareBusinessTypes.py -o <output.csv>')
            sys.exit()
        elif opt in ("-o", "--out"):
            out_businessTypes = arg
    #run the command
    if len(out_businessTypes):
       dat_businessTypes = prepareBusinessTypes()
       print("Saving businesstypes to file")
       dat_businessTypes.to_csv(out_businessTypes, index = False)
    else:
        print("Please provide the required output location")
        print('prepareBusinessTypes.py -o <output.csv>')
        sys.exit()
    
def prepareBusinessTypes():
    """
    stub 
    """
    # Obtain the FSA definitions of businessTypes
    wrapper = FHRSWrapper()
    decoded_response = wrapper.get_businesstypes()
    list_businesstypes = decoded_response['businessTypes'] 
    dat_businessTypes = pd.DataFrame()
    dat_businessTypes = (
        dat_businessTypes
        .assign(FSABusinessTypeId = [item["BusinessTypeId"] for item in list_businesstypes])
        .assign(FSABusinessTypeName = [item["BusinessTypeName"] for item in list_businesstypes])
        .assign(Registered = 1)
    )   
    dat_businessTypes['BusinessTypeName'] = dat_businessTypes['FSABusinessTypeName']
    other_row = pd.DataFrame(data = {"FSABusinessTypeId" : [np.nan], "FSABusinessTypeName": [np.nan], "Registered": [0], "BusinessTypeName": ["Unclassified business"]})
    dat_businessTypes = (
        dat_businessTypes
        .append(other_row)
        .assign(BusinessTypeID = range(1, (len(dat_businessTypes.index) + 2)))
        .filter(["BusinessTypeID", "BusinessTypeName", "Registered", "FSABusinessTypeId", "FSABusinessTypeName"])
    )
    return(dat_businessTypes)



if __name__ == "__main__":
    main(sys.argv[1:])
