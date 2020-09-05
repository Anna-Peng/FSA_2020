#!/usr/bin/python
import pandas as pd
import warnings
import sys, getopt
# python3 src/features/preparePlatforms.py -i data/raw/Platforms_editable.csv -j data/raw/PlatformTypes_editable.csv -k data/interim/Platforms_dbpart.csv -l data/interim/platformPlatformTypes_dbpart.csv -m data/interim/platformTypes_dbpart.csv 
def main(argv):
    in_platforms = ''
    in_platformTypes = ''
    out_platforms = ''
    out_platformPlatformTypes = ''
    out_platformTypes = ''
    try:
        opts, args = getopt.getopt(argv,"hi:j::k:l:m:",["inplat=","intype=", "outplat=", "outlink=", "outtype="])
    except getopt.GetoptError:
        print('preparePlatforms.py -i <in_platforms.csv> -j <in_platformTypes.csv> -k <out_platforms.csv> -l <out_platformPlatformType> -m <out_platformTypes>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('preparePlatforms.py -i <in_platforms.csv> -j <in_platformTypes.csv> -k <out_platforms.csv> -l <out_platformPlatformType> -m <out_platformTypes>')
            sys.exit()
        elif opt in ("-i", "--inplat"):
            in_platforms = arg
        elif opt in ("-j", "--intype"):
            in_platformTypes = arg
        elif opt in ("-k", "--outplat"):
            out_platforms = arg
        elif opt in ("-l", "--outlink"):
            out_platformPlatformTypes = arg
        elif opt in ("-m", "--outtype"):
            out_platformTypes = arg
    #run the command
    if all(len(f) > 0 for f in [in_platforms, in_platformTypes, out_platforms, out_platformPlatformTypes,out_platformTypes]):
        # Read in data
        dat_platforms = pd.read_csv(in_platforms)
        dat_platformTypes = pd.read_csv(in_platformTypes)
        # prepare the tables
        (dat_platforms_db, dat_platformPlatformType_db, dat_platformTypes_db) = preparePlatforms(dat_platforms, dat_platformTypes)
        # Write to file
        dat_platforms_db.to_csv(out_platforms, index = False)
        dat_platformPlatformType_db.to_csv(out_platformPlatformTypes, index = False)
        dat_platformTypes_db.to_csv(out_platformTypes, index = False)
    else:
        print("Please provide all the required files")
        print('usage: preparePlatforms.py -i <in_platforms.csv> -j <in_platformTypes.csv> -k <out_platforms.csv> -l <out_platformPlatformType> -m <out_platformTypes>')
        sys.exit()
    
def preparePlatforms(dat_platforms, dat_platformTypes):
    """
    Takes two pandas dataframes (dat_platforms, dat_platformTypes), combines them,
    removes redundancy, checks for errors and returns a tuple of three pandas dataframes 
    (dat_platforms_db, dat_platformPlatformType_db, dat_platformTypes_db)
    (1) is a unique list of platforms, (2) a linking table that connect platforms to
    platformtypes and (3) a unique list of platformtypes.

    """
    # add row ids (1 based therefore +1)
    dat_platforms_unique = (
        dat_platforms.
        filter(["PlatformName"])
        .drop_duplicates()
    )
    dat_platforms_rowid = (
        dat_platforms_unique
        .assign(PlatformID = range(1, (len(dat_platforms_unique.index) + 1)))
        .merge(dat_platforms, "outer")
    )
    dat_platformTypes_rowid = (
        dat_platformTypes
        .assign(PlatformTypeID = range(1, (len(dat_platformTypes.index) + 1)))
    )

    # Now print if any platforms are missing a type(Problem!) and vice versa (not a problem)
    dat_platforms_unmatched = (pd.merge(dat_platforms_rowid, dat_platformTypes_rowid, how = "left", on = ["FoodType", "ServiceType", "BusinessModelType"])
                           .query('PlatformTypeID.isnull()'))
    if dat_platforms_unmatched.shape[0]:
        print("Printing unmatched platforms")
        print(dat_platforms_unmatched)
        raise("Some platforms don't have a matching platformType, check the input for errors")
    
    dat_platformtypes_unmatched = (pd.merge(dat_platforms_rowid, dat_platformTypes_rowid, how = "right", on = ["FoodType", "ServiceType", "BusinessModelType"])
                           .query('PlatformID.isnull()'))
    if dat_platformtypes_unmatched.shape[0]:
        print("Printing unmatched platformTypes")
        print(dat_platformtypes_unmatched)
        warnings.warn("The above platformTypes don't have any assigned platforms")

    # Creating the 3 cleaned up tables
    # 1_platformPlatformType:  Inner join of the two tables, entries that are only in 1 list are thrown out
    # Then take only combinations of PlatformID and PlatformTypeID 
    dat_platformPlatformType_db = (
        pd.merge(dat_platforms_rowid, dat_platformTypes_rowid, how = "inner", on = ["FoodType", "ServiceType", "BusinessModelType"])
        .filter(["PlatformID", "PlatformTypeID"])
        .drop_duplicates()
        )
    
    # 2_platforms: select correct columns only and drop duplicates
    dat_platforms_db = (
        dat_platforms_rowid
        .filter(["PlatformID", "PlatformName", "PlatformURL", "BusinessNameAvailable", "BusinessAddressAvailable", "FHRSShown", "FHRSRequiredOnRegistration", "ScrapingRestrictions", "ScrapingRestrictionsURL", "OfficialAPIURL", "RobotsURL", "RobotsAllowsSearch", "Javascript", "LastUpdated", "LastUpdatedBy"])
        .drop_duplicates())
    # 3_platformTypes: select correct columns only and drop duplicates
    dat_platformTypes_db = (
        dat_platformTypes_rowid
        .filter(["PlatformTypeID","FoodType", "ServiceType", "BusinessModelType", "Description", "Examples", "EcosystemNode"])
        .drop_duplicates())
    return(dat_platforms_db, dat_platformPlatformType_db, dat_platformTypes_db)


if __name__ == "__main__":
    main(sys.argv[1:])