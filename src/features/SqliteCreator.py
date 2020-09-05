from src.features.DeliverooPreprocessor import DeliverooPreprocessor
from src.features.KukdPreprocessor import KukdPreprocessor
from src.data.FHRSWrapper import FHRSWrapper
import warnings
import pandas as pd
import sqlite3
import time
import re
from fuzzywuzzy import fuzz
import numpy as np

##################################################################################################
# SQLite Creator class
##################################################################################################

class SqliteCreator:

    def __init__(self, path='sqlite.db'):                                           
        """
        Creates Connection object for a sqlite database, and a Cursor object to execute commands
        arguments: self, path (string with path to database file)
        """
        self._conn = sqlite3.connect(path)
        self._crsr = self._conn.cursor()

    def create(self):
        self._crsr.execute('''
        CREATE TABLE IF NOT EXISTS RawInput(
            [FileName] TEXT,
            [InputType] TEXT);
        ''')
        self._crsr.execute('''
        CREATE TABLE IF NOT EXISTS Platforms(
            [PlatformID] INTEGER PRIMARY KEY,
            [PlatformName] TEXT,
            [PlatformURL] TEXT,
            [BusinessNameAvailable] TEXT, 
            [BusinessAddressAvailable] TEXT,
            [FHRSShown] TEXT,
            [FHRSRequiredOnRegistration] TEXT,
            [ScrapingRestrictions] TEXT,
            [ScrapingRestrictionsURL] TEXT,
            [OfficialAPIURL] TEXT,
            [RobotsURL] TEXT,
            [RobotsAllowsSearch] TEXT,
            [Javascript] TEXT,
            [LastUpdated] TEXT,
            [LastUpdatedBy] TEXT);            
        ''')
        self._crsr.execute('''
        CREATE TABLE IF NOT EXISTS PlatformTypes(
            [PlatformTypeID] INTEGER PRIMARY KEY, 
            [FoodType] TEXT,
            [ServiceType] TEXT,
            [BusinessModelType] TEXT,
            [Description] TEXT,
            [Examples] TEXT,
            [EcosystemNode] INTEGER);
        ''')
        self._crsr.execute('''
        CREATE TABLE IF NOT EXISTS PlatformPlatformTypes(
            [PlatformPlatformTypesID] INTEGER PRIMARY KEY,
            [PlatformID] INTEGER NOT NULL,
            [PlatformTypeID] INTEGER NOT NULL,
            FOREIGN KEY (PlatformID) 
                REFERENCES Platforms ([PlatformID]),
            FOREIGN KEY (PlatformTypeID) 
                REFERENCES PlatformTypes ([PlatformTypeID])
        )''')
        self._crsr.execute('''
        CREATE TABLE IF NOT EXISTS BusinessTypes(
            [BusinessTypeID] INTEGER PRIMARY KEY, 
            [BusinessTypeName] TEXT,
            [Registered] INTEGER,
            [FSABusinessTypeId] INTEGER,
            [FSABusinessTypeName] TEXT);
        ''')
        self._crsr.execute('''
        CREATE TABLE IF NOT EXISTS Establishments(
            [EstablishmentID] INTEGER PRIMARY KEY, 
            [BusinessTypeID] INTEGER, 
            [FHRSID] INTEGER,
            [BusinessName] TEXT,
            [BusinessAddress] TEXT,
            [PostCode] TEXT,
            [RatingValue] TEXT,
            [RatingDate] TEXT,
            [LocalAuthorityName] TEXT,
            [LocalAuthorityCode] INTEGER,
            FOREIGN KEY ([BusinessTypeID]) 
                REFERENCES BusinessTypes ([BusinessTypeID]) 
                    ON DELETE CASCADE 
                    ON UPDATE CASCADE);
        ''')
        self._crsr.execute('''
        CREATE TABLE IF NOT EXISTS Listings(
        [EstablishmentID] INTEGER, 
        [PlatformID] INTEGER,
        [PlatformBusinessName] TEXT,
        [PlatformBusinessAddress] TEXT,
        [PlatformPostCode] TEXT,
        [PlatformDescription] TEXT,
        [PlatformRatingValue] TEXT,
        [PlatformRating] TEXT,
        [PlatformTags] TEXT,
        [ScrapingSearch] TEXT,
        [ScrapingTime] TEXT,
        [IdentificationCertainty] TEXT,
        PRIMARY KEY ([EstablishmentID], [PlatformID]),
        FOREIGN KEY ([EstablishmentID]) 
            REFERENCES Establishments ([EstablishmentID]) 
                ON DELETE CASCADE 
                ON UPDATE CASCADE, 
        FOREIGN KEY ([PlatformID]) 
            REFERENCES Platforms ([PlatformID]) 
                ON DELETE CASCADE 
                ON UPDATE CASCADE);
        ''')

    def isRawinputPresent(self, fileloc, filetype):
        rawInput_filtered = self._crsr.execute('''
            SELECT [FileName]
            FROM RawInput
            WHERE [FileName] = ? AND [InputType] = ?
        ''', (fileloc, filetype)).fetchall()
        if len(rawInput_filtered) > 1:
            msg = "Raw file '%s' is present multiple times already!" % fileloc
            warnings.warn(msg)
        return(len(rawInput_filtered) != 0)

    def insertRawfile(self, fileloc, filetype):
        if not self.isRawinputPresent(fileloc, filetype):
            self._crsr.execute("INSERT OR REPLACE INTO RawInput ([FileName], [InputType]) VALUES (?, ?)", (fileloc, filetype))
            try: 
                df_raw = pd.read_csv(fileloc, error_bad_lines=False, warn_bad_lines=False)
            except pd.errors.EmptyDataError as error:
                print("SqliteCreator::insertRawfile WARNING...", error)
                return
            df_raw.to_sql(filetype, self._conn, if_exists='replace', index=False)

    def populatePlatforms(self, in_platforms, in_platformtypes, in_platformplatformtypes, in_businesstypes): 
        self.insertRawfile(in_platforms, "Platforms")
        self.insertRawfile(in_platformtypes, "PlatformTypes")
        self.insertRawfile(in_platformplatformtypes, "PlatformPlatformTypes")
        self.insertRawfile(in_businesstypes, "BusinessTypes")

    def insertListings(self, in_preprocessed): 
        if self.isRawinputPresent(in_preprocessed, "Preprocessed"):
            msg = "Preprocessing file '%s' is already in the database, skipping..." % in_preprocessed
            warnings.warn(msg)
            return None
        
        # Create FHRSWrapper object
        wrapper = FHRSWrapper()

        # Load preprocessed table
        try:
            df_preprocessed = pd.read_csv(in_preprocessed, error_bad_lines=False, warn_bad_lines=False)
        except pd.errors.EmptyDataError as error:
            print("SqliteCreator::insertListings WARNING...", error)
            return

        # get platform ID
        PlatformName = df_preprocessed['PlatformName'][0]
        PlatformID_table = self._crsr.execute('''
            SELECT [PlatformID]
            FROM Platforms
            WHERE upper([PlatformName]) = upper(?)
        ''', (PlatformName, )).fetchall()
        PlatformID = PlatformID_table[0][0]
        if(type(PlatformID) != int):
            print("Printing PlatformID:")
            print(PlatformID)
            raise("Something is wrong with the platform id!")

        # Request all establishments from the api at the outward postcodes of the listings
        # list of all unique outward codes to request from the api
        all_outward_postcodes = list(set([re.sub(r' *...$', '', pc) for pc in df_preprocessed['PlatformPostCode'].dropna()]))
        # Empty df to fill in
        df_FSA = pd.DataFrame(columns = ['FHRSID', 'BusinessTypeID', 'BusinessName', 'RatingValue', 'RatingDate', 'LocalAuthorityName',
        'LocalAuthorityCode', 'AddressLine1', 'AddressLine2', 'AddressLine3', 'AddressLine4', 'PostCode'])
        # copy of the empty df to populate each area with
        df_FSA_empty = df_FSA.copy()
        print("Fetching %i outward codes from the FHRS API" % len(all_outward_postcodes))
        for pc in all_outward_postcodes:
            FSAdata = wrapper.get_outward_postcode(pc)
            # Some potential troublesomes returns from the api, which are not tested when writing the code
            if FSAdata['meta']['returncode'] != "OK":
                warnings.warn("FHRS returncode is not 'OK', the results might be corrupted")
            if FSAdata['meta']['pageNumber'] != 1:
                warnings.warn("FHRS returned more than 1 page. The response structure is not tested, so there might be missing establishments")
            # Extract the relevant info from each establishment and concat to the collecting table
            FSAestablishments = FSAdata['establishments']
            df_FSA_1area = df_FSA_empty.copy()
            for colname in df_FSA_empty.columns:
                df_FSA_1area[colname] = [establishment[colname] for establishment in FSAestablishments]
            # Add to full list
            df_FSA = pd.concat([df_FSA, df_FSA_1area])
        # Rename some columns
        df_FSA = df_FSA.rename(columns = {'BusinessTypeID': 'FSABusinessTypeId'})
        # drop duplicates
        df_FSA = df_FSA.drop_duplicates()
        # create single column address
        df_FSA = (df_FSA
            .assign(BusinessAddress = lambda df: df["AddressLine1"] +" "+ df["AddressLine2"] + " " +df["AddressLine3"] +" "+ df["AddressLine4"] + " " +df["PostCode"])
            .assign(BusinessAddress = lambda df: [re.sub(r' +', ' ', address) for address in df['BusinessAddress']])
        )

        # add a rowid to df_preprocessed and a name approximation
        df_preprocessed = df_preprocessed.assign(preprocessedrowid = range(len(df_preprocessed.index)))

        # Empty establishments table
        df_establishments_empty = pd.DataFrame(columns = ['FSABusinessTypeId', 'BusinessName', 'BusinessAddress', 
        'PostCode', 'FHRSID', 'RatingValue', 'RatingDate', 'LocalAuthorityName', 'LocalAuthorityCode', 'preprocessedrowid'])
        # Create our establishments table at several levels of certainty (IdentificationCertainty)
        
        # TODO match with PlatformFHRSID
        df_establishments = (df_preprocessed
            .merge(df_FSA, how = "inner", left_on = ["PlatformFHRSID"], right_on = ["FHRSID"])
            .filter(df_establishments_empty.columns)
            .assign(IdentificationCertainty = 'confident: FHRSID from platform')
            )

        # List of hits not found yet
        unfoundkeys = list(set(df_preprocessed.preprocessedrowid).difference(set(df_establishments.preprocessedrowid)))
        # preprocessed merged with FSA hit based on postcode (when many to many match then new lines are created)
        df_preprocessed_unfound = df_preprocessed.copy().iloc[unfoundkeys]

        # IdentificationCertainty = 'confident: Postcode + full name'
        df_establishments = (df_preprocessed_unfound
            .merge(df_FSA, how = "inner", left_on = ["PlatformBusinessName", "PlatformPostCode"], right_on = ["BusinessName", "PostCode"],)
            .filter(df_establishments_empty.columns)
            .assign(IdentificationCertainty = 'confident: Postcode + full name')
            .append(df_establishments)
            )
        

        # IdentificationCertainty = 'medium: Postcode + partial name'
        #update list of hits not found yet
        unfoundkeys = list(set(df_preprocessed.preprocessedrowid).difference(set(df_establishments.preprocessedrowid)))
        # preprocessed merged with FSA hit based on postcode (when many to many match then new lines are created)
        df_preprocessed_unfound = df_preprocessed.copy().iloc[unfoundkeys]
        # Merge on postcode and fuzzy match score between the businessnames
        df_postcodemerge = (df_FSA
            .merge(df_preprocessed_unfound, how = "inner", left_on = ["PostCode"], right_on = ["PlatformPostCode"])
            .assign(name_fuzzy_token_set_ratio = lambda df: [fuzz.token_set_ratio(row["BusinessName"],row["PlatformBusinessName"]) for index,row in df.iterrows()] )
        )
        # Keep only those with fuzzy score > 80 + the highest per preprocessedrowid, then add to df_establishments
        df_establishments = (df_postcodemerge
            .query('name_fuzzy_token_set_ratio > 80')
            .sort_values(by=['name_fuzzy_token_set_ratio'], ascending=False)
            .groupby(by = ["preprocessedrowid"])
            .first()
            .reset_index()
            .filter(df_establishments_empty.columns + [""])
            .assign(IdentificationCertainty = 'medium: Postcode + partial name')
            .append(df_establishments)
        )
        
        
        # update lists of hits not found yet
        unfoundkeys = list(set(df_preprocessed.preprocessedrowid).difference(set(df_establishments.preprocessedrowid)))
        df_preprocessed_unfound = df_preprocessed.iloc[unfoundkeys]

        # TODO: Address matching (not implemented because it is unclear in how far the address is sufficient to assign to an establishment)

        # Look for BusinessType id of found business using FSABusinessTypeId
        df_businesstypes = pd.read_sql("SELECT * FROM BusinessTypes;", self._conn)
        df_establishments = df_establishments.merge(df_businesstypes[['FSABusinessTypeId', 'BusinessTypeID']])

        # fill table with unfound establishments
        df_establishments_unfound = df_establishments_empty.copy()
        df_establishments_unfound['BusinessName'] = df_preprocessed_unfound['PlatformBusinessName']
        df_establishments_unfound['BusinessAddress'] = df_preprocessed_unfound['PlatformBusinessAddress']
        df_establishments_unfound['PostCode'] = df_preprocessed_unfound['PlatformPostCode']
        df_establishments_unfound['preprocessedrowid'] = df_preprocessed_unfound['preprocessedrowid']
        df_establishments_unfound['IdentificationCertainty'] = 'not found'
        df_establishments_unfound['BusinessTypeID'] = df_businesstypes.query('Registered == 0')['BusinessTypeID'].tolist()[0]

        

        # Append unfound to found establishments and sort to original order
        df_establishments = (df_establishments
            .append(df_establishments_unfound)
            .sort_values(by=['preprocessedrowid'])
        )
        # order of estab and preprocessed is identical so we can append the [IdentificationCertainty]
        df_preprocessed = df_preprocessed.assign(IdentificationCertainty = df_establishments['IdentificationCertainty'].tolist())

        # list all columns except for the id
        establishments_truecolumns = list(pd.read_sql("SELECT * FROM Establishments LIMIT 1;", self._conn).columns)
        establishments_truecolumns = [col for col in establishments_truecolumns if col != "EstablishmentID"]

        map_estab = {}
        print("Populating establishments table")      
        for index, row in df_establishments.iterrows():
            insert_vals = [row[col] for col in establishments_truecolumns]
            match = (self._crsr
                .execute('''
                    SELECT EstablishmentID FROM Establishments
                        WHERE %s = ?
                        AND %s = ?
                        AND %s = ?
                        AND %s = ?
                        AND %s = ?
                        AND %s = ?
                        AND %s = ?
                        AND %s = ?
                        AND %s = ?
                    ''' % tuple(establishments_truecolumns),
                    tuple(insert_vals))
                .fetchone()
            )
            if match is None:
                lastrowid = self._crsr.execute('''
                    INSERT INTO Establishments(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    ''' % tuple(establishments_truecolumns),
                    tuple(insert_vals)).lastrowid
            else:
                lastrowid = match[0]
            map_estab[index] =  lastrowid


        # Populate Listings table   
        print("Populating listings table")        
        for index, row in df_preprocessed.iterrows():
            if index not in map_estab.keys():
                continue
            #print("Inserting row " + str(index) + " from " + PlatformName + " to Listings table")
            EstablishmentID = map_estab[index] 
            PlatformBusinessName = row['PlatformBusinessName']
            PlatformBusinessAddress = row['PlatformBusinessAddress']
            PlatformPostCode = row['PlatformPostCode']
            PlatformDescription = row['PlatformDescription']
            PlatformRatingValue = row['PlatformRatingValue']
            PlatformTags = row['PlatformTags']
            ScrapingSearch = row['ScrapingSearch']
            ScrapingTime = row['ScrapingTime']
            IdentificationCertainty = row['IdentificationCertainty']

            match = self._crsr.execute('''
                    SELECT * FROM Listings
                        WHERE PlatformBusinessName = ?
                        AND PlatformID = ?
                        AND EstablishmentID = ?
                    ''',
                    (PlatformBusinessName, PlatformID, EstablishmentID)).fetchone()
            if match is None:
                self._crsr.execute('''
                    INSERT OR REPLACE INTO Listings ([EstablishmentID], [PlatformID], [PlatformBusinessName], [PlatformBusinessAddress], [PlatformPostCode], [PlatformDescription], [PlatformRatingValue], [PlatformTags], [ScrapingSearch], [ScrapingTime], [IdentificationCertainty]) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (EstablishmentID, PlatformID, PlatformBusinessName, PlatformBusinessAddress, PlatformPostCode, PlatformDescription, PlatformRatingValue, PlatformTags, ScrapingSearch, ScrapingTime, IdentificationCertainty))
        
        # Adding the file to rawInput
        self._crsr.execute("INSERT OR REPLACE INTO RawInput ([FileName], [InputType]) VALUES (?, ?)", (in_preprocessed, "Preprocessed"))

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()
