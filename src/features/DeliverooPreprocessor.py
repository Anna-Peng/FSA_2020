from src.features.Preprocessor import Preprocessor
import json
import pandas as pd
import numpy as np
import re

##########################################################################################
# Deliveroo Preprocessor class, inherits from Preprocessor
##########################################################################################

class DeliverooPreprocessor(Preprocessor):

    def __init__(self, path_to_json):
        super().__init__(path_to_json)

    def get_dataframe(self):

        # Final dataframe columns description:
        #
        # PlatformBusinessName (or ad title for platforms such as Facebook, Etsy, etc.)
        # PlatformBusinessAddress (full address if available or approximate location otherwise)
        # PlatformPostCode
        # PlatformName (name of the platform scraped; e.g. deliveroo)
        # PlatformFHRSID (business FHRS id - if shown by the platform)
        # PlatformRatingValue (business FHRS score - if shown by the platform)
        # PlatformRating (business average rating given by the platform users)
        # PlatformTags (business food tags - e.g. chinese, chicken - provided by the platform)
        # PlatformDescription (business extra information provided by the platform)
        # ScrapingSearch (postcode search used to scrap the platform; e.g. E16AW)
        # ScrapingTime (scraping timestamp) 

        # Open .json file
        with open(self._path_to_json) as stream:
            try:
                data = json.load(stream)
            except ValueError as e:
                return pd.DataFrame()

        # Set function to return dictionary to df
        df=pd.DataFrame.from_dict(data)    

        # Replace all "not-found" with an empty string
        df.replace(to_replace='not-found', value='', inplace=True)

        # Remove location in 'heading'
        df['heading'] = [re.sub(r'\s\-.*','', string) for string in df.heading]

        # Add new columns to initiate the extraction of business addresses, postcodes, and food tags
        df['PlatformBusinessAddress']=''
        df['PlatformPostCode']=''
        df['PlatformTags']=''

        # Define functions to extract business addresses, postcodes, and food tags
        def out_regex(expression, regex_list):
            """
            Keeps everything that does not match a regex pattern 
            """
            regex = re.compile(expression)
            out_regex_list = filter(lambda i: not regex.search(i), regex_list)
            return list(out_regex_list)

        def in_regex(expression, regex_list): 
            """
            Keeps anything that matches a regex pattern
            """
            regex = re.compile(expression)
            in_regex_list = filter(lambda i: regex.search(i), regex_list)
            return list(in_regex_list)

        def add_space(str1):
            """
            Adds spaces between postcodes' incode and outcode
            """
            return re.sub(r'(([A-Za-z])|([0-9]))([0-9][A-Za-z]{2})', r'\1 \4', str1)

        # Define regex patterns to extract postcodes and food tags
        regex_pat_tags = '[0-9]+|View map|Free delivery'
        regex_pat_postcode = '([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})'

        # Generate regex lists for food tags and addresses
        tags = (out_regex(regex_pat_tags, item) for item in df.extras)
        address = (in_regex(regex_pat_postcode, item) for item in df.extras)

        # Assign values to the business addresses, postcodes, and food tags columns from the regex-generated lists 
        for i in df.index.array:
            df.loc[i, 'PlatformTags'] = next(tags)
            address_line = str(next(address)).strip(" [] '' ")
            df.loc[i, 'PlatformBusinessAddress'] = address_line
            postcode = re.search(regex_pat_postcode, df.loc[i, 'PlatformBusinessAddress']).group(0) if len(df.loc[i, 'PlatformBusinessAddress'])!=0 else ''
            df.loc[i, 'PlatformPostCode'] = add_space(postcode)

        # Add new column with info on the 'ScrapingSearch' 
        regex_pat_search = re.compile('_(.*?).json', flags=re.DOTALL)
        path_to_json_search = re.findall(regex_pat_search, self._path_to_json)
        

        # Add new column with info on the 'PlatformID' 
        regex_pat_platform = re.compile('raw/(.*?)_', flags=re.DOTALL)
        path_to_json_platform = re.findall(regex_pat_platform, self._path_to_json)
        df['PlatformName'] = 'Deliveroo'

        # Drop unnecessary columns
        df.drop(['extras'], axis=1, inplace=True)

        # Remove any business duplicates
        df = df.drop_duplicates(subset = ['heading', 'PlatformBusinessAddress']) 

        # Rename columns
        df = df.rename(columns={"heading": "PlatformBusinessName", 
                                "rating": "PlatformRating", 
                                "fsa_id": "PlatformFHRSID", 
                                "description": "PlatformDescription", 
                                "fhrs":"PlatformRatingValue", 
                                "time": "ScrapingTime",
                                "search_term1": "ScrapingSearch"})
       
        # Re-order columns
        df = df[['PlatformBusinessName',
            'PlatformBusinessAddress', 
            'PlatformPostCode', 
            'PlatformName', 
            'PlatformFHRSID', 
            'PlatformRatingValue',
            'PlatformRating', 
            'PlatformTags', 
            'PlatformDescription', 
            'ScrapingSearch', 
            'ScrapingTime']] 
        
        return df
