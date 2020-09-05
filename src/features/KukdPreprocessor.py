from src.features.Preprocessor import Preprocessor
import json
import pandas as pd
import numpy as np
import re

##########################################################################################
# Kukd Preprocessor class, inherits from Preprocessor
##########################################################################################

class KukdPreprocessor(Preprocessor):

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
              
        # Remove "\u0026" (i.e. &) for "and" in 'heading'
        regex_pat_ampersand = re.compile("\\\\u0026", flags=re.IGNORECASE)
        df['heading'] = df['heading'].str.replace(regex_pat_ampersand, 'and')

        # Replace "_" for "," in 'address_line1' 
        df['address_line1'] = [re.sub('_',',', string) for string in df.address_line1]

        # Replace "null" for "London" in 'address_line2'
        df['address_line2'] = [re.sub('null','London', string) for string in df.address_line2]

        # Replace nulls for "London" in 'address_line3' 
        df['address_line3'] = 'London' 

        # Merge address lines into one variable
        df['PlatformBusinessAddress'] = df['address_line1'] + ', ' + df['address_line2'] + ', ' + df['address_line3'] + ', ' + df['postcode']

        # Replace ", ," for "London" in 'address'
        df['PlatformBusinesAddress'] = [re.sub(', ,','London', string) for string in df.PlatformBusinessAddress]

        # Add new column with info on the 'ScrapingSearch' 
        regex_pat_search = re.compile('_(.*?).json', flags=re.DOTALL)
        path_to_json_search = re.findall(regex_pat_search, self._path_to_json)

        # Add new column with info on the 'PlatformID' 
        regex_pat_platform = re.compile('raw/(.*?)_', flags=re.DOTALL)
        path_to_json_platform = re.findall(regex_pat_platform, self._path_to_json)
        df['PlatformName'] = 'Kukd'

        # Drop unnecessary columns
        df.drop(['address_line1', 'address_line2', 'address_line3', 'has_halal_food', 'reviews_count'], axis=1, inplace=True)

        # Remove any business duplicates
        df = df.drop_duplicates(subset = ['heading', 'PlatformBusinessAddress'])

        # Rename columns 
        df = df.rename(columns={'heading':'PlatformBusinessName',
                                'postcode':'PlatformPostCode',
                                'cuisine_types':'PlatformTags',
                                'reviews_average':'PlatformRating',
                                'time':'ScrapingTime',
                                'search_term1': 'ScrapingSearch'})    

        # Add missing columns
        df['PlatformFHRSID'] = ''
        df['PlatformRatingValue'] = ''
        df['PlatformDescription'] = ''

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
