from src.features.Preprocessor import Preprocessor
import json
import pandas as pd
import re
from unicodedata import normalize

##########################################################################################
# Facebook Marketplace Preprocessor class, inherits from Preprocessor
##########################################################################################

class FbmktplacePreprocessor(Preprocessor):

    def __init__(self, path_to_json):
        super().__init__(path_to_json)

    ### This function output dataframe from the raw json file    
    def get_dataframe(self):
        with open(self._path_to_json) as stream:
            try:
                data = json.load(stream)
            except ValueError as e:
                return pd.DataFrame()

        # these are the names for the final dataframe
        # 
        #     PlatformBusinessName
        #     PlatformBusinessAddress
        #     PlatformPostCode
        #     PlatformName
        #     PlatformFHRSID
        #     PlatformRatingValue
        #     PlatformRating
        #     PlatformTags
        #     PlatformDescription
        #     ScrapingSearch
        #     ScrapingTime
        
        ## This function encode the string to latin-1 code
        # and then decode it to symbols (e.g. from \u00a to £) by unicode escape rules with UTF-8 (i.e. starting with '\')
        def decode_unicode(string, ignore_dicts = True):
            string = u"".join(string)
            string = string.encode("latin1").decode("unicode_escape")
            return normalize("NFC", string)

        ## Emojis here are made up with surrogate pairs of UTF-16 e.g. (\ud83d\udc4c)
        # the function encode the string to UTF-16 with the permission of surrogate pairs 
        # and then decode it back with UTF-16 rules
        def decode_emoji(string, ignore_dicts = True):
            string = u"".join(string)
            return string.encode("utf-16", "surrogatepass").decode("utf-16") # decode emoji


        ## This function tidy up random and redundant stuff: Very ugly part
        # convert \\n and \n to actual space
        # get rid of random \\
        # get rid of urls
        # convert all 'home made' into 'homemade'
        def clean_text(string):
            string = string.lower()
            string = string.split("\\n")
            string = " ".join(string)
            string = string.split("\n")
            string = " ".join(string)
            string = string.split("\\")
            string ="".join(string)
            string = string.replace("home made", "homemade")
            regex1 = r"https?:\\/\\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
            string = re.sub(regex1,"", string)
            regex2 = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
            string = re.sub(regex2,"", string)
            return string

        ## Make Dataframe
        df =pd.DataFrame.from_dict(data)
        df['PlatformName'] = 'Facebook Marketplace' # hard coded subject to future changes
        df['ScrapingSearch']= df['search_term1'] + ', ' + df['search_term2']
        df['PlatformTags']=''
        df['PlatformPostCode']=''
        df['PlatformRating']=''
        df['PlatformFHRSID']=''
        df['PlatformRatingValue']=''
        df = df.rename(columns={"heading": "PlatformBusinessName", "description": "PlatformDescription",   "location": "PlatformBusinessAddress", "time": "ScrapingTime"})
        df.drop(['search_term1', 'search_term2'], axis=1, inplace=True)
        # decode unicode to symbols (e.g. £)
        name = map(lambda x: decode_unicode(x), df["PlatformBusinessName"])
        description = map(lambda x: decode_unicode(x), df["PlatformDescription"])
        for i in df.index.array:
            df.loc[i, "PlatformBusinessName"] = next(name)
            df.loc[i, "PlatformDescription"] = next(description)

        # decode emoji if the string in the dataframe contains a surrogate pair starting with \ud83d or \ud83e
        name = map(lambda x: decode_emoji(x), df["PlatformBusinessName"])
        description = map(lambda x: decode_emoji(x), df["PlatformDescription"])
        for i in df.index.array:
            df.loc[i,"PlatformBusinessName"] = next(name)
            df.loc[i,"PlatformDescription"] = next(description)

        # further cleaning using clean_text function
        name = map(lambda x: clean_text(x), df["PlatformBusinessName"])
        description = map(lambda x: clean_text(x), df["PlatformDescription"])
        for i in df.index.array:
            df.loc[i, "PlatformBusinessName"] = next(name)
            df.loc[i, "PlatformDescription"] = next(description)

            
        # rearrange the order of columns    
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
