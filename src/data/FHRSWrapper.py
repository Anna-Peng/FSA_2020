import pandas as pd
import requests
import json
from os import path, makedirs
import time

##################################################################################################
# FHRS Wrapper class
##################################################################################################

class FHRSWrapper:
    
    def __init__(self, cache = ".FHRS-cache/"):
        self.session = requests.Session()
        self.web_root = 'http://api.ratings.food.gov.uk/'
        self.headers = {'x-api-version': '2', 'accept': 'application/json'}
        self.outward_postcode_cache = {}
        self.cache = cache
        
    def get_businesstypes(self):
        """Returns decoded JSON according to https://api.ratings.food.gov.uk/Help/Api/GET-BusinessTypes
        """
        url = self.web_root + 'BusinessTypes/'
        response = self.session.get(url, headers=self.headers)

        try:
            response_json = json.loads(str(response.text))
        except ValueError as error:
            print("FHRSWrapper::get_businesstypes WARNING...", error)
            return None

        return response_json

    def get_establishments_id(self, fhrsid):
        """Returns decoded JSON according to https://docs.python.org/2/library/json.html#json-to-py-table 
            
            Arguments
            ---------
            This is a python wrapper around https://api.ratings.food.gov.uk/Help/Api/GET-Establishments-id
        """
        url = self.web_root + 'Establishments/' + str(fhrsid)
        response = self.session.get(url, headers=self.headers)

        try:
            response_json = json.loads(str(response.text))
        except ValueError as error:
            print("FHRSWrapper::get_establishments_id WARNING...", error)
            return None

        return response_json

    def get_establishments(self, **kwargs):
        """Returns decoded JSON according to https://docs.python.org/2/library/json.html#json-to-py-table 

            Arguments
            ---------
            This is a python wrapper around https://api.ratings.food.gov.uk/Help/Api/GET-Establishments_name_address_longitude_latitude_maxDistanceLimit_businessTypeId_schemeTypeKey_ratingKey_ratingOperatorKey_localAuthorityId_countryId_sortOptionKey_pageNumber_pageSize
        """
        # construct URL according to the format set by the FHRS API
        url = self.web_root + 'Establishments?'
        for key, value in kwargs.items(): 
            url = url + f"{key}={value}" + "&"
        url = url[:-1] #remove last ampersand
        
        response = self.session.get(url, headers=self.headers)

        try:
            response_json = json.loads(str(response.text))
        except ValueError as error:
            print("FHRSWrapper::get_establishments WARNING...", error)
            return None

        return response_json
    
    def get_outward_postcode(self, outward_postcode: str) -> list:
        """Returns decoded JSON in the same manner as get_establishments, but caches the results in the cache directory
        """
        if not path.exists(self.cache):
            print(f"Creating cache directory at {self.cache}")
            makedirs(self.cache)
        if type(outward_postcode) != str:
            raise("Please provide a single outward_postcode of type string")
        filename = path.join(self.cache, outward_postcode + '.json')
        if path.exists(filename):
            print("Retrieving '%s' from cache" % filename)
            with open(filename) as injson:
                outward_response = json.load(injson)
        else:
            outward_response = self.get_establishments(address = outward_postcode)
            print("Creating '%s'" % filename)
            with open(filename, 'w') as outjson:
                json.dump(outward_response, outjson)
        return(outward_response)
