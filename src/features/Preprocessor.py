##########################################################################################
# Preprocessor class
##########################################################################################

class Preprocessor:

    def __init__(self, path_to_json):
        self._path_to_json = path_to_json

    def print_parameters(self):
        print(f"----------------------------------------------------")
        print(f"Loading data from {self._path_to_json}...")
        print(f"----------------------------------------------------")
       
    def get_dataframe(self):
        '''
        Dataframe columns description:
        
        PlatformBusinessName (or ad title for platforms such as Facebook, Etsy, etc.)
        PlatformBusinessAddress (full address if available or approximate location otherwise)
        PlatformPostCode
        PlatformName (name of the platform scraped; e.g. deliveroo)
        PlatformFHRSID (business FHRS id - if shown by the platform)
        PlatformRatingValue (business FHRS score - if shown by the platform)
        PlatformRating (business average rating given by the platform users)
        PlatformTags (business food tags - e.g. chinese, chicken - provided by the platform)
        PlatformDescription (business extra information provided by the platform)
        ScrapingSearch (postcode search used to scrap the platform; e.g. E16AW)
        ScrapingTime (scraping timestamp)
        '''
        pass
