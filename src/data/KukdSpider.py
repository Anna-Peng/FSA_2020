import scrapy
import re
from datetime import datetime

##################################################################################################
# Kukd Spider class
##################################################################################################

class KukdSpider(scrapy.Spider):
    name = "kukd"
    
    def start_requests(self):
        yield scrapy.Request(url='https://www.kukd.com/restaurants/%s' % self.postcode, callback=self.parse)
    
    def parse(self, response):
         
         script = response.css("script::text").get()
         #get cuisine ids into a dictionary
         cuisines = re.findall("cuisines.*\}],\"restaurant", script)[0]
         cuisines = re.sub(r'\].*','',cuisines)
         cuisines = re.sub(r'.*\[','',cuisines)
         cuisines = re.sub(r'\"name\":','',cuisines)
         cuisines = re.sub(r'\"id\":','',cuisines)
         cuisines = re.sub(r'\,"status\":[0-9]+','',cuisines)
         cuisines = re.sub(r'\,\"',':"',cuisines)
         cuisines = re.sub(r'[{}]','',cuisines)
         cuisines_dict = {key: val for key,val in (item.split(':') for item in cuisines.split(','))}
         
         #clean restaurant data
         matches = re.findall(r'\{\"logo_url\"(.*?)\}', script)
         matches = [re.sub('.*\.png\",','',match) for match in matches]
         matches = [re.sub('\,\"promotions_featured\":.*','',match) for match in matches]
         #replace commas between quotes with underscores
         matches = [re.sub(r'"[^"]*"', lambda x: x.group(0).replace(',', '_'), match) for match in matches]
         matches = [re.sub('\"','',match) for match in matches]
         
         
         #if on test mode, take only 2 restaurants
         if self.mode == "test":
             matches = matches[:2]
         elif self.mode == "run":
             matches = matches
        

         #split to select variables
         for rest in matches:
            res = {key: val for key,val in (item.split(':') for item in rest.split(','))} 
            dateTimeObj = datetime.now()
            yield{
                'heading': res['name'],
                'postcode': res['postcode'],
                'address_line1': res['address_line1'], 
                'address_line2': res['address_line2'], 
                'address_line3': res['address_line3'],
                'reviews_count': res['reviews_count'],
                'cuisine_types': re.sub(r'\"','',cuisines_dict[res['cuisine_types']]),
                'reviews_average':res['reviews_average'],
                'has_halal_food':res['has_halal_food'],
                'search_term1':self.postcode,
                'time': dateTimeObj
            }
