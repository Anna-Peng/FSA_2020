import scrapy
import re
from datetime import datetime
import time
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

##################################################################################################
# Facebook Marketplace Spider class
##################################################################################################

class FBmktplaceSpider(scrapy.Spider):
    name = 'fbmktplace'

    #open selenium to grab list of item urls
    def get_item_urls_selenium(self,url):
        #this option prevents new browser windows from opening with every link
        op = webdriver.ChromeOptions()
        op.add_argument('headless')
        #install chromedriver, alternatively download chromedriver to local and point to path
        driver = webdriver.Chrome(ChromeDriverManager().install(), options = op)
        driver.get(url)        
        time.sleep(1.0)
        
        #scroll to bottom of page
        #scroll_pause_time has to be increased to ensure all items are retrieved
        scroll_pause_time = float(self.scroll_pause_time)
        #if on test mode reduce scroll_pause_time
        if self.mode == 'test':
            scroll_pause_time = 0.5
        elif self.mode == 'run':
            scroll_pause_time =scroll_pause_time
        print('scroll_pause_time set to', scroll_pause_time)
        # Get scroll height
        last_height = driver.execute_script("return document.body.scrollHeight")
        #wait for scroll_pause_time seconds, go to bottom and check if page height has changed
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")        
            # Wait to load page
            time.sleep(scroll_pause_time)        
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height        
    
        #get item urls
        listings = driver.find_elements_by_class_name('oajrlxb2')#this could break if/when FB structure changes
        item_urls = [l.get_attribute('href') for l in listings]
        item_urls = [u for u in item_urls if u]
        item_urls = [u for u in item_urls if '/marketplace/item/' in u]
        if self.mode == 'test':
            item_urls = item_urls[:2]
        print(len(item_urls),'items found')
        
        #max_n_results limits the number of items that are grabbed to avoid problems with scraping FB data
        max_n_results = int(self.max_n_results)
        urls = item_urls[:max_n_results]
        print('retrieving a maximum of',max_n_results,'items')
        return(urls)        
        
    def start_requests(self):
        #city and keywords arguments are used here
        urls = self.get_item_urls_selenium(url='https://www.facebook.com/marketplace/'+str(self.city)+'/search/?query='+str(self.keywords))
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_listing)
   
    
    def parse_listing(self, response):
        dateTimeObj = datetime.now()
        #this looks for the target info inside the javascript
        listing_script = response.css("script")
        listing_script = [s for s in listing_script if 'location_text' in s.get()]
        
        #clean title, location_text and description from script
        title = re.findall('\"marketplace_listing_title\"\:.*?\"\,',listing_script[0].get())
        title = re.sub(r'\"marketplace_listing_title\"\:','',title[0])
        title = title.rstrip('\,')
        title = title.strip('\"')
        location = re.findall('\"location_text\"\:\{.*?\}',listing_script[0].get())
        location = re.sub(r'.*\{\"text\":','',location[0])
        location = location.rstrip('\}')
        location = location.strip('\"')
        description = re.findall('\"redacted_description\"\:\{.*?\}',listing_script[0].get())  
        description = re.sub(r'.*\{\"text\":','',description[0])
        description = description.rstrip('\}')
        description = description.strip('\"')
        #output to json
        yield {
            'heading': title,
            'description': description,
            'location': location,
            'search_term1':self.city,
            'search_term2':self.keywords,
            'time': dateTimeObj
        }
            


