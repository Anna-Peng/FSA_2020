import scrapy
import re
from datetime import datetime

##################################################################################################
# Deliveroo Spider class
##################################################################################################

class DeliverooSpider(scrapy.Spider):
	name = "deliveroo"
	#deliveroo changes /london/london in the url if another postcode is used
	def start_requests(self):
		yield scrapy.Request(url='https://deliveroo.co.uk/restaurants/london/london?fulfillment_method=DELIVERY&postcode=%s' % self.postcode, callback=self.parse)
        
	def parse(self, response):

		#if on test mode, take only 2 restaurants
		if self.mode == 'test':
			for listing in response.css('div.HomeFeedUICard-9e4c25acad3130ed')[:2]:
				listing_page_link = listing.css('a::attr(href)')
				listing_page_link = listing_page_link[:2]
				yield from response.follow_all(listing_page_link, self.parse_listing)

		# follow the link for each listing in the current page
		elif self.mode == 'run':
			for listing in response.css('div.HomeFeedUICard-9e4c25acad3130ed'):
				listing_page_link = listing.css('a::attr(href)')
				yield from response.follow_all(listing_page_link, self.parse_listing)

	def parse_listing(self,response):
		dateTimeObj = datetime.now()
		#get fsa_id
		hygiene = response.css("script::text")[2].get()# this [2] can be replaced with a search for more robustness
		fsa_url = re.findall("\"link_url\":\"https://ratings.food.gov.uk/business/en-GB/[0-9]+\"",hygiene)
		#check that there is an id in the fsa_url
		if not fsa_url:
			fsa_id = 'not-found'
		else:
			fsa_id = re.sub('.*\/','',fsa_url[0])
			fsa_id = re.sub('\"', '', fsa_id)
		#check if there is an image with FHRS (check for the alternate text) - helpful if logged in
		fhrs_score_url = re.findall("\"hygiene_rating_image_alt_text\":\"The FSA food hygiene rating is [0-9]", hygiene)
		if not fhrs_score_url:
			fhrs = 'not-found'
		else:
			fhrs = re.sub('.* is ','',fhrs_score_url[0])
		yield {
			'heading': response.css('h1.ccl-2a4b5924e2237093::text').get(default='not-found'),
			'rating': response.css('div.orderweb__61671603').css('span.ccl-b308a2db3758e3e5::text').get(default='not-found'),
			'description': response.css('p.ccl-19882374e640f487').css('::text').get(default='not-found'),
			'fsa_id': fsa_id,
			'fhrs': fhrs,
			'extras': response.css('div.ccl-9aab795066526b4d').css('ul').css('li').css('span::text').getall(),
			'search_term1': self.postcode,
			'time': dateTimeObj
		}
