"""
A driver for the snapsort coding challenge.

Given a set of listings and products as defined at http://blog.snapsort.com/coding-challenge/, returns a list
of results mapping products to their listings.
"""
from __future__ import with_statement
import json as simplejson
import string
import sys
import time

try:
  import multiprocessing
except ImportError:
  print "multiprocessing module not found; please run under at least Python 2.6"
  sys.exit()

BOUNDARIES = [' ', '-','(', ')']
NUKE_TABLE = dict((ord(char), None) for char in u' -()')

def load_products():
  """
  Loads the JSON-encoded products from products.txt into an in-memory representation.

  This function also processes the manufacturer/model/family strings to enable fuzzy-matching later on.
  """
  products = []

  with open('products.txt') as products_txt:
    for line in products_txt:
      product = simplejson.loads(line)
      for x in ['family', 'manufacturer', 'model']:
        if product.get(x, None) != None:
          product[x + '_'] = product[x].translate(NUKE_TABLE).lower()
      products.append( product )
  return products

def fuzzy_match( haystack, needle, settings=[None,{},None] ):
  """
  Checks for the presence of the needle in the haystack.

  settings is used to cache search results for a given needle in a haystack until the haystack changes.
  Common delimiters in the haystack like space, -, ( and ) will not impede a match.
  The needle should be lowercase and stripped of spaces, -, ( and ) characters.
  The needle must lie within a word boundary.

  >>> fuzzy_match( "Canon SX-1200-IS", "sx1200is" )
  6
  >>> fuzzy_match( "Sony T-DSC9/K", "tdsc9" )
  5
  >>> fuzzy_match( "Canon SX-1200-IS", "sony")
  -1
  >>> fuzzy_match( "Canon SX1200(IS)", "sx1200is")
  6
  >>> fuzzy_match( "Panasonic FX25", "fx2")
  -1
  """

  if haystack != settings[0]:
    settings[0] = haystack
    settings[1] = {}
    settings[2] = haystack.translate(NUKE_TABLE).lower()

  if needle in settings[1]:
    return settings[1][needle]

  #If the needle isn't in the stripped haystack, don't bother with the exhaustive search below
  #that applies the rest of the word boundary rules.
  quick_start = settings[2].find( needle )
  if quick_start < 0:
    return -1

  can_match = True

  for x in xrange(quick_start, len(haystack)-len(needle)+1):
    if haystack[x] in BOUNDARIES:
      can_match = True
      continue

    if not can_match:
      continue

    can_match = False
    xprime = x
    needle_match = 0
    while needle_match < len(needle) and xprime < len(haystack):
      if haystack[xprime] in BOUNDARIES:
        xprime += 1
        continue

      if haystack[xprime].lower() == needle[needle_match]:
        needle_match += 1
        xprime += 1
        continue
      break

    if needle_match == len(needle):
      if xprime == len(haystack) or (haystack[xprime] in BOUNDARIES) or (haystack[xprime].isalpha() ^
          haystack[xprime-1].isalpha()) or (haystack[xprime].isdigit() ^ haystack[xprime-1].isdigit()):
        settings[1][needle] = x
        return x

  settings[1][needle] = -1
  return -1

def match_product( listing, products ):
  """
  Determine which product a listing matches.

  Returns an array of matching products.

  Accessories shouldn't match.
  >>> match_product( { 'title' : 'Battery for Sony DSC-T99' }, [ {'product_name' : 'T99', 'manufacturer_' : 'sony','model_' : 'dsct99' } ] )
  []

  Straightforward examples should match.
  >>> match_product( { 'title' : 'Sony DSC-T99' }, [ {'product_name' : 'T99', 'manufacturer_' : 'sony', 'model_' : 'dsct99' } ] )
  ['T99']

  Small permutations like whitespace and hyphen differences should match. Filtering of cases like this (WG-1-GPS vs
  WG-1) is handled in a post processing step.
  >>> match_product( { 'title' : 'Pentax WG 1 GPS' }, [ {'product_name' : 'WG-1-GPS', 'manufacturer_' : 'pentax', 'family_' : 'optio', 'model_' : 'wg1gps' }, { 'product_name': 'WG-1', 'manufacturer_' : 'pentax', 'family_' : 'optio', 'model_' : 'wg1' } ] )
  ['WG-1-GPS', 'WG-1']

  If all 3 criteria match, it should win out over only 2 criteria matching.
  >>> match_product( { 'title' : 'Pentax Optio WG 1 GPS' }, [ {'product_name' : 'WG-1-GPS', 'manufacturer_' : 'pentax', 'family_' : 'optio', 'model_' : 'wg1gps' }, { 'product_name': 'WG-1', 'manufacturer_' : 'pentax', 'model_' : 'wg1' } ] )
  ['WG-1-GPS']
   """

#English/French/German for 'for' is a strong indicator that the product is an accessory,
#e.g., "Battery pack for Sony DSC-T99"
  lower_title = listing['title'].lower()
  if lower_title.find(' for ') > 0 or lower_title.find(' pour ') > 0 or lower_title.find(u' f\u00fcr ') > 0:
    return []

  family_hits = []
  all_hits = []
  title = listing['title']
  for x in products:
    family_ = x.get('family_', None)
    manufacturer_ = x['manufacturer_']
    model_ = x['model_']
    if fuzzy_match( title, manufacturer_ ) >= 0 and fuzzy_match( title, model_ ) >= 0:
      all_hits.append(x['product_name'])
      if family_ != None and fuzzy_match(title, family_) >= 0:
        family_hits.append( x['product_name'] )

  #Prefer to return only hits which matched all 3 criteria of manufacturer, family and model
  #This will create some false rejections, for example Olympus Mju 9010 and Olympus Stylus 9010
  #Arguably, that's a data cleanliness issue in the products source file and is best addressed
  #there.
  if len(family_hits)>0:
    return family_hits

  return all_hits

def match_listings(q, modulo, divisor):
  """
  Match a subset of all the listings and return them in q.

  This will match 1/divisor of the listings and place a dictionary from product to listings
  into the queue.
  """
  product_to_listings = {}
  products = load_products()

  listings = []
  count = 0
  with open('listings.txt') as listings_txt:
    for x in listings_txt:
      if count % divisor == modulo:
        listings.append(simplejson.loads(x))
      count += 1

  index = modulo
  for listing in listings:
    product_names = match_product( listing, products )
    result = None
    if len(product_names) > 0:
      #If all matches are substrings of the longest match, use the longest match.
      #This handles Pentax-WG-1-GPS vs Pentax-WG-1 ambiguity.
      longest_match = max(product_names)
      if ''.join([str(longest_match.find(product_name)) for product_name in product_names]) == '0'*len(product_names):
        result = longest_match
      elif len(product_names) == 1:
        #Exact match
        result = product_names[0]

    if result != None:
       product_matches = product_to_listings.get( result, [] )
       if len(product_matches) == 0:
         product_to_listings[result] = product_matches

       product_matches.append(index)

    index += divisor

  q.put( product_to_listings )


if __name__ == '__main__':
  if len(sys.argv) == 1:
    print "usage: go.py <number-of-processes>"
    sys.exit()

  num_processes = int(sys.argv[1])

  q = multiprocessing.Queue()
  processes = [multiprocessing.Process(target = match_listings, args = (q, x, num_processes)) for x in range(num_processes)]
  [p.start() for p in processes]
  results = [q.get() for p in processes]

  merged_results = {}
  for result in results:
    for product_name,matching_listings in result.iteritems():
      if product_name in merged_results:
        merged_results[product_name].extend(matching_listings)
      else:
        merged_results[product_name] = matching_listings

  with open('listings.txt') as listings_txt:
    listings = [simplejson.loads(x) for x in listings_txt]

  for product_name, matching_listings in merged_results.iteritems():
    tmp = { 'product_name' : product_name, 'listings' : [listings[x] for x in matching_listings] }
    print simplejson.dumps( tmp )
