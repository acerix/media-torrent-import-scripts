#!/usr/bin/env python3

import json
from urllib.parse import quote_plus
import requests
from lxml import etree
import re
import datetime

# config
import config

# media-torrent-db stuff
import mediatorrentdb

base_url = 'https://eztv.ag/'
start_page_number = 0
last_page_number = 2000

# database connection
db = config.db_connect()
db_cursor = db.cursor()

# prepare session to use for requests
requests_session = requests.Session()
requests_session.headers.update({
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
})


# @dev
from pprint import pprint


# get a list of episode magnets on the index page
def get_index_page_episode_magnets(page_number = 0):
  magnets = []

  page_url = base_url
  if page_number:
    page_url = page_url + 'page_' + str(page_number)

  # cache the downloaded index page so it can be tested without having to download the page each time
  cache_filename = './cache/page_' + str(page_number) + '.html'

  # @dev try reading cached page
  try:
    with open(cache_filename, 'r') as cached_page_file:
      response_text = cached_page_file.read()

  except EnvironmentError:

    # download page
    response = requests_session.get(page_url)

    # @dev save downloaded page to cache
    cached_page_file = open(cache_filename, 'w')
    cached_page_file.write(response.text)
    cached_page_file.close()

    response_text = response.text

  # parse html into tree
  dom_tree = etree.fromstring(
    response_text,
    parser = etree.HTMLParser(),
    base_url = base_url
  )

  # find episode links
  magnet_els = dom_tree.xpath('//a[@class="epinfo"]')

  for magnet_el in magnet_els:
    magnet = {}
    magnet['release_name'] = magnet_el.text.strip()

    # match episode code (eg. S02E13 or S2017E11) or episode date (eg. 2017 12 31)
    # IGNORECASE eg. "S04e14"
    episode_code_match = re.match(r'(?P<title>.*)(S(?P<season>\d{2,4})E(?P<episode>\d{2})|(?P<release_date>(20|19)\d\d [01]\d [0123]\d))(?P<release_name>.*)', magnet_el.text, re.IGNORECASE)
    if episode_code_match:
      release_date = episode_code_match.group('release_date')

    else:
      # check for format like "Collection 1 10of10" or just "5of5"
      episode_code_match = re.match(r'(?P<title>.*)((Collection (?P<season>\d+) )?(?P<episode>\d+)of\d+)(?P<release_name>.*)', magnet_el.text, re.IGNORECASE)
      release_date = None

    if episode_code_match:
      # most episodes match...
      magnet['series_title'] = episode_code_match.group('title').strip()
      magnet['season'] = episode_code_match.group('season')
      if magnet['season'] is None:
        magnet['season'] = 1 # assume no season is first season
      magnet['episode'] = episode_code_match.group('episode')
    else:
      # @todo strip quality, release info, etc., try to get series/episode number
      # eg. "Ch4 Big Ben Saving the Worlds Most Famous Clock 1080i HDTV MVGroup mkv [eztv]" should be "Big Ben Saving the Worlds Most Famous Clock"
      magnet['series_title'] = magnet_el.text.strip()
      magnet['season'] = magnet['episode'] = 1


    if len(magnet['series_title']) < 2:
      print('Title too short')
      pprint(magnet_el.text)
      pprint(magnet['series_title'])
      exit()

    # put date in ISO format, ensuring it is valid
    if release_date:
      try:
        parsed_release_date = datetime.datetime.strptime(release_date, '%Y %m %d')
      except ValueError:
        # where ya'll from? eg. "2018 17 02"
        parsed_release_date = datetime.datetime.strptime(release_date, '%Y %d %m')
      magnet['release_date'] = parsed_release_date.strftime('%Y-%m-%d')

      # convert release date to eg. Season 2018 Episode 123
      magnet['release_date'] = parsed_release_date.strftime('%Y-%m-%d')
      magnet['season'] = int(parsed_release_date.strftime('%Y'))
      magnet['episode'] = int(parsed_release_date.strftime('%j')) # day of the year

    # get corresponding magnet link
    magnet_link_el = magnet_el.getparent().getparent().find('.//a[@class="magnet"]')

    if magnet_link_el is None:
      print('Magnet link not found')
      pprint(magnet_el.text)
      continue # ignore episodes with no magnet link
      exit()

    # match magnet hash and filename
    magnet_url_match = re.match(r'magnet:\?xt=urn:btih:(?P<info_hash>[0-9a-f]{40})', magnet_link_el.attrib['href'], re.IGNORECASE)
    if magnet_url_match is None:
      print('Magnet link parse failed')
      pprint(magnet_link_el.attrib['href'])
      exit()

    magnet['info_hash'] = magnet_url_match.group('info_hash').lower()

    #pprint(magnet)

    magnets.append(magnet)

  return magnets




duplicates_in_a_row = 0

for page_number in range(start_page_number,last_page_number):
  print('Scraping page ' + str(page_number))
  magnets = get_index_page_episode_magnets(page_number)
  if len(magnets) == 0:
    print('No magnets found, assuming last page reached.')
    break

  for magnet in magnets:
    magnet_added = mediatorrentdb.add_magnet(magnet)
    if not magnet_added:
      print('Torrent already exists:', magnet['info_hash'])
      duplicates_in_a_row += 1
      if duplicates_in_a_row > 2:
          break

  if duplicates_in_a_row > 2:
    print('Saw', duplicates_in_a_row, 'existing torrent in a row, assuming caught up to the last scrape')
    break
