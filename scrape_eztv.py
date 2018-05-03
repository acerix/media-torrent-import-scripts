#!/usr/bin/env python3

import json
from urllib.parse import quote_plus
import requests
from lxml import etree
import re
import datetime

# config
import config

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

    # match episode code (eg. S02E13) or episode date (eg. 2017 12 31)
    episode_code_match = re.match(r'(?P<title>.*)(S(?P<season>\d\d)E(?P<episode>\d\d)|(?P<release_date>(20|19)\d\d [01]\d [0123]\d))(?P<quality>.*)', magnet_el.text)
    if not episode_code_match:
      print('No episode code or release date found.')
      exit()

    magnet['title'] = episode_code_match.group('title').strip()
    magnet['season_number'] = episode_code_match.group('season')
    magnet['episode_number'] = episode_code_match.group('episode')
    release_date = episode_code_match.group('release_date')

    if len(magnet['title']) < 4:
      print('Title too short')
      exit()

    # put date in ISO format, ensuring it is valid
    if release_date:
      parsed_release_date = datetime.datetime.strptime(release_date, '%Y %m %d')
      magnet['release_date'] = parsed_release_date.strftime('%Y-%m-%d')


    # @todo get hash!

    # @todo detect quality level from string
    #quality_info = episode_code_match.group('quality').strip()

    magnets.append(magnet)

  return magnets






for page_number in range(start_page_number,last_page_number):
  print(page_number)
  magnets = get_index_page_episode_magnets(page_number)
  if len(magnets) == 0:
    print('No magnets found, assuming last page reached.')
    break
  print('wood loop')
  exit()

















print('done yo')
exit()



# find/add torrent
def get_torrent_id(import_row):
  db_cursor.execute("""
SELECT
  id
FROM
  torrent
WHERE
  hash = :info_hash
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  torrent
(
  hash
)
VALUES
(
  :info_hash
)
""", import_row)
    return db_cursor.lastrowid



# find the movie in the database from the row data otherwise add it, then return the database row id
def get_movie_id(import_row):
  # find existing by id
  db_cursor.execute("""
SELECT
  id
FROM
  movie
WHERE
  id = :id
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  movie
(
  id,
  title,
  synopsis,
  theater_release_year,
  theater_release_date,
  online_release_date,
  minutes_long,
  content_rating_id
)
VALUES
(
  :id,
  :title,
  :synopsis,
  :theater_release_year,
  :theater_release_date,
  :online_release_date,
  :minutes_long,
  (SELECT id FROM content_rating WHERE name = :content_rating)
)
""", import_row)
    movie_id = db_cursor.lastrowid

    for genre in import_row['genres'].split(','):
      if len(genre):
        db_cursor.execute("""
INSERT INTO
  movie_genre
(
  movie_id,
  genre_id
)
VALUES
(
  :movie_id,
  (SELECT id FROM genre WHERE name = :genre)
)
""", {
  'movie_id': movie_id,
  'genre': str(genre)
})

    return movie_id


# find/add release
def get_movie_release_id(import_row):
  db_cursor.execute("""
SELECT
  id
FROM
  movie_release
WHERE
  movie_id = :movie_id
AND
  release_format_id = (SELECT id FROM release_format WHERE name = :release_format)
AND
  video_quality_id = (SELECT id FROM video_quality WHERE name = :video_quality)
AND
  name = :release_name
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  movie_release
(
  movie_id,
  release_format_id,
  video_quality_id,
  name,
  created
)
VALUES
(
  :movie_id,
  (SELECT id FROM release_format WHERE name = :release_format),
  (SELECT id FROM video_quality WHERE name = :video_quality),
  :release_name,
  strftime('%s', 'now')
)
""", import_row)
    return db_cursor.lastrowid

# find/add release video
def get_movie_release_video_id(import_row):
  db_cursor.execute("""
SELECT
  id
FROM
  movie_release_video
WHERE
  movie_release_id = :movie_release_id
AND
  torrent_id = :torrent_id
AND
  filename = :video_filename
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  movie_release_video
(
  movie_release_id,
  torrent_id,
  filename
)
VALUES
(
  :movie_release_id,
  :torrent_id,
  :video_filename
)
""", import_row)
    return db_cursor.lastrowid





# find/add series
def get_series_id(import_row):
  # find existing by id
  db_cursor.execute("""
SELECT
  id
FROM
  series
WHERE
  id = :id
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  series
(
  id,
  title,
  synopsis,
  content_rating_id
)
VALUES
(
  :id,
  :series_title,
  :series_synopsis,
  (SELECT id FROM content_rating WHERE name = :content_rating)
)
""", import_row)
    series_id = db_cursor.lastrowid

    for genre in import_row['genres'].split(','):
      if len(genre):
        db_cursor.execute("""
INSERT INTO
  series_genre
(
  series_id,
  genre_id
)
VALUES
(
  :series_id,
  (SELECT id FROM genre WHERE name = :genre)
)
""", {
  'series_id': series_id,
  'genre': genre
})

    return series_id

# find/add series season
def get_series_season_id(import_row):
  # find existing by id
  db_cursor.execute("""
SELECT
  id
FROM
  series_season
WHERE
  series_id = :series_id
AND
  number = :season
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  series_season
(
  series_id,
  number
)
VALUES
(
  :series_id,
  :season
)
""", import_row)
    return db_cursor.lastrowid

# find/add series season episode
def get_series_season_episode_id(import_row):
  # find existing by id
  db_cursor.execute("""
SELECT
  id
FROM
  series_season_episode
WHERE
  series_season_id = :series_season_id
AND
  number = :episode
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  series_season_episode
(
  series_season_id,
  number,
  title,
  synopsis,
  minutes_long,
  release_date
)
VALUES
(
  :series_season_id,
  :episode,
  :title,
  :synopsis,
  :minutes_long,
  :release_date
)
""", import_row)
    return db_cursor.lastrowid

# find/add episode release
def get_series_season_episode_release_id(import_row):
  db_cursor.execute("""
SELECT
  id
FROM
  series_season_episode_release
WHERE
  episode_id = :series_season_episode_id
AND
  release_format_id = (SELECT id FROM release_format WHERE name = :release_format)
AND
  video_quality_id = (SELECT id FROM video_quality WHERE name = :video_quality)
AND
  name = :release_name
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  series_season_episode_release
(
  episode_id,
  release_format_id,
  video_quality_id,
  name,
  created
)
VALUES
(
  :series_season_episode_id,
  (SELECT id FROM release_format WHERE name = :release_format),
  (SELECT id FROM video_quality WHERE name = :video_quality),
  :release_name,
  strftime('%s', 'now')
)
""", import_row)
    return db_cursor.lastrowid

# find/add episode release video
def get_series_season_episode_release_video_id(import_row):
  db_cursor.execute("""
SELECT
  id
FROM
  series_season_episode_release_video
WHERE
  episode_release_id = :series_season_episode_release_id
AND
  torrent_id = :torrent_id
AND
  filename = :video_filename
""", import_row)
  db_row = db_cursor.fetchone()

  if db_row:
    return db_row['id']
  else:
    db_cursor.execute("""
INSERT INTO
  series_season_episode_release_video
(
  episode_release_id,
  torrent_id,
  filename
)
VALUES
(
  :series_season_episode_release_id,
  :torrent_id,
  :video_filename
)
""", import_row)
    return db_cursor.lastrowid




# open csv file and process each row
with open(import_csv_filename) as csv_file:
  csv_reader = csv.DictReader(csv_file)
  for row in csv_reader:


    # import torrent
    row['torrent_id'] = get_torrent_id(row)

    # import show
    if 'episode' in row and len(row['episode']):
      row['series_id'] = get_series_id(row)
      row['series_season_id'] = get_series_season_id(row)
      row['series_season_episode_id'] = get_series_season_episode_id(row)
      row['series_season_episode_release_id'] = get_series_season_episode_release_id(row)
      row['series_season_episode_release_video_id'] = get_series_season_episode_release_video_id(row)

    # import movie
    else:
      row['movie_id'] = get_movie_id(row)
      row['movie_release_id'] = get_movie_release_id(row)
      row['release_video_id'] = get_movie_release_video_id(row)


# close db
db.commit()
db_cursor.close()

