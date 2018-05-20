
# config
import config

# database connection
db = config.db_connect()
db_cursor = db.cursor()

# imdb connection
from imdb import IMDb
imdb = IMDb()

# @dev
from pprint import pprint

# add magnet, return True if added, False if already exists
def add_magnet(import_row):
  torrent_id = get_torrent_id(import_row)
  if torrent_id == False:
    return False

  # find existing series title to skip imdb lookup
  db_cursor.execute("""
SELECT
  id
FROM
  series
WHERE
  title = :series_title
""", import_row)
  db_row = db_cursor.fetchone()


  if not db_row:
    # check alternate titles
    db_cursor.execute("""
SELECT
  id
FROM
  series_alias
WHERE
  title = :series_title
""", import_row)
    db_row = db_cursor.fetchone()


  if db_row:
    # use cached imdb data
    import_row['id'] = db_row['id']
  else:
    # print('IMDB lookup:', import_row['series_title'])
    get_imdb_data(import_row)

  # skip series that are not found on imdb
  if 'id' not in import_row:
    return -1

  # specify to fix queries..  @todo  get from IMDB?
  import_row['series_synopsis'] = None
  import_row['content_rating'] = None
  import_row['genres'] = ''

  # episode data  @todo  get from IMDB?
  import_row['title'] = None
  import_row['synopsis'] = None
  import_row['minutes_long'] = None
  import_row['release_date'] = None

  # release data
  # @todo detect from release name
  import_row['release_format'] = 'HDTV'
  import_row['video_quality'] = '720p'

  #pprint(import_row)
  import_row['series_id'] = get_series_id(import_row)
  import_row['series_season_id'] = get_series_season_id(import_row)
  import_row['series_season_episode_id'] = get_series_season_episode_id(import_row)
  import_row['series_season_episode_release_id'] = get_series_season_episode_release_id(import_row)

  db.commit()
  return True


# add imdb data to the magnet
def get_imdb_data(import_row):
  imdb_movie_results = imdb.search_movie(import_row['series_title'])

  # eg. "Forged in Fire Knife or Death" is not found because it's actually just "Forged in Fire"
  # try chopping words off the end of the title until there is a result or no title
  test_title = import_row['series_title']
  while len(imdb_movie_results) == 0 and test_title.count(' '):
    test_title = test_title.rsplit(' ', 1)[0]
    # print('Title not found on IMDB, trying:', test_title)
    imdb_movie_results = imdb.search_movie(test_title)

  if len(imdb_movie_results) == 0:
    print('Title not found:', import_row['series_title'])
    return import_row

  # assume first result is correct
  imdb_movie = imdb_movie_results[0]

  # use imdbid
  import_row['id'] = imdb_movie.movieID

  # use imdb title
  if import_row['series_title'] != imdb_movie['title']:
    db_cursor.execute("""
INSERT INTO
  series_alias
(
  series_id,
  title
)
VALUES
(
  :id,
  :series_title
)
""", import_row)
    db.commit()
    import_row['series_title'] = imdb_movie['title']


  # get details
  #imdb_infoset = imdb.get_movie_infoset()
  #imdb.update(imdb_movie, imdb_infoset)

  # pprint(import_row)

  return import_row


# add torrent and return ID or return false if it exists
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
    return False  # skip existing
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
    db.commit()
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
