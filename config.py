
settings = {}

# copy version number to settings
from version import __version__
settings['version'] = __version__

# sqlite database to import to
settings['db_path'] = 'media-torrent-import-scripts.sqlite'

# database just initialized?
settings['first_run'] = False

# define database connection
import sqlite3

def db_connect():
  db = sqlite3.connect( settings['db_path'] )
  db.row_factory = sqlite3.Row
  return db


# test db connection
db = db_connect()

# initialize tables if none exist
table_count = db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0]

if table_count is 0:
  settings['first_run'] = True
  print('Initializing database...')

  # download media-torrent-db schema
  # @todo include in package instead of downloading directly
  import urllib.request
  print('Downloading schema...')
  with urllib.request.urlopen('https://raw.githubusercontent.com/acerix/media-torrent-db/master/schema.sql') as f:
    schema_sql = f.read().decode('utf-8')

  # add to the database
  print('Importing schema...')
  db.executescript(schema_sql)
