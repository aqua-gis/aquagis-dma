import os

APP_PORT = os.getenv('APP_PORT', 8088)

POSTGRESQL_DB_HOST = os.getenv('PG_DB_HOST', 'localhost')
POSTGRESQL_DB_NAME = os.getenv('PG_DB_NAME', 'aquagis_warehouse')
POSTGRESQL_DB_USER = os.getenv('PG_DB_USER')
POSTGRESQL_DB_PORT = os.getenv('PG_DB_PORT', 5432)
POSTGRESQL_DB_PASSWORD = os.getenv('PG_DB_PASS')


VERSION = '1.0'
VERSION_DATE = '2021.08.20'


POINT_LAYER = 'aquagis_line'
LINE_LAYER = 'aquagis_point'


