

from typing import Dict, List
import os
import time

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from datetime import datetime
import pyproj
from shapely.ops import transform
from shapely import wkt


try:
    from .settings import APP_PORT, VERSION, VERSION_DATE
    from .settings import POSTGRESQL_DB_PORT, POSTGRESQL_DB_NAME, POSTGRESQL_DB_USER, POSTGRESQL_DB_HOST, POSTGRESQL_DB_PASSWORD
    from .settings import POINT_LAYER, LINE_LAYER
except Exception as ex:
    print(ex)
    from settings import APP_PORT, VERSION, VERSION_DATE
    from settings import POSTGRESQL_DB_PORT, POSTGRESQL_DB_NAME, POSTGRESQL_DB_USER, POSTGRESQL_DB_HOST, POSTGRESQL_DB_PASSWORD
    from settings import POINT_LAYER, LINE_LAYER


import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

app = Flask(__name__)
app.config.from_mapping(
    SQLALCHEMY_DATABASE_URI=os.getenv('GIS_DB_URI'),
    SQLALCHEMY_TRACK_MODIFICATIONS=False
)

db = SQLAlchemy(app)

API_VERSION = 'v1'

'''
SELECT * 
FROM aquagis_line a
WHERE ST_Within(a.way, st_geomfromtext('POLYGON ((2555772.3558608964085579 5242951.9218813907355070, 2556422.2947078933939338 5242906.5328255081549287, 2556584.6653171642683446 5242505.2480738135054708, 2556417.4300461458042264 5242080.0641137417405844, 2555849.8230945393443108 5242020.3416704852133989, 2555481.7674621297046542 5242158.8838137919083238, 2555436.3602418354712427 5242459.8611671319231391, 2555772.3558608964085579 5242951.9218813907355070))', 3857));
'''


def get_points_within_polygon(polygon: str, feature_crs='3857') -> str:
    sql = f'''
    with points as (
    SELECT count(1) as objects_count, aq_type as aquagis_type
    FROM aquagis_point a
    WHERE ST_Within(a.way, st_geomfromtext('{polygon}', {feature_crs}))
    GROUP BY a.aq_type
    )
    SELECT *
    FROM points
    ;
    '''
    return sql


def get_lines_intersecting_polygon(polygon: str, res_crs='32635', feature_crs='3857') -> str:
    sql = f'''
    with lines as (
    SELECT count(1) as objects_count, aq_type as aquagis_type, sum(ST_LENGTH(ST_TRANSFORM(way, {res_crs}))) as length
    FROM aquagis_line a
    WHERE ST_Intersects(a.way, st_geomfromtext('{polygon}', {feature_crs}))
    GROUP BY a.aq_type
    )
    SELECT *
    FROM lines
    ;
    '''
    return sql


def get_lines_whitin_polygon(polygon: str, res_crs='32635', feature_crs='3857') -> str:
    sql = f'''
    with lines as (
    SELECT count(1) as objects_count, aq_type as aquagis_type, sum(ST_LENGTH(ST_TRANSFORM(way, {res_crs}))) as length
    FROM aquagis_line a
    WHERE ST_Within(a.way, st_geomfromtext('{polygon}', {feature_crs}))
    GROUP BY a.aq_type
    )
    SELECT *
    FROM lines
    ;
    '''
    return sql


@app.route('/', methods=['GET'])
def aq_service():
    return {
        'type': 'AquaGIS API Functions',
        'name': 'AquaGIS GIS Functions',
        'version': VERSION,
        'version_date': VERSION_DATE,
        'current_time': f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}",
        'sys_cpu_count': f'{os.cpu_count()}',
        'sys_os_uname': f'{os.uname()}'
    }


@app.route(f"/{API_VERSION}/check", methods=['GET'])
def get_check():
    return {
        'service': 'AquaGIS GeoSpatial Functions',
        'author': 'Todor Lubenov'
    }


@app.route('/healthcheck')
def aq_health():
    return {
        'service': 'working'
    }


@app.route('/get_dma_resources', methods=['POST'])
def get_dma():
    if request.method == 'POST':
        req = None
        errors = []
        # srs_polygon = None
        # dest_polygon = None
        res_arr = []
        res_intersect_line_arr = []
        res_within_line_arr = []

        s = time.perf_counter()

        try:
            req = request.get_json(force=True)
            r_geom_type = req.get('geom_type')
            r_coords = req.get('coordinates')
            source_crs = req.get('crs', 4326)
            res_crs = req.get('result_crs', 32635)

            if 'polygon' not in r_geom_type.lower():
                errors.append({'geom_type': r_geom_type,
                               'msg': f'{r_geom_type} is not suitable for DMA zone extraction'
                               })
            else:
                arr = []
                for e in r_coords[0]:
                    # arr.append(' '.join(map(str, e)))
                    arr.append(f'{e[-1]} {e[0]}')
                srs_polygon = f"{r_geom_type.upper()}(({','.join(arr)}))"
                g = wkt.loads(srs_polygon)
                dest_crs = '3857'
                project = pyproj.Transformer.from_proj(
                    pyproj.Proj(f'epsg:{source_crs}'),  # source coordinate system
                    pyproj.Proj(f'epsg:{dest_crs}'))  # destination coordinate system
                dest_polygon = transform(project.transform, g)
                dest_polygon = wkt.dumps(dest_polygon)

                sql = get_points_within_polygon(dest_polygon, dest_crs)
                r = db.engine.execute(text(sql))
                res = r.mappings().all()
                for e in res:
                    el = dict(e)
                    res_arr.append(el)

                sql_intersect_line = get_lines_intersecting_polygon(dest_polygon, res_crs, dest_crs)
                r = db.engine.execute(text(sql_intersect_line))
                res_intersect_line = r.mappings().all()
                for e in res_intersect_line:
                    el = dict(e)
                    res_intersect_line_arr.append(el)

                sql_within_line = get_lines_whitin_polygon(dest_polygon, res_crs, dest_crs)
                r = db.engine.execute(text(sql_within_line))
                res_within_line = r.mappings().all()
                for e in res_within_line:
                    el = dict(e)
                    res_within_line_arr.append(el)

        except Exception as ex:
            errors.append({'error': str(ex)})
        finally:
            t = float(time.perf_counter() - s) * 1000.00
            obj = {
                'request': req,
                'response': {
                    # 'source_polygon': srs_polygon,
                    # 'dest_polygon': dest_polygon,
                    'aquagis_points': res_arr,
                    'aquagis_line_intersection': res_intersect_line_arr,
                    'aquagis_line_within': res_within_line_arr
                },
                'exec_time_ms': f'{t:.3f}',
            }
            if len(errors) > 0:
                obj['errors'] = errors
            return jsonify(obj)


# main
def run():
    """Serve the REST service"""
    app.run(host='0.0.0.0', port=3000, debug=False)


if __name__ == '__main__':
    print("building ...")
    if os.getenv('DEBUG') and os.getenv('DEBUG').lower() == 'true':
        run()
