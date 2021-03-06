import bonobo
import flyby
import os
import requests
import json
import logging
import math
import time
import numpy as np
from uboto3 import UBoto3


ACTIVITY_ID = 1813559518
s3 = UBoto3()

trunk = dict()


def extract_flyby(activity_id=ACTIVITY_ID):
    """Extract flyby results for the activity """
    fb = flyby.flyby(activity_id)
    yield fb


def extract_matches(fb):
    """Extract matches from the flyby results """
    for m in fb.matches:
        yield m


def filter_matches(m):
    if m['activityType'] == 'Ride':
        yield m


def extract_ids(m):
    """Extract relevant ids"""
    yield m['id']


def extract_power(id):
    """Extract power"""
    if not s3.head_object("power-{}.json".format(id), Prefix="sh-power"):
        time.sleep(1)
        p = retrieve_power(id)
        if p:
            s3.upload_json("power-{}.json".format(id), Prefix="sh-power")
    else:
        p = s3.get_object("power-{}.json".format(id), Prefix="sh-power")
    if p:
        yield p


def filter_power(p):
    is_ftp = bool(p.get('athlete_ftp'))
    is_weight = bool(p.get('athlete_weight'))
    is_moving_time = bool(p.get('moving_time'))
    print(p.get('moving_time'))
    if is_moving_time:
        is_moving_time_ok = bool(p.get('moving_time') > 2*3600)
    else:
        is_moving_time_ok = False
    if (is_ftp and is_weight and is_moving_time and is_moving_time_ok):
        yield p


def enrich_power(p):
    """Run calculations on the power data"""
    p.update({'nwpk': p['weighted_power'] / p['athlete_weight']})
    p.update({'mwpk': p['max_watts'] / p['athlete_weight']})
    p.update({'ftppk': p['athlete_ftp'] / p['athlete_weight']})
    p.update({'elapsed_time': p.get('moving_time')})
    yield p


def extract_power_ids(p):
    yield p['activity_id']


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    trunk['flyby'] = graph.add_chain(extract_flyby)
    trunk['matches'] = graph.add_chain(extract_matches,
                                       _input=trunk['flyby'].output)
    trunk['filtered matches'] = graph.add_chain(filter_matches,
                                                _input=trunk['matches'].output)
    trunk['json matches'] = graph.add_chain(bonobo.JsonWriter('matches.json'),
                                            _input=trunk['filtered matches'].output)
    trunk['ids'] = graph.add_chain(extract_ids,
                                   _input=trunk['filtered matches'].output)
    # trunk['print ids'] = graph.add_chain(bonobo.PrettyPrinter(),
    #                                      _input=trunk['ids'].output)
    trunk['json ids'] = graph.add_chain(bonobo.JsonWriter('flyby-ids.json'),
                                        _input=trunk['ids'].output)
    trunk['power'] = graph.add_chain(retrieve_power, _input=trunk['ids'].output)
    trunk['filtered power'] = graph.add_chain(filter_power, _input=trunk['power'].output)
    trunk['enrich power'] = graph.add_chain(enrich_power, _input=trunk['filtered power'].output)
    trunk['print power'] = graph.add_chain(bonobo.PrettyPrinter(),
                                           _input=trunk['enrich power'].output)
    trunk['power ids'] = graph.add_chain(extract_power_ids,
                                         _input=trunk['enrich power'].output)
    trunk['json power'] = graph.add_chain(bonobo.JsonWriter('uci-gf-wc-varese-2018.json'),
                                          _input=trunk['enrich power'].output)
    trunk['json power ids'] = graph.add_chain(bonobo.JsonWriter('flyby-power-ids.json'),
                                              _input=trunk['power ids'].output)
    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {}


def verify_strava_cookies(**kwargs):
    """Verify validity of Strava cookies

    Parameters
    ----------
    activity_id : int
    kwargs : optional
        _strava3_session : str, default=os.getenv(STRAVA3)
        _strava4_session : str, default=os.getenv(STRAVA4

    Returns
    -------
    Bool
    """

    COOKIES = dict(
        _strava4_session=kwargs.get('_strava4_session', os.getenv('STRAVA4_SESSION')),
        _strava_local_session=kwargs.get('_strava_local_session', os.getenv('STRAVA_LOCAL_SESSION')),
        _strava_cookie_banner='true'
    )

    base_url = 'https://www.strava.com/activities/'
    suffix = 'power_data'
    uri = '{}{}/{}'.format(base_url, 1861083433, suffix)

    r = requests.get(uri, cookies=COOKIES)

    logger = logging.getLogger('stravaio.verify_strava_cookies')

    if r.ok:

        logger.info('Cookies are OK {}'.format(COOKIES))
        return True

    else:

        logger.error('Connection refused {}'.format(COOKIES))
        raise ConnectionRefusedError('Reason: {}'.format(r.reason))


def retrieve_power(activity_id, **kwargs):
    """Power data associated with activity_id

    Parameters
    ----------
    activity_id : int
    kwargs : optional
        _strava3_session : str, default=os.getenv(STRAVA3)
        _strava4_session : str, default=os.getenv(STRAVA4

    Returns
    -------
    Power object
    """

    COOKIES = dict(
        _strava4_session=kwargs.get('_strava4_session', os.getenv('STRAVA4_SESSION')),
        _strava_cookie_banner='true'
    )

    base_url = 'https://www.strava.com/activities/'
    suffix = 'power_data'
    uri = '{}{}/{}'.format(base_url, activity_id, suffix)

    r = requests.get(uri, cookies=COOKIES)

    logger = logging.getLogger('retrieve_power')

    if r.ok:

        d = json.loads(r.text)
        d.update({'activity_id': activity_id})

        p = Power(d)
        logger.info('Power data are found for ID={}'.format(activity_id))

        return p.summary

    else:

        logger.error('No power data for ID={}'.format(activity_id))

        return False


class Power():

    def __init__(self, content):
        self.content = content

    def to_dict(self):
        """Export summary to dict

        Returns
        -------
        dict
        """
        rv = {
            "id": self.activity_id,
            "activity_id": self.activity_id,
            "weighted_power": self.weighted_power,
            "training_load": self.training_load,
            "max_watts": self.max_watts,
            "relative_intensity": self.relative_intensity,
            "athlete_ftp": self.athlete_ftp,
            "athlete_weight": self.athlete_weight,
            "moving_time": self.moving_time
        }
        return rv

    @property
    def summary(self):
        return self.to_dict()

    @property
    def activity_id(self):
        return self.content.get('activity_id')

    @property
    def weighted_power(self):
        return self.content.get('weighted_power', None)

    @property
    def training_load(self):
        return self.content.get('training_load', None)

    @property
    def max_watts(self):
        return self.content.get('max_watts', None)

    @property
    def relative_intensity(self):
        return self.content.get('relative_intensity', None)

    @property
    def cp_data(self):
        return self.content.get('cp_data', None)

    @property
    def time_in_bucket(self):
        return self.content.get('time_in_bucket', None)

    @property
    def time_in_zone(self):
        return self.content.get('time_in_zone', None)

    @property
    def athlete_ftp(self):
        return self.content.get('athlete_ftp', None)

    @property
    def athlete_weight(self):
        return self.content.get('athlete_weight', None)

    @property
    def moving_time(self):
        if self.content.get('athlete_ftp'):
            return math.fsum(self.time_in_zone)
        else:
            return None


def combine(data_path=None):
    """Return results json from activity and power data"""
    RESULTS_PATH = os.path.join(data_path, 'results')
    POWER_COLUMNS = ['id',
                     'athlete_ftp',
                     'athlete_weight',
                     'relative_intensity',
                     'training_load',
                     'max_watts',
                     'weighted_power',
                     'moving_time']

    df = pd.DataFrame(power)
    df = df[POWER_COLUMNS]

    df = (df[(pd.notnull(df.athlete_ftp)
              & pd.notnull(df.athlete_weight)
              & (df.athlete_weight > 0.0))]
          .sort_values('elapsed_time')
          .reset_index(drop=True))

    df['nwpk'] = df['weighted_power'] / df['athlete_weight']
    df['mwpk'] = df['max_watts'] / df['athlete_weight']
    df['ftppk'] = df['athlete_ftp'] / df['athlete_weight']

    df['nwpk'] = df['nwpk'].replace([np.inf, -np.inf], np.nan).replace([0], np.nan)
    df['mwpk'] = df['mwpk'].replace([np.inf, -np.inf], np.nan).replace([0], np.nan)
    df['ftppk'] = df['ftppk'].replace([np.inf, -np.inf], np.nan).replace([0], np.nan)

    f_name = self.slug + '.json'
    df.to_json(os.path.join(RESULTS_PATH, f_name), orient='records')


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    verify_strava_cookies()
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
