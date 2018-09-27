import bonobo
import flyby
import os
import requests
import json
import logging
import math
import time

ACTIVITY_ID = 1861083433

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
    time.sleep(1)
    p = retrieve_power(id)
    if p:
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
    trunk['print power'] = graph.add_chain(bonobo.PrettyPrinter(),
                                           _input=trunk['power'].output)
    trunk['power ids'] = graph.add_chain(extract_power_ids,
                                           _input=trunk['power'].output)
    trunk['json power'] = graph.add_chain(bonobo.JsonWriter('flyby-power.json'),
                                             _input=trunk['power'].output)
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
        return math.fsum(self.time_in_zone)


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    verify_strava_cookies()
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
