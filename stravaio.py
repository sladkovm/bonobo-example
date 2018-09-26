import os
import requests
import json
import logging


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

    if r.ok:

        d = json.loads(r.text)
        d.update({'activity_id': activity_id})

        return Power(d)

    else:
        logger = logging.getLogger('strava_hacks.retrieve_power')
        logger.error('Connection refused {}'.format(COOKIES))
        raise ConnectionRefusedError('Reason: {}'.format(r.reason))


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

if __name__ == '__main__':
    verify_strava_cookies()
