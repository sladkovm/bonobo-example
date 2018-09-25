import bonobo
from bonobo.config import use_context_processor, use
import requests


FABLABS_API_URL = 'https://public-us.opendatasoft.com/api/records/1.0/search/?dataset=fablabs&rows=1000'


@use('http')
def extract_fablabs(http):
    yield from http.get(FABLABS_API_URL).json().get('records')


def with_opened_file(self, context):
    with context.get_service('fs').open('ouput.txt', 'w+') as f:
        yield f


@use_context_processor(with_opened_file)
def write_repr_to_file(f, *row):
    f.write(repr(row) + "\n")


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        extract_fablabs,
        bonobo.Limit(10),
        bonobo.CsvWriter('output.csv'),
        _name='extract'
    )
    # graph.add_chain(bonobo.CsvWriter('output.csv'), _input='extract')
    graph.add_chain(bonobo.PrettyPrinter(), _input=1)

    return graph


def get_services(use_cache=False, **options):
    if use_cache:
        from requests_cache import CachedSession
        http = CachedSession('http.cache')
    else:
        import requests
        http = requests.Session()
        http.headers = {'User-Agent': 'Monkeys!'}
    return {
        'http': http
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    parser.add_argument('--use-cache', action='store_true', default=False)
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
