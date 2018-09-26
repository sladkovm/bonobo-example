import bonobo
import flyby


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


def extract_ids(fb):
    """Extract relevant ids"""
    for id in fb.ids:
        yield id


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    trunk['flyby'] = graph.add_chain(extract_flyby)
    trunk['matches'] = graph.add_chain(extract_matches,
                                       _input=trunk['flyby'].output)
    trunk['json matches'] = graph.add_chain(bonobo.JsonWriter('matches.json'),
                                             _input=trunk['matches'].output)
    trunk['ids'] = graph.add_chain(extract_ids,
                                   _input=trunk['flyby'].output)
    trunk['print ids'] = graph.add_chain(bonobo.PrettyPrinter(),
                                         _input=trunk['ids'].output)
    trunk['json ids'] = graph.add_chain(bonobo.JsonWriter('flyby-ids.json'),
                                             _input=trunk['ids'].output)

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


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
