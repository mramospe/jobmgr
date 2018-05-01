'''
Auxiliar functions.
'''

import os

__all__ = []

__default_dir__ = 'output'


def create_dir( path = None ):
    '''
    Create a directory in the given path.

    :param path: path to the desired directory.
    :type path: str
    '''
    path = path if path is not None else __default_dir__

    try:
        os.mkdir(path)
    except OSError:
        pass

    files = os.listdir(path)

    if files:
        pid = max(map(lambda s: int(s), files)) + 1
    else:
        pid = 0

    cdir = os.path.join(path, str(pid))

    os.mkdir(cdir)

    return cdir
