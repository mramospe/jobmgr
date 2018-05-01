'''
Auxiliar functions to run the tests.
'''

import atexit
import shutil


def rm_tmp( path ):
    '''
    Function to remove a directory at exit.

    :param path: path to the directory.
    :type path: str
    '''
    atexit.register(lambda: shutil.rmtree(path))


def using_directory( path ):
    '''
    Create a decorator using the given path, which will
    be removed after exiting python.
    '''
    def _wrapper( func ):
        '''
        Decorator working with the given function.
        '''
        def __wrapper( *args, **kwargs ):
            '''
            Internal wrapper to remove the directory.
            '''
            rm_tmp(path)
            return func(*args, **kwargs)

        return __wrapper

    return _wrapper
