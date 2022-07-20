class Error(Exception):
    """Base class for exceptions for this module.
    It is used to recognize errors specific to this module"""

    pass


class LVM2Error(Error):
    pass


class DiskError(Error):
    pass
