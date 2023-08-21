class Error(Exception):
    pass


class LVM2Error(Error):
    pass


class DiskError(Error):
    pass


class RAIDError(Error):
    pass


class DMError(Error):
    pass


class MDError(Error):
    pass


class DDFError(Error):
    pass
