from logging import LoggerAdapter, getLogger

class LoggingBase:
    def _init_logging(self, parent=None):
        if not parent: self._task = self.__class__.__name__
        elif type(parent) is str: self._task = parent
        else: self._task = parent._task
        self.logger = LoggerAdapter(getLogger(self.__module__), {'task': self._task})
