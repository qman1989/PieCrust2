import logging
import multiprocessing
from piecrust.environment import CacheDataNotReady


logger = logging.getLogger(__name__)


class BakeScheduler(object):
    def __init__(self, record):
        self.record = record
        self.jobs = multiprocessing.Queue()
        self.results = multiprocessing.Queue()

    def addJob(self, job):
        logger.debug("Queuing job '%s:%s'." % (
            job.factory.source.name, job.factory.rel_path))
        self.jobs.put_nowait(job)

    def prepareJobs(self):
        pass

