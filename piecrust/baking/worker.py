import sys
import time
import queue
import logging
import colorama
from piecrust.app import PieCrust
from piecrust.baking.records import BakeRecordPageEntry
from piecrust.baking.single import PageBaker, BakingError
from piecrust.chefutil import (
        setup_logging, format_timed, log_friendly_exception)
from piecrust.main import pre_parse_chef_args
from piecrust.sources.base import PageFactory


logger = logging.getLogger(__name__)


class BakeWorkerContext(object):
    """ The initialization context for a bake worker.
        This needs to be easily picklable for serialization between processes.
    """
    def __init__(self, wid, app_root_dir, bake_out_dir, record,
                 jobs, results, commands,
                 force):
        self.wid = wid
        self.app_root_dir = app_root_dir
        self.bake_out_dir = bake_out_dir
        self.record = record
        self.jobs = jobs
        self.commands = commands
        self.results = results
        self.force = force


class BakeWorkerJob(object):
    """ A job description for a bake worker.
        This needs to be easily picklable for serialization between processes.
    """
    def __init__(self, factory, route,
                 taxonomy_name=None, taxonomy_term=None):
        self.source_name = factory.source.name
        self.fac_rel_path = factory.rel_path
        self.fac_metadata = factory.metadata
        self.route_uri_pattern = route.uri_pattern if route else None
        self.taxonomy_name = taxonomy_name
        self.taxonomy_term = taxonomy_term


class UpdateBakeRecordCommand(object):
    def __init__(self, entries):
        self.entries = entries

    def execute(self, worker):
        for e in self.entries:
            worker.ctx.record.current.addEntry(e)


def run_worker(ctx):
    argv = sys.argv
    pre_args = pre_parse_chef_args(argv)

    colorama.init()
    setup_logging(pre_args.quiet, pre_args.debug, prefix=('[%s]' % ctx.wid))

    worker = BakeWorker(ctx, pre_args.debug)
    worker.run()
    logger.debug("Done with worker %s" % ctx.wid)


class BakeWorkerException(Exception):
    pass


class BakeWorker(object):
    def __init__(self, ctx, debug=False):
        self.ctx = ctx
        self.app = PieCrust(ctx.app_root_dir, debug=debug)
        self.app.env.accept_uncached_data = False
        self.page_baker = PageBaker(self.app, ctx.bake_out_dir,
                                    record=ctx.record,
                                    force=ctx.force)

    def run(self):
        while True:
            if self._try_run_job():
                continue
            if self._try_run_command():
                continue
            time.sleep(0.1)

    def _try_run_job(self):
        try:
            i, job = self.ctx.jobs.get_nowait()
        except queue.Empty:
            return False

        try:
            self._run_job(i, job)
        except Exception as ex:
            if self.ctx.debug:
                logger.exception(ex)
            else:
                log_friendly_exception(logger, ex)

        return True

    def _run_job(self, idx, job):
        logger.debug("Running job %d: %s:%s" %
                     (idx, job.source_name, job.fac_rel_path))
        start_time = time.clock()

        source = self.app.getSource(job.source_name)
        if source is None:
            raise BakeWorkerException(
                    "Can't find source: %s" % job.source_name)
        factory = PageFactory(source, job.fac_rel_path, job.fac_metadata)

        entry = BakeRecordPageEntry(factory,
                                    job.taxonomy_name, job.taxonomy_term)
        self.page_baker.record.addEntry(entry)

        route = None
        if job.route_uri_pattern:
            route = next(
                    iter(
                        [r for r in self.app.routes
                         if r.uri_pattern == job.route_uri_pattern]),
                    None)
        if route is None:
            entry.errors.append(
                    "Can't get route for page: %s" % factory.ref_spec)
            logger.error(entry.errors[-1])
            return

        try:
            self.page_baker.bake(factory, route, entry,
                                 taxonomy_name=job.taxonomy_name,
                                 taxonomy_term=job.taxonomy_term)
        except BakingError as ex:
            logger.debug("Got baking error. Adding it to the record.")
            while ex:
                entry.errors.append(str(ex))
                ex = ex.__cause__

        if entry.was_baked_successfully:
            uri = entry.out_uris[0]
            friendly_uri = uri if uri != '' else '[main page]'
            friendly_count = ''
            if entry.num_subs > 1:
                friendly_count = ' (%d pages)' % entry.num_subs
            logger.info(format_timed(
                    start_time, '%s%s' % (friendly_uri, friendly_count)))
        elif entry.errors:
            for e in entry.errors:
                logger.error(e)

        logger.debug(
                "Done with job %d: %s:%s, pushing result to master process." %
                (idx, job.source_name, job.fac_rel_path))
        self.ctx.results.put_nowait(entry)

    def _try_run_command(self):
        try:
            cmd = self.ctx.commands.get_nowait()
        except queue.Empty:
            return False

        logger.debug("Executing command: %s" % type(cmd))
        cmd.execute(self)
        return True

