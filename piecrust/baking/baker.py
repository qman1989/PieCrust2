import time
import os.path
import queue
import shutil
import hashlib
import logging
import multiprocessing
from piecrust.baking.records import TransitionalBakeRecord
from piecrust.baking.worker import (
        BakeWorkerContext, BakeWorkerJob, run_worker)
from piecrust.chefutil import format_timed
from piecrust.sources.base import (
        PageFactory,
        REALM_NAMES, REALM_USER, REALM_THEME)


logger = logging.getLogger(__name__)


class Baker(object):
    def __init__(self, app, out_dir, force=False, portable=False,
                 no_assets=False, num_workers=None):
        assert app and out_dir
        self.app = app
        self.out_dir = out_dir
        self.force = force
        self.portable = portable
        self.no_assets = no_assets
        self.num_workers = num_workers or min(2, multiprocessing.cpu_count())

        # Remember what taxonomy pages we should skip
        # (we'll bake them repeatedly later with each taxonomy term)
        self.taxonomy_pages = []
        logger.debug("Gathering taxonomy page paths:")
        for tax in self.app.taxonomies:
            for src in self.app.sources:
                path = tax.resolvePagePath(src.name)
                if path is not None:
                    self.taxonomy_pages.append(path)
                    logger.debug(" - %s" % path)

    def bake(self):
        logger.debug("  Bake Output: %s" % self.out_dir)
        logger.debug("  Root URL: %s" % self.app.config.get('site/root'))

        # Get into bake mode.
        start_time = time.clock()
        self.app.config.set('baker/is_baking', True)
        self.app.env.base_asset_url_format = '%uri%'

        # Make sure the output directory exists.
        if not os.path.isdir(self.out_dir):
            os.makedirs(self.out_dir, 0o755)

        # Load/create the bake record.
        record = TransitionalBakeRecord()
        record_cache = self.app.cache.getCache('baker')
        record_name = (
                hashlib.md5(self.out_dir.encode('utf8')).hexdigest() +
                '.record')
        if not self.force and record_cache.has(record_name):
            t = time.clock()
            record.loadPrevious(record_cache.getCachePath(record_name))
            logger.debug(format_timed(t, 'loaded previous bake record',
                                      colored=False))

        # Figure out if we need to clean the cache because important things
        # have changed.
        self._handleCacheValidity(record)

        # Pre-create the file-system caches.
        for n in ['app', 'pages', 'renders', 'baker', 'proc']:
            self.app.cache.getCache(n)

        # Create the bake context (worker pool, job queue, etc.).
        # This will start the worker processes.
        bake_ctx = self._createBakeContext(record)
        with bake_ctx:
            # Gather all sources by realm -- we're going to bake each realm
            # separately so we can handle "overlaying" (i.e. one realm
            # overrides another realm's pages).
            sources_by_realm = {}
            for source in self.app.sources:
                srclist = sources_by_realm.setdefault(source.realm, [])
                srclist.append(source)

            # Bake the realms.
            realm_list = [REALM_USER, REALM_THEME]
            for realm in realm_list:
                srclist = sources_by_realm.get(realm)
                if srclist is not None:
                    self._bakeRealm(bake_ctx, realm, srclist)

            # Bake taxonomies.
            self._bakeTaxonomies(bake_ctx)

        # Delete files from the output.
        self._handleDeletetions(record)

        # Save the bake record.
        t = time.clock()
        record.current.bake_time = time.time()
        record.current.out_dir = self.out_dir
        record.collapseRecords()
        record.saveCurrent(record_cache.getCachePath(record_name))
        logger.debug(format_timed(t, 'saved bake record', colored=False))

        # All done.
        self.app.config.set('baker/is_baking', False)
        logger.info(format_timed(start_time, 'done baking'))

    def _handleCacheValidity(self, record):
        start_time = time.clock()

        reason = None
        if self.force:
            reason = "ordered to"
        elif not self.app.config.get('__cache_valid'):
            # The configuration file was changed, or we're running a new
            # version of the app.
            reason = "not valid anymore"
        elif (not record.previous.bake_time or
                not record.previous.hasLatestVersion()):
            # We have no valid previous bake record.
            reason = "need bake record regeneration"
        else:
            # Check if any template has changed since the last bake. Since
            # there could be some advanced conditional logic going on, we'd
            # better just force a bake from scratch if that's the case.
            max_time = 0
            for d in self.app.templates_dirs:
                for dpath, _, filenames in os.walk(d):
                    for fn in filenames:
                        full_fn = os.path.join(dpath, fn)
                        max_time = max(max_time, os.path.getmtime(full_fn))
            if max_time >= record.previous.bake_time:
                reason = "templates modified"

        if reason is not None:
            # We have to bake everything from scratch.
            for cache_name in self.app.cache.getCacheNames(
                    except_names=['app']):
                cache_dir = self.app.cache.getCacheDir(cache_name)
                if os.path.isdir(cache_dir):
                    logger.debug("Cleaning baker cache: %s" % cache_dir)
                    shutil.rmtree(cache_dir)
            self.force = True
            record.incremental_count = 0
            record.clearPrevious()
            logger.info(format_timed(start_time,
                                     "cleaned cache (reason: %s)" % reason))
        else:
            record.incremental_count += 1
            logger.debug(format_timed(start_time, "cache is assumed valid",
                                      colored=False))

    def _bakeRealm(self, bake_ctx, realm, srclist):
        # Gather all page factories from the sources and queue them
        # for the workers to pick up. Just skip taxonomy pages for now.
        logger.debug("Baking realm %s" % REALM_NAMES[realm])
        jobs = []
        for source in srclist:
            factories = source.getPageFactories()
            for fac in factories:
                if fac.path in self.taxonomy_pages:
                    logger.debug(
                            "Skipping taxonomy page: %s:%s" %
                            (source.name, fac.ref_spec))
                    continue

                route = self.app.getRoute(source.name, fac.metadata)
                jobs.append(BakeWorkerJob(fac, route))

        self._runJobs(bake_ctx, jobs)

    def _bakeTaxonomies(self, bake_ctx):
        logger.debug("Baking taxonomies")

        # Let's see all the taxonomy terms for which we must bake a
        # listing page... first, pre-populate our big map of used terms.
        buckets = {}
        tax_names = [t.name for t in self.app.taxonomies]
        source_names = [s.name for s in self.app.sources]
        for sn in source_names:
            source_taxonomies = {}
            buckets[sn] = source_taxonomies
            for tn in tax_names:
                source_taxonomies[tn] = set()

        # Now see which ones are 'dirty' based on our bake record.
        logger.debug("Gathering dirty taxonomy terms")
        for prev_entry, cur_entry in bake_ctx.record.transitions.values():
            for tax in self.app.taxonomies:
                changed_terms = None
                # Re-bake all taxonomy pages that include new or changed
                # pages.
                if (not prev_entry and cur_entry and
                        cur_entry.was_baked_successfully):
                    changed_terms = cur_entry.config.get(tax.name)
                elif (prev_entry and cur_entry and
                        cur_entry.was_baked_successfully):
                    changed_terms = []
                    prev_terms = prev_entry.config.get(tax.name)
                    cur_terms = cur_entry.config.get(tax.name)
                    if tax.is_multiple:
                        if prev_terms is not None:
                            changed_terms += prev_terms
                        if cur_terms is not None:
                            changed_terms += cur_terms
                    else:
                        if prev_terms is not None:
                            changed_terms.append(prev_terms)
                        if cur_terms is not None:
                            changed_terms.append(cur_terms)
                if changed_terms is not None:
                    if not isinstance(changed_terms, list):
                        changed_terms = [changed_terms]
                    buckets[cur_entry.source_name][tax.name] |= (
                            set(changed_terms))

        # Re-bake the combination pages for terms that are 'dirty'.
        known_combinations = set()
        logger.debug("Gathering dirty term combinations")
        for prev_entry, cur_entry in bake_ctx.record.transitions.values():
            if cur_entry:
                known_combinations |= cur_entry.used_taxonomy_terms
            elif prev_entry:
                known_combinations |= prev_entry.used_taxonomy_terms
        for sn, tn, terms in known_combinations:
            changed_terms = buckets[sn][tn]
            if not changed_terms.isdisjoint(set(terms)):
                changed_terms.add(terms)

        # Start baking those terms.
        jobs = []
        for source_name, source_taxonomies in buckets.items():
            for tax_name, terms in source_taxonomies.items():
                if len(terms) == 0:
                    continue

                logger.debug(
                        "Baking '%s' for source '%s': %s" %
                        (tax_name, source_name, terms))
                tax = self.app.getTaxonomy(tax_name)
                tax_page_ref = tax.getPageRef(source_name)
                if not tax_page_ref.exists:
                    logger.debug(
                            "No taxonomy page found at '%s', skipping." %
                            tax.page_ref)
                    continue

                tax_page_source = tax_page_ref.source
                tax_page_rel_path = tax_page_ref.rel_path
                route = self.app.getTaxonomyRoute(tax_name, source_name)
                logger.debug(
                        "Using taxonomy page: %s:%s" %
                        (tax_page_source.name, tax_page_rel_path))

                for term in terms:
                    fac = PageFactory(
                            tax_page_source, tax_page_rel_path,
                            {tax.term_name: term})
                    logger.debug(
                            "Queuing: %s [%s, %s]" %
                            (fac.ref_spec, tax_name, term))
                    jobs.append(
                            BakeWorkerJob(fac, route, tax_name, term))

        self._runJobs(bake_ctx, jobs)

    def _handleDeletetions(self, record):
        for path, reason in record.getDeletions():
            logger.debug("Removing '%s': %s" % (path, reason))
            try:
                os.remove(path)
                logger.info('[delete] %s' % path)
            except OSError:
                # Not a big deal if that file had already been removed
                # by the user.
                pass

    def _createBakeContext(self, record):
        pool = []
        jobs = multiprocessing.Queue()
        for i in range(self.num_workers):
            results = multiprocessing.Queue()
            commands = multiprocessing.Queue()
            worker_ctx = BakeWorkerContext(
                    i, self.app.root_dir, self.out_dir, record,
                    jobs, results, commands,
                    self.force)
            proc = multiprocessing.Process(
                    name=('baker-%d' % i),
                    target=run_worker,
                    args=(worker_ctx,))
            pool.append((worker_ctx, proc))

        for worker_ctx, proc in pool:
            proc.start()

        bake_ctx = BakeContext(record, pool, jobs)
        return bake_ctx

    def _runJobs(self, bake_ctx, jobs):
        job_count = len(jobs)
        logger.debug("Running %d jobs.", job_count)
        for i, j in enumerate(jobs):
            bake_ctx.jobs.put_nowait((i, j))

        try:
            result_count = 0
            while result_count < job_count:
                for c, p in bake_ctx.worker_pool:
                    try:
                        r = c.results.get_nowait()
                        result_count += 1
                        logger.debug("Got record for: %s:%s" %
                                     (r.source_name, r.rel_path))
                        bake_ctx.record.addEntry(r)
                    except queue.Empty:
                        pass
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.debug("Got KeyboardInterrupt... terminating jobs.")
            for proc in bake_ctx.worker_pool:
                proc.terminate()
            raise
        logger.debug("Done running jobs.")


class BakeContext(object):
    def __init__(self, record, worker_pool, jobs):
        self.record = record
        self.worker_pool = worker_pool
        self.jobs = jobs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug("Terminating worker processes.")
        for c, p in self.worker_pool:
            p.terminate()
        return False

