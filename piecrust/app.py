import re
import json
import os.path
import codecs
import hashlib
import logging
import collections
import yaml
from werkzeug.utils import cached_property
from piecrust import (APP_VERSION,
        CACHE_DIR, TEMPLATES_DIR, ASSETS_DIR,
        PLUGINS_DIR, THEME_DIR,
        CONFIG_PATH, THEME_CONFIG_PATH,
        DEFAULT_FORMAT, DEFAULT_TEMPLATE_ENGINE, DEFAULT_POSTS_FS,
        DEFAULT_DATE_FORMAT, DEFAULT_PLUGIN_SOURCE, DEFAULT_THEME_SOURCE)
from piecrust.cache import ExtensibleCache, NullCache, NullExtensibleCache
from piecrust.plugins.base import PluginLoader
from piecrust.environment import StandardEnvironment
from piecrust.configuration import (Configuration, ConfigurationError,
        ConfigurationLoader, merge_dicts)
from piecrust.routing import Route
from piecrust.sources.base import REALM_USER, REALM_THEME
from piecrust.taxonomies import Taxonomy


logger = logging.getLogger(__name__)


CACHE_VERSION = 14


class VariantNotFoundError(Exception):
    def __init__(self, variant_path, message=None):
        super(VariantNotFoundError, self).__init__(
                message or ("No such configuration variant: %s" % variant_path))


class PieCrustConfiguration(Configuration):
    def __init__(self, paths=None, cache=None, values=None, validate=True,
                 accept_uncached=True):
        super(PieCrustConfiguration, self).__init__(values, validate)
        self.paths = paths
        self.cache = cache or NullCache()
        self.accept_uncached = accept_uncached
        self.fixups = []

    def applyVariant(self, variant_path, raise_if_not_found=True):
        variant = self.get(variant_path)
        if variant is None:
            if raise_if_not_found:
                raise VariantNotFoundError(variant_path)
            return
        if not isinstance(variant, dict):
            raise VariantNotFoundError(variant_path,
                    "Configuration variant '%s' is not an array. "
                    "Check your configuration file." % variant_path)
        self.merge(variant)

    def _load(self):
        if self.paths is None:
            self._values = self._validateAll({})
            return

        path_times = [os.path.getmtime(p) for p in self.paths]
        cache_key = hashlib.md5(("version=%s&cache=%d" % (
                APP_VERSION, CACHE_VERSION)).encode('utf8')).hexdigest()

        if self.cache.isValid('config.json', path_times):
            logger.debug("Loading configuration from cache...")
            config_text = self.cache.read('config.json')
            self._values = json.loads(config_text,
                    object_pairs_hook=collections.OrderedDict)

            actual_cache_key = self._values.get('__cache_key')
            if actual_cache_key == cache_key:
                self._values['__cache_valid'] = True
                return
            logger.debug("Outdated cache key '%s' (expected '%s')." % (
                    actual_cache_key, cache_key))

        if not self.accept_uncached:
            raise Exception("Expected cached configuration.")

        values = {}
        logger.debug("Loading configuration from: %s" % self.paths)
        for i, p in enumerate(self.paths):
            with codecs.open(p, 'r', 'utf-8') as fp:
                loaded_values = yaml.load(fp.read(),
                        Loader=ConfigurationLoader)
            if loaded_values is None:
                loaded_values = {}
            for fixup in self.fixups:
                fixup(i, loaded_values)
            merge_dicts(values, loaded_values)

        for fixup in self.fixups:
            fixup(len(self.paths), values)

        self._values = self._validateAll(values)

        logger.debug("Caching configuration...")
        self._values['__cache_key'] = cache_key
        config_text = json.dumps(self._values)
        self.cache.write('config.json', config_text)
        self._values['__cache_valid'] = False

    def _validateAll(self, values):
        # Put all the defaults in the `site` section.
        default_sitec = collections.OrderedDict({
                'title': "Untitled PieCrust website",
                'root': '/',
                'default_format': DEFAULT_FORMAT,
                'default_template_engine': DEFAULT_TEMPLATE_ENGINE,
                'enable_gzip': True,
                'pretty_urls': False,
                'slugify': 'transliterate|lowercase',
                'timezone': False,
                'locale': False,
                'date_format': DEFAULT_DATE_FORMAT,
                'auto_formats': collections.OrderedDict([
                    ('html', ''),
                    ('md', 'markdown'),
                    ('textile', 'textile')]),
                'default_auto_format': 'md',
                'pagination_suffix': '/%num%',
                'plugins_sources': [DEFAULT_PLUGIN_SOURCE],
                'themes_sources': [DEFAULT_THEME_SOURCE],
                'cache_time': 28800,
                'display_errors': True,
                'enable_debug_info': True,
                'use_default_content': True
                })
        sitec = values.get('site')
        if sitec is None:
            sitec = collections.OrderedDict()
        for key, val in default_sitec.items():
            sitec.setdefault(key, val)
        values['site'] = sitec

        # Add a section for our cached information.
        cachec = collections.OrderedDict()
        values['__cache'] = cachec

        # Cache auto-format regexes.
        if not isinstance(sitec['auto_formats'], dict):
            raise ConfigurationError("The 'site/auto_formats' setting must be a dictionary.")
        html_auto_format = sitec['auto_formats']
        if not html_auto_format:
            sitec['auto_formats']['html'] = sitec['default_format']
        cachec['auto_formats_re'] = r"\.(%s)$" % (
                '|'.join(
                        [re.escape(i) for i in list(sitec['auto_formats'].keys())]))
        if sitec['default_auto_format'] not in sitec['auto_formats']:
            raise ConfigurationError("Default auto-format '%s' is not declared." % sitec['default_auto_format'])

        # Cache pagination suffix regex.
        pgn_suffix = re.escape(sitec['pagination_suffix'])
        pgn_suffix = pgn_suffix.replace("\\%num\\%", "(?P<num>\\d+)") + '$'
        cachec['pagination_suffix_re'] = pgn_suffix

        # Make sure plugins and theme sources are lists.
        if not isinstance(sitec['plugins_sources'], list):
            sitec['plugins_sources'] = [sitec['plugins_sources']]
        if not isinstance(sitec['themes_sources'], list):
            sitec['themes_sources'] = [sitec['themes_sources']]

        # Setup values for posts/items.
        ipp = sitec.get('posts_per_page')
        if ipp is not None:
            sitec.setdefault('items_per_page', ipp)
        pf = sitec.get('posts_filters')
        if pf is not None:
            sitec.setdefault('items_filters', pf)

        # Figure out if we need to validate sources/routes, or auto-generate
        # them from simple blog settings.
        orig_sources = sitec.get('sources')
        orig_routes = sitec.get('routes')
        orig_taxonomies = sitec.get('taxonomies')
        use_default_content = sitec.get('use_default_content')
        if (orig_sources is None or orig_routes is None or
                orig_taxonomies is None or use_default_content):

            # Setup defaults for various settings.
            posts_fs = sitec.setdefault('posts_fs', DEFAULT_POSTS_FS)
            blogsc = sitec.setdefault('blogs', ['posts'])

            g_post_url = sitec.get('post_url', '%year%/%month%/%day%/%slug%')
            g_tag_url = sitec.get('tag_url', 'tag/%tag%')
            g_category_url = sitec.get('category_url', '%category%')
            g_posts_per_page = sitec.get('items_per_page', 5)
            g_posts_filters = sitec.get('items_filters')
            g_date_format = sitec.get('date_format', DEFAULT_DATE_FORMAT)

            # The normal pages and tags/categories.
            sourcesc = collections.OrderedDict()
            sourcesc['pages'] = {
                    'type': 'default',
                    'ignore_missing_dir': True,
                    'data_endpoint': 'site.pages',
                    'item_name': 'page'}
            sitec['sources'] = sourcesc

            routesc = []
            routesc.append({
                    'url': '/%path:path%',
                    'source': 'pages',
                    'func': 'pcurl(path)'})
            sitec['routes'] = routesc

            taxonomiesc = collections.OrderedDict()
            taxonomiesc['tags'] = {
                    'multiple': True,
                    'term': 'tag'}
            taxonomiesc['categories'] = {
                    'term': 'category'}
            sitec['taxonomies'] = taxonomiesc

            # Setup sources/routes/taxonomies for each blog.
            for blog_name in blogsc:
                blogc = values.get(blog_name, {})
                url_prefix = blog_name + '/'
                endpoint = 'posts/%s' % blog_name
                item_name = '%s-post' % blog_name
                items_per_page = blogc.get('posts_per_page', g_posts_per_page)
                items_filters = blogc.get('posts_filters', g_posts_filters)
                date_format = blogc.get('date_format', g_date_format)
                if len(blogsc) == 1:
                    url_prefix = ''
                    endpoint = 'posts'
                    item_name = 'post'
                sourcesc[blog_name] = {
                        'type': 'posts/%s' % posts_fs,
                        'fs_endpoint': endpoint,
                        'ignore_missing_dir': True,
                        'data_type': 'blog',
                        'item_name': item_name,
                        'items_per_page': items_per_page,
                        'items_filters': items_filters,
                        'date_format': date_format,
                        'default_layout': 'post'}
                tax_page_prefix = ''
                if len(blogsc) > 1:
                    tax_page_prefix = blog_name + '/'
                sourcesc[blog_name]['taxonomy_pages'] = {
                        'tags': ('pages:%s_tag.%%ext%%;'
                                 'theme_pages:_tag.%%ext%%' %
                                 tax_page_prefix),
                        'categories': ('pages:%s_category.%%ext%%;'
                                       'theme_pages:_category.%%ext%%' %
                                       tax_page_prefix)}

                post_url = blogc.get('post_url', url_prefix + g_post_url)
                post_url = '/' + post_url.lstrip('/')
                tag_url = blogc.get('tag_url', url_prefix + g_tag_url)
                tag_url = '/' + tag_url.lstrip('/')
                category_url = blogc.get('category_url', url_prefix + g_category_url)
                category_url = '/' + category_url.lstrip('/')
                routesc.append({'url': post_url, 'source': blog_name,
                        'func': 'pcposturl(year,month,day,slug)'})
                routesc.append({'url': tag_url, 'source': blog_name,
                        'taxonomy': 'tags',
                        'func': 'pctagurl(tag)'})
                routesc.append({'url': category_url, 'source': blog_name,
                        'taxonomy': 'categories',
                        'func': 'pccaturl(category)'})

            # If the user defined some additional sources/routes/taxonomies,
            # add them to the default ones. For routes, the order matters,
            # though, so we make sure to add the user routes at the front
            # of the list so they're evaluated first.
            if orig_sources:
                sourcesc.update(orig_sources)
            sitec['sources'] = sourcesc
            if orig_routes:
                routesc = orig_routes + routesc
            sitec['routes'] = routesc
            if orig_taxonomies:
                taxonomiesc.update(orig_taxonomies)
            sitec['taxonomies'] = taxonomiesc

        # Validate sources/routes.
        sourcesc = sitec.get('sources')
        routesc = sitec.get('routes')
        if not sourcesc:
            raise ConfigurationError("There are no sources defined.")
        if not routesc:
            raise ConfigurationError("There are no routes defined.")
        if not isinstance(sourcesc, dict):
            raise ConfigurationError("The 'site/sources' setting must be a dictionary.")
        if not isinstance(routesc, list):
            raise ConfigurationError("The 'site/routes' setting must be a list.")

        # Add the theme page source if no sources were defined in the theme
        # configuration itself.
        has_any_theme_source = False
        for sn, sc in sourcesc.items():
            if sc.get('realm') == REALM_THEME:
                has_any_theme_source = True
                break
        if not has_any_theme_source:
            sitec['sources']['theme_pages'] = {
                    'theme_source': True,
                    'fs_endpoint': 'pages',
                    'data_endpoint': 'site/pages',
                    'item_name': 'page',
                    'realm': REALM_THEME}
            sitec['routes'].append({
                    'url': '/%path:path%',
                    'source': 'theme_pages',
                    'func': 'pcurl(path)'})

        # Sources have the `default` scanner by default, duh. Also, a bunch
        # of other default values for other configuration stuff.
        for sn, sc in sourcesc.items():
            if not isinstance(sc, dict):
                raise ConfigurationError("All sources in 'site/sources' must be dictionaries.")
            sc.setdefault('type', 'default')
            sc.setdefault('fs_endpoint', sn)
            sc.setdefault('ignore_missing_dir', False)
            sc.setdefault('data_endpoint', sn)
            sc.setdefault('data_type', 'iterator')
            sc.setdefault('item_name', sn)
            sc.setdefault('items_per_page', 5)
            sc.setdefault('date_format', DEFAULT_DATE_FORMAT)
            sc.setdefault('realm', REALM_USER)

        # Check routes are referencing correct routes, have default
        # values, etc.
        for rc in routesc:
            if not isinstance(rc, dict):
                raise ConfigurationError("All routes in 'site/routes' must be dictionaries.")
            rc_url = rc.get('url')
            if not rc_url:
                raise ConfigurationError("All routes in 'site/routes' must have an 'url'.")
            if rc_url[0] != '/':
                raise ConfigurationError("Route URLs must start with '/'.")
            if rc.get('source') is None:
                raise ConfigurationError("Routes must specify a source.")
            if rc['source'] not in list(sourcesc.keys()):
                raise ConfigurationError("Route is referencing unknown source: %s" %
                        rc['source'])
            rc.setdefault('taxonomy', None)
            rc.setdefault('page_suffix', '/%num%')

        # Validate taxonomies.
        sitec.setdefault('taxonomies', {})
        taxonomiesc = sitec.get('taxonomies')
        for tn, tc in taxonomiesc.items():
            tc.setdefault('multiple', False)
            tc.setdefault('term', tn)
            tc.setdefault('page', '_%s.%%ext%%' % tc['term'])

        # Validate endpoints, and make sure the theme has a default source.
        reserved_endpoints = set(['piecrust', 'site', 'page', 'route',
                                  'assets', 'pagination', 'siblings',
                                  'family'])
        for name, src in sitec['sources'].items():
            endpoint = src['data_endpoint']
            if endpoint in reserved_endpoints:
                raise ConfigurationError(
                        "Source '%s' is using a reserved endpoint name: %s" %
                        (name, endpoint))


        # Done validating!
        return values


class PieCrust(object):
    def __init__(self, root_dir, cache=True, debug=False, theme_site=False,
                 env=None):
        self.root_dir = root_dir
        self.debug = debug
        self.theme_site = theme_site
        self.plugin_loader = PluginLoader(self)

        if cache:
            self.cache = ExtensibleCache(self.cache_dir)
        else:
            self.cache = NullExtensibleCache()

        self.env = env
        if self.env is None:
            self.env = StandardEnvironment()
        self.env.initialize(self)

    @cached_property
    def config(self):
        logger.debug("Creating site configuration...")
        paths = []
        if self.theme_dir:
            paths.append(os.path.join(self.theme_dir, THEME_CONFIG_PATH))
        paths.append(os.path.join(self.root_dir, CONFIG_PATH))

        config_cache = self.cache.getCache('app')
        config = PieCrustConfiguration(
                paths, config_cache,
                accept_uncached=self.env.accept_uncached_data)
        if self.theme_dir:
            # We'll need to patch the templates directories to be relative
            # to the site's root, and not the theme root.
            def _fixupThemeTemplatesDir(index, config):
                if index != 0:
                    return
                sitec = config.get('site')
                if sitec is None:
                    return
                tplc = sitec.get('templates_dirs')
                if tplc is None:
                    return
                if isinstance(tplc, str):
                    tplc = [tplc]
                sitec['templates_dirs'] = list(filter(tplc,
                        lambda p: os.path.join(self.theme_dir, p)))
            config.fixups.append(_fixupThemeTemplatesDir)

            # We'll also need to flag all page sources as coming from
            # the theme.
            def _fixupThemeSources(index, config):
                if index != 0:
                    return
                sitec = config.get('site')
                if sitec is None:
                    sitec = {}
                    config['site'] = sitec
                srcc = sitec.get('sources')
                if srcc is not None:
                    for sn, sc in srcc.items():
                        sc['realm'] = REALM_THEME
            config.fixups.append(_fixupThemeSources)

        return config

    @cached_property
    def assets_dirs(self):
        assets_dirs = self._get_configurable_dirs(ASSETS_DIR,
                'site/assets_dirs')

        # Also add the theme directory, if any.
        if self.theme_dir:
            default_theme_dir = os.path.join(self.theme_dir, ASSETS_DIR)
            if os.path.isdir(default_theme_dir):
                assets_dirs.append(default_theme_dir)

        return assets_dirs

    @cached_property
    def templates_dirs(self):
        templates_dirs = self._get_configurable_dirs(TEMPLATES_DIR,
                'site/templates_dirs')

        # Also, add the theme directory, if any.
        if self.theme_dir:
            default_theme_dir = os.path.join(self.theme_dir, TEMPLATES_DIR)
            if os.path.isdir(default_theme_dir):
                templates_dirs.append(default_theme_dir)

        return templates_dirs

    @cached_property
    def plugins_dirs(self):
        return self._get_configurable_dirs(PLUGINS_DIR,
                'site/plugins_dirs')

    @cached_property
    def theme_dir(self):
        td = self._get_dir(THEME_DIR)
        if td is not None:
            return td
        return os.path.join(os.path.dirname(__file__), 'resources', 'theme')

    @cached_property
    def cache_dir(self):
        return os.path.join(self.root_dir, CACHE_DIR)

    @cached_property
    def sources(self):
        defs = {}
        for cls in self.plugin_loader.getSources():
            defs[cls.SOURCE_NAME] = cls

        sources = []
        for n, s in self.config.get('site/sources').items():
            cls = defs.get(s['type'])
            if cls is None:
                raise ConfigurationError("No such page source type: %s" % s['type'])
            src = cls(self, n, s)
            sources.append(src)
        return sources

    @cached_property
    def routes(self):
        routes = []
        for r in self.config.get('site/routes'):
            rte = Route(self, r)
            routes.append(rte)
        return routes

    @cached_property
    def taxonomies(self):
        taxonomies = []
        for tn, tc in self.config.get('site/taxonomies').items():
            tax = Taxonomy(self, tn, tc)
            taxonomies.append(tax)
        return taxonomies

    def getSource(self, source_name):
        for source in self.sources:
            if source.name == source_name:
                return source
        return None

    def getRoutes(self, source_name, skip_taxonomies=False):
        for route in self.routes:
            if route.source_name == source_name:
                if not skip_taxonomies or route.taxonomy is None:
                    yield route

    def getRoute(self, source_name, source_metadata):
        for route in self.getRoutes(source_name, True):
            if route.isMatch(source_metadata):
                return route
        return None

    def getTaxonomyRoute(self, tax_name, source_name):
        for route in self.routes:
            if route.taxonomy == tax_name and route.source_name == source_name:
                return route
        return None

    def getTaxonomy(self, tax_name):
        for tax in self.taxonomies:
            if tax.name == tax_name:
                return tax
        return None

    def _get_dir(self, default_rel_dir):
        abs_dir = os.path.join(self.root_dir, default_rel_dir)
        if os.path.isdir(abs_dir):
            return abs_dir
        return None

    def _get_configurable_dirs(self, default_rel_dir, conf_name):
        dirs = []

        # Add custom directories from the configuration.
        conf_dirs = self.config.get(conf_name)
        if conf_dirs is not None:
            if isinstance(conf_dirs, str):
                dirs.append(os.path.join(self.root_dir, conf_dirs))
            else:
                dirs += [os.path.join(self.root_dir, p) for p in conf_dirs]

        # Add the default directory if it exists.
        default_dir = os.path.join(self.root_dir, default_rel_dir)
        if os.path.isdir(default_dir):
            dirs.append(default_dir)

        return dirs

