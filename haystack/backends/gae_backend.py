"""
A search backend using Google's search api.
https://developers.google.com/appengine/docs/python/search/
"""
from __future__ import unicode_literals
import six
import warnings
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.models.loading import get_model
from haystack.backends import BaseEngine, BaseSearchBackend, BaseSearchQuery, SearchNode, log_query
from haystack.constants import ID, DJANGO_CT, DJANGO_ID
from haystack.utils import log as logging
from haystack.utils import get_identifier
from haystack.exceptions import MissingDependency
from haystack.inputs import PythonData, Clean, Exact, Raw
from haystack.models import SearchResult

try:
    from google.appengine.api import search
except ImportError:
    raise MissingDependency("The 'gae' backend requires the Google App Engine SDK.")

DEFAULT_FIELD_MAPPING = search.TextField
FIELD_MAPPINGS = {
    'edge_ngram': search.TextField,
    'ngram': search.TextField,
    'date': search.DateField,
    'datetime': search.DateField,

    'location': search.GeoField,
    'boolean': search.AtomField,
    'float': search.NumberField,
    'long': search.NumberField,
    'integer': search.NumberField,
}

class GAESearchBackend(BaseSearchBackend):
    RESERVED_WORDS = ['NOT', 'OR', 'AND'] # TODO: should include geopoint functions?
    RESERVED_CHARACTERS = ['~', ':', '>=', '<=', '>', '<', '=', '(', ')']

    def __init__(self, connection_alias, **connection_options):
        super(GAESearchBackend, self).__init__(connection_alias, **connection_options)

        if 'INDEX_NAME' not in connection_options:
            raise ImproperlyConfigured("You must specify a 'INDEX_NAME' in your settings for connection '%s'." % connection_alias)

        self.index_name = connection_options['INDEX_NAME']
        self.setup_complete = False
        self.log = logging.getLogger('haystack')
        self.batch_size = connection_options.get('BATCH_SIZE', 200)

    def _do_setup(self):
        if not self.setup_complete:
            try:
                self.setup()
            except search.Error as e:
                if not self.silently_fail:
                    raise
                self.log.error("Error while setting up appengine: %s", e)
                return True
        return False

    def setup(self):
        """
        Defers loading until needed.
        """

        from haystack import connections
        unified_index = connections[self.connection_alias].get_unified_index()
        self.content_field_name, self.schema = self.build_schema(unified_index.all_searchfields())
        self.index = search.Index(name=self.index_name)
        self.setup_complete = True

    def build_schema(self, fields):
        content_field_name = ''
        schema_fields = {
            ID: search.AtomField,
            DJANGO_CT: search.AtomField,
            DJANGO_ID: search.AtomField,
        }

        for field_name, field_class in fields.items():
            field_mapping = FIELD_MAPPINGS.get(field_class.field_type, DEFAULT_FIELD_MAPPING)

            if field_class.document is True:
                content_field_name = field_class.index_fieldname

            schema_fields[field_class.index_fieldname] = field_mapping

        return (content_field_name, schema_fields)

    def update(self, index, iterable, commit=True):
        if self._do_setup(): return

        prepped_docs = []

        for obj in iterable:
            try:
                prepped_data = index.full_prepare(obj)
                final_data = []

                for key, value in prepped_data.items():
                    field_type = self.schema[key]
                    final_data.append(field_type(name=key, value=value))

                document = search.Document(doc_id=prepped_data[ID], fields=final_data)
                prepped_docs.append(document)
            except search.Error:
                if not self.silently_fail:
                    raise
                self.log.error("{0} while preparing object for update".format(e.__class__.__name__),
                    exc_info=True, extra={
                        "data": {
                            "index": index,
                            "object": get_identifier(obj),
                        }
                    })

        self.index.put(prepped_docs)

    def remove(self, obj_or_string, commit=True):
        doc_id = get_identifier(obj_or_string)

        if self._do_setup(): return

        try:
            self.index.delete([doc_id])
        except search.Error as e:
            if not self.silently_fail:
                raise
            self.log.error("Failed to remove document '%s': %s", doc_id, e)

    def clear(self, models=[], commit=True):
        if self._do_setup(): return

        try:
            if not models:
                while True:
                    # Get a list of documents populating only the doc_id field
                    doc_ids = [doc.doc_id for doc in self.index.get_range(ids_only=True)]
                    if not doc_ids:
                        break
                    self.index.delete(doc_ids)
            else:
                self.index.delete([get_identifier(x) for x in models])
        except search.Error as e:
            if not self.silently_fail:
                raise
            self.log.error("Failed to clear documents: %s", e)

    @log_query
    def search(self, query_string, **kwargs):
        if len(query_string) == 0:
            return {
                'results': [],
                'hits': 0,
            }

        if self._do_setup(): return

        search_kwargs = self.build_search_kwargs(query_string, **kwargs)

        if search_kwargs['options'].get('limit', None) is 0:
            return {
                'results': [],
                'hits': 0,
            }

        try:
            print search_kwargs
            search_kwargs['options'] = search.QueryOptions(**search_kwargs['options'])
            raw_results = self.index.search(search.Query(**search_kwargs))
        except search.Error as e:
            if not self.silently_fail:
                raise
            self.log.error("Failed to query Google App Engine using '%s': %s",
                            query_string, e)
            raw_results = {}

        return self._process_results(raw_results, kwargs['result_class'])

    def build_search_kwargs(self, query_string, sort_by=None, start_offset=0,
                            end_offset=None, fields='', highlight=False,
                            facets=None, date_facets=None, query_facets=None,
                            narrow_queries=None, spelling_query=None,
                            within=None, dwithin=None, distance_point=None,
                            models=None, limit_to_registered_models=None,
                            result_class=None):
        kwargs = {}
        query_chunks = []

        if result_class is None:
            result_class = SearchResult

        if limit_to_registered_models is None:
            limit_to_registered_models = getattr(settings,
                                                    'HAYSTACK_LIMIT_TO_REGISTERED_MODELS',
                                                    True)

        if facets is not None:
            warnings.warn("Google App Engine does not handle faceting.",
                            Warning, stacklevel=2)

        if date_facets is not None:
            warnings.warn("Google App Engine does not handle date faceting.",
                            Warning, stacklevel=2)

        if query_facets is not None:
            warnings.warn("Google App Engine does not handle query faceting.",
                            Warning, stacklevel=2)

        if spelling_query is not None:
            warnings.warn("Google App Engine does not handle spelling queries.",
                            Warning, stacklevel=2)

        if within is not None:
            warnings.warn("Google App Engine does not handle bounding box queries.",
                            Warning, stacklevel=2)

        if highlight is not False:
            warnings.warn("Google App Engine does not support highlighting.",
                            Warning, stacklevel=2)

        # Search Options
        kwargs['options'] = {
            'offset': start_offset,
        }

        if end_offset is not None:
            kwargs['options']['limit'] = end_offset-start_offset

        if fields and len(fields) > 0:
            kwargs['options']['returned_fields'] = fields.split(' ') if isinstance(fields, six.string_types) else fields

        # Sorting
        if sort_by:
            expressions = []
            for name, direction in sort_by:
                if direction == 'desc':
                    direction = search.SortExpression.DESCENDING
                else:
                    direction = search.SortExpression.ASCENDING

                expressions.append(search.SortExpression(expression=name, direction=direction))

            kwargs['options']['sort_options'] = search.SortOptions(expressions=expressions)

        # Models
        if models and len(models) > 0:
            model_choices = sorted(['%s.%s' % (model._meta.app_label, model._meta.module_name) for model in models])
        elif limit_to_registered_models:
            model_choices = self.build_models_list()
        else:
            model_choices = []

        if model_choices:
            model_chunk = '{}=("{}")'.format(DJANGO_CT, '" OR "'.join(model_choices))
            query_chunks.append(model_chunk)

        # Narrow Queries
        if narrow_queries is not None:
            query_chunks += narrow_queries

        # Distance Queries
        if dwithin is not None:
            lng, lat = dwithin['point'].get_coords()
            query_chunks.append('distance(geopoint({},{}), {}) <= {}'
                .format(lat, lng, dwithin['field'], dwithin['distance'].m))

        kwargs['query_string'] = '({}) AND ({})'.format(query_string, ') AND ('.join(query_chunks))
        return kwargs

    def _process_results(self, raw_results, result_class):
        hits = raw_results.number_found
        results = []

        from haystack import connections
        unified_index = connections[self.connection_alias].get_unified_index()
        indexed_models = unified_index.get_indexed_models()

        for raw_result in raw_results.results:
            fields = dict((str(f.name), f.value) for f in raw_result.fields)
            doc_id = fields[DJANGO_ID]
            app_label, model_name = fields[DJANGO_CT].split('.')
            model = get_model(app_label, model_name)

            if model and model in indexed_models:
                index = unified_index.get_index(model)
                for key, value in fields.items():
                    if key in index.fields and hasattr(index.fields[key], 'convert'):
                        fields[key] = index.fields[key].convert(value)
                    else:
                        fields[key] = value
                del(fields[DJANGO_CT])
                del(fields[DJANGO_ID])
                result = result_class(app_label, model_name, doc_id,
                                        0, **fields) # TODO: Get the real score
                results.append(result)
            else:
                hits -= 1
        return {
            'results': results,
            'hits': hits,
        }

class GAESearchQuery(BaseSearchQuery):
    def build_query_fragment(self, field, filter_type, value):
        query_frag = ''

        if not hasattr(value, 'input_type_name'):
            # Handle when we've got a ``ValuesListQuerySet``
            if hasattr(value, 'values_list'):
                value = list(value)

            if isinstance(value, six.string_types):
                # It's not an ``InputType``. Assume ``Clean``
                value = Clean(value)
            else:
                value = PythonData(value)

        prepared_value = value.prepare(self)

        if field == 'content':
            index_fieldname = ''
            filter_types = {
                'contains': u'({value})',
                'startswith': u'~{value}',
                'exact': u'"{value}"',
            }
        else:
            index_fieldname = connections[self._using].get_unified_index().get_index_fieldname(field)
            filter_types = {
                'contains': u'{field} = ({value})',
                'startswith': u'{field} = ~{value}',
                'exact': u'{field} = "{value}"',
                'gt': u'{field} > {value}',
                'gte': u'{field} >= {value}',
                'lt': u'{field} < {value}',
                'lte': u'{field <= {value}',
            }

        if filter_type in ['contains', 'startswith']:
            if value.input_type_name == 'exact':
                prepared_value = '"{}"'.format(prepared_value)
            query_frag = filter_types[filter_type].format(field=index_fieldname, value=prepared_value)
        elif filter_type == 'in':
            in_options = []

            for possible_value in prepared_value:
                if isinstance(possible_value, six.string_types):
                    in_options.append('"{}"'.format(possible_value))
                else:
                    in_options.append('{}'.format(possible_value))

            frag = '({})'.format(') OR ('.join(in_options))
            query_frag = filter_types['contains'].format(field=index_fieldname, value=frag)
        elif filter_type == 'range':
            start = prepared_value[0]
            end = prepared_value[1]

            start = filter_types['gte'].format(field=index_fieldname, value=start)
            end = filter_types['lte'].format(field=index_fieldname, value=end)
            query_frag = '({}) AND ({})'.format(start, end)
        else:
            if value.input_type_name != 'exact':
                prepared_value = Exact(prepared_value).prepare(self)
            query_frag = filter_types[filter_type].format(field=index_fieldname, value=prepared_value)

        if len(query_frag) and not isinstance(value, Raw):
            if not query_frag.startswith('(') and not query_frag.endswith(')'):
                query_frag = '({})'.format(query_frag)

        return query_frag

class GAEEngine(BaseEngine):
    backend = GAESearchBackend
    query = GAESearchQuery
