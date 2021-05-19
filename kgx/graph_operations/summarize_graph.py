from typing import Dict, List, Optional, Any, Callable
from sys import stderr

import yaml
from json import dump
from json.encoder import JSONEncoder

from kgx import GraphEntityType
from kgx.graph.base_graph import BaseGraph
from kgx.prefix_manager import PrefixManager

TOTAL_NODES = 'total_nodes'
NODE_CATEGORIES = 'node_categories'

NODE_ID_PREFIXES_BY_CATEGORY = 'node_id_prefixes_by_category'
NODE_ID_PREFIXES = 'node_id_prefixes'

COUNT_BY_CATEGORY = 'count_by_category'

COUNT_BY_ID_PREFIXES_BY_CATEGORY = 'count_by_id_prefixes_by_category'
COUNT_BY_ID_PREFIXES = 'count_by_id_prefixes'

TOTAL_EDGES = 'total_edges'
EDGE_PREDICATES = 'predicates'
COUNT_BY_EDGE_PREDICATES = 'count_by_predicates'
COUNT_BY_SPO = 'count_by_spo'


# Note: the format of the stats generated might change in the future

####################################################################################
# New "Inspector Class" design pattern for KGX stream data processing
####################################################################################
def gs_default(o):
    """
    JSONEncoder 'default' function override to
    properly serialize 'Set' objects (into 'List')
    """
    if isinstance(o, GraphSummary.Category):
        return o.json_object()
    else:
        try:
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)
        # Let the base class default method raise the TypeError
        return JSONEncoder.default(o)


class GraphSummary:
    def __init__(
            self,
            name='',
            node_facet_properties: Optional[List] = None,
            edge_facet_properties: Optional[List] = None,
            error_log: str = None,
            progress_monitor: Optional[Callable[[GraphEntityType, List], None]] = None,
    ):
        """
        Class for generating a "classical" knowledge graph summary.

        The optional 'progress_monitor' for the validator should be a lightweight Callable
        which is injected into the class 'inspector' Callable, designed to intercepts
        node and edge records streaming through the Validator (inside a Transformer.process() call.
        The first (GraphEntityType) argument of the Callable tags the record as a NODE or an EDGE.
        The second argument given to the Callable is the current record itself.
        This Callable is strictly meant to be procedural and should *not* mutate the record.
        The intent of this Callable is to provide a hook to KGX applications wanting the
        namesake function of passively monitoring the graph data stream. As such, the Callable
        could simply tally up the number of times it is called with a NODE or an EDGE, then
        provide a suitable (quick!) report of that count back to the KGX application. The
        Callable (function/callable class) should not modify the record and should be of low
        complexity, so as not to introduce a large computational overhead to validation!

        Parameters
        ----------
        name: str
            (Graph) name assigned to the summary.
        node_facet_properties: Optional[List]
                A list of properties to facet on. For example, ``['provided_by']``
        edge_facet_properties: Optional[List]
                A list of properties to facet on. For example, ``['provided_by']``
        progress_monitor: Optional[Callable[[GraphEntityType, List], None]]
            Function given a peek at the current record being processed by the class wrapped Callable.
        error_log: str
            Where to write any graph processing error message (stderr, by default)

        """
        # formal arguments
        self.name = name
        
        self.node_facet_properties: Optional[List] = node_facet_properties
        if self.node_facet_properties:
            for facet_property in self.node_facet_properties:
                self.add_node_stat(facet_property, set())
                
        self.edge_facet_properties: Optional[List] = edge_facet_properties
        if self.edge_facet_properties:
            for facet_property in self.edge_facet_properties:
                self.edge_stats[facet_property] = set()

        self.progress_monitor: Optional[Callable[[GraphEntityType, List], None]] = progress_monitor

        if error_log:
            self.error_log = open(error_log, mode='w')
        else:
            self.error_log = stderr

        # internal attributes
        self.node_catalog: Dict[str, List[int]] = dict()

        self.node_categories: Dict[str, GraphSummary.Category] = dict()
        self.node_categories['unknown'] = self.Category('unknown')

        self.nodes_processed = False

        self.node_stats: Dict = {
            TOTAL_NODES: 0,
            NODE_CATEGORIES: set(),
            NODE_ID_PREFIXES: set(),
            NODE_ID_PREFIXES_BY_CATEGORY: {'unknown': set()},
            COUNT_BY_CATEGORY: {'unknown': {'count': 0}},
            COUNT_BY_ID_PREFIXES_BY_CATEGORY: dict(),
            COUNT_BY_ID_PREFIXES: dict(),
        }

        self.edges_processed: bool = False

        self.edge_stats: Dict = {
            TOTAL_EDGES: 0,
            EDGE_PREDICATES: set(),
            COUNT_BY_EDGE_PREDICATES: {'unknown': {'count': 0}},
            COUNT_BY_SPO: {},
        }

        self.graph_stats: Dict[str, Dict] = dict()
    
    def __call__(self, entity_type: GraphEntityType, rec: List):
        """
        Transformer 'inspector' Callable
        """
        if self.progress_monitor:
            self.progress_monitor(entity_type, rec)
        if entity_type == GraphEntityType.EDGE:
            self.analyse_edge(*rec)
        elif entity_type == GraphEntityType.NODE:
            self.analyse_node(*rec)
        else:
            raise RuntimeError("Unexpected GraphEntityType: " + str(entity_type))

    class Category:

        # The 'category map' just associates a unique int catalog
        # index ('cid') value as a proxy for the full curie string,
        # to reduce storage in the main node catalog
        _category_curie_map: List[str] = list()

        def __init__(self, category=''):
            self.category = category
            if category not in self._category_curie_map:
                self._category_curie_map.append(category)
            self.category_stats: Dict[str, Any] = dict()
            self.category_stats['count']: int = 0
            self.category_stats['count_by_source']: Dict[str, int] = {'unknown': 0}
            self.category_stats['count_by_id_prefix']: Dict[str, int] = dict()

        def get_name(self):
            return self.category

        def get_cid(self):
            return self._category_curie_map.index(self.category)

        @classmethod
        def get_category_curie(cls, cid: int):
            return cls._category_curie_map[cid]

        def get_id_prefixes(self):
            return set(self.category_stats['count_by_id_prefix'].keys())

        def get_count_by_id_prefixes(self):
            return self.category_stats['count_by_id_prefix']

        def get_count(self):
            return self.category_stats['count']

        def analyse_node_category(self, summary, n, data):

            self.category_stats['count'] += 1

            prefix = PrefixManager.get_prefix(n)
            if not prefix:
                print(f"Warning: node id {n} has no CURIE prefix", file=self.error_log)
            else:
                if prefix in self.category_stats['count_by_id_prefix']:
                    self.category_stats['count_by_id_prefix'][prefix] += 1
                else:
                    self.category_stats['count_by_id_prefix'][prefix] = 1

            if 'provided_by' in data:
                for s in data['provided_by']:
                    if s in self.category_stats['count_by_source']:
                        self.category_stats['count_by_source'][s] += 1
                    else:
                        self.category_stats['count_by_source'][s] = 1
            else:
                self.category_stats['count_by_source']['unknown'] += 1
            #
            # Moved this computation from the 'analyse_node() method below
            #
            if summary.node_facet_properties:
                for facet_property in summary.node_facet_properties:
                    summary.node_stats = summary.get_facet_counts(
                        data, summary.node_stats, COUNT_BY_CATEGORY, self.category, facet_property
                    )

        def json_object(self):
            return {
                'id_prefixes': list(self.category_stats['count_by_id_prefix'].keys()),
                'count': self.category_stats['count'],
                'count_by_source': self.category_stats['count_by_source'],
                'count_by_id_prefix': self.category_stats['count_by_id_prefix']
            }

    def analyse_node(self, n, data):
        if n in self.node_catalog:
            # Report duplications of node records, as discerned from node id.
            print("Duplicate node identifier '" + n +
                  "' encountered in input node data? Ignoring...", file=self.error_log)
            return
        else:
            self.node_catalog[n] = list()
        
        if 'category' not in data:
            category = self.node_categories['unknown']
            category.analyse_node_category(self, n, data)
            print(
                "Node with identifier '" + n + "' is missing its 'category' value? " +
                "Counting it as 'unknown', but otherwise ignoring in the analysis...", file=self.error_log
            )
            return
        
        categories = data['category']
        for category_curie in categories:
            if category_curie not in self.node_categories:
               self.node_categories[category_curie] = self.Category(category_curie)
            category = self.node_categories[category_curie]
            category_idx: int = category.get_cid()
            if category_idx not in self.node_catalog[n]:
               self.node_catalog[n].append(category_idx)
            category.analyse_node_category(self, n, data)

        #
        # Moved this computation from the 'analyse_node_category() method above
        #
        # if self.node_facet_properties:
        #     for facet_property in self.node_facet_properties:
        #         self.node_stats = self.get_facet_counts(
        #             data, self.node_stats, COUNT_BY_CATEGORY, category_curie, facet_property
        #         )
    
    def analyse_edge(self, u, v, k, data):
        # we blissfully now assume that all the nodes of a
        # graph stream were analysed first by the GraphSummary
        # before the edges are analysed, thus we can test for
        # node 'n' existence internally, by identifier.

        self.edge_stats[TOTAL_EDGES] += 1

        if 'predicate' not in data:
            self.edge_stats[COUNT_BY_EDGE_PREDICATES]['unknown']['count'] += 1
            edge_predicate = "unknown"
        else:
            edge_predicate = data['predicate']
            self.edge_stats[EDGE_PREDICATES].add(edge_predicate)
            if edge_predicate in self.edge_stats[COUNT_BY_EDGE_PREDICATES]:
                self.edge_stats[COUNT_BY_EDGE_PREDICATES][edge_predicate]['count'] += 1
            else:
                self.edge_stats[COUNT_BY_EDGE_PREDICATES][edge_predicate] = {'count': 1}
            
            if self.edge_facet_properties:
                for facet_property in self.edge_facet_properties:
                    self.edge_stats = self.get_facet_counts(
                        data, self.edge_stats, COUNT_BY_EDGE_PREDICATES, edge_predicate, facet_property
                    )
        
        if u not in self.node_catalog:
            print("Edge 'subject' node ID '" + u + "' not found in node catalog? Ignoring...", file=self.error_log)
            # removing from edge count
            self.edge_stats[TOTAL_EDGES] -= 1
            self.edge_stats[COUNT_BY_EDGE_PREDICATES]['unknown']['count'] -= 1
            return

        for subj_cat_idx in self.node_catalog[u]:
            subject_category = self.Category.get_category_curie(subj_cat_idx)

            if v not in self.node_catalog:
                print("Edge 'object' node ID '" + v +
                      "' not found in node catalog? Ignoring...", file=self.error_log)
                self.edge_stats[TOTAL_EDGES] -= 1
                self.edge_stats[COUNT_BY_EDGE_PREDICATES]['unknown']['count'] -= 1
                return

            for obj_cat_idx in self.node_catalog[v]:

                object_category = self.Category.get_category_curie(obj_cat_idx)

                # Process the 'valid' S-P-O triple here...
                key = f"{subject_category}-{edge_predicate}-{object_category}"
                if key in self.edge_stats[COUNT_BY_SPO]:
                    self.edge_stats[COUNT_BY_SPO][key]['count'] += 1
                else:
                    self.edge_stats[COUNT_BY_SPO][key] = {'count': 1}
        
                if self.edge_facet_properties:
                    for facet_property in self.edge_facet_properties:
                        self.edge_stats = \
                            self.get_facet_counts(data, self.edge_stats, COUNT_BY_SPO, key, facet_property)
    
    def get_name(self):
        return self.name
    
    def get_node_stats(self) -> Dict[str, Any]:

        if not self.nodes_processed:
            self.nodes_processed = True

            for category in self.node_categories.values():
                category_curie = category.get_name()
                self.node_stats[NODE_CATEGORIES].add(category_curie)
                self.node_stats[COUNT_BY_CATEGORY][category_curie] = category.get_count()

                id_prefixes = category.get_id_prefixes()
                self.node_stats[NODE_ID_PREFIXES_BY_CATEGORY][category_curie] = id_prefixes
                self.node_stats[NODE_ID_PREFIXES].update(id_prefixes)

                self.node_stats[COUNT_BY_ID_PREFIXES_BY_CATEGORY][category_curie] = category.get_count_by_id_prefixes()

                for prefix in self.node_stats[COUNT_BY_ID_PREFIXES_BY_CATEGORY][category_curie]:
                    if prefix not in self.node_stats[COUNT_BY_ID_PREFIXES]:
                        self.node_stats[COUNT_BY_ID_PREFIXES][prefix] = 0
                    self.node_stats[COUNT_BY_ID_PREFIXES][prefix] += \
                        self.node_stats[COUNT_BY_ID_PREFIXES_BY_CATEGORY][category_curie][prefix]

            self.node_stats[NODE_CATEGORIES] = sorted(list(self.node_stats[NODE_CATEGORIES]))

            if self.node_facet_properties:
                for facet_property in self.node_facet_properties:
                    self.node_stats[facet_property] = sorted(list(self.node_stats[facet_property]))

            if not self.node_stats[TOTAL_NODES]:
                self.node_stats[TOTAL_NODES] = len(self.node_catalog)

        return self.node_stats
    
    def add_node_stat(self, tag: str, value: Any):
        self.node_stats[tag] = value
    
    def get_edge_stats(self) -> Dict[str, Any]:
        # Not sure if this is "safe" but assume that edge_stats may be finalized
        # and cached once after the first time the edge stats are accessed
        if not self.edges_processed:
            self.edges_processed = True

            self.edge_stats[EDGE_PREDICATES] = sorted(list(self.edge_stats[EDGE_PREDICATES]))

            if self.edge_facet_properties:
                for facet_property in self.edge_facet_properties:
                    self.edge_stats[facet_property] = sorted(list(self.edge_stats[facet_property]))
        
        return self.edge_stats

    def wrap_graph_stats(
            self,
            graph_name: str,
            node_stats: Dict[str, Any],
            edge_stats: Dict[str, Any],
    ):
        if not self.graph_stats:
            self.graph_stats = {
                'graph_name': graph_name,
                'node_stats': node_stats,
                'edge_stats': edge_stats,
            }
        return self.graph_stats

    def get_graph_summary(self, name: str = None, **kwargs) -> Dict:
        """
        Similar to summarize_graph except that the node and edge statistics are already captured
        in the GraphSummary class instance (perhaps by Transformer.process() stream inspection)
        and therefore, the data structure simply needs to be 'finalized' for saving or similar use.

        Parameters
        ----------
        name: Optional[str]
            Name for the graph (if being renamed)
        kwargs: Dict
            Any additional arguments (ignored in this method at present)

        Returns
        -------
        Dict
            A knowledge map dictionary corresponding to the graph

        """
        return self.wrap_graph_stats(
            graph_name=name if name else self.name,
            node_stats=self.get_node_stats(),
            edge_stats=self.get_edge_stats()
        )
    
    def summarize_graph(
            self,
            graph: BaseGraph
    ) -> Dict:
        """
        Summarize the entire graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph

        Returns
        -------
        Dict
            The stats dictionary

        """
        return self.wrap_graph_stats(
            graph_name=self.name if self.name else graph.name,
            node_stats=self.summarize_graph_nodes(graph),
            edge_stats=self.summarize_graph_edges(graph)
        )
    
    def summarize_graph_nodes(self, graph: BaseGraph) -> Dict:
        """
        Summarize the nodes in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph

        Returns
        -------
        Dict
            The node stats
        """
        for n, data in graph.nodes(data=True):
            self.analyse_node(n, data)
        
        return self.get_node_stats()
    
    def summarize_graph_edges(self, graph: BaseGraph) -> Dict:
        """
        Summarize the edges in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph

        Returns
        -------
        Dict
            The edge stats
        """
        for u, v, k, data in graph.edges(keys=True, data=True):
            self.analyse_edge(u, v, k, data)
        
        return self.get_edge_stats()
    
    def get_facet_counts(self, data: Dict, stats: Dict, x: str, y: str, facet_property: str) -> Dict:
        """
        Facet on ``facet_property`` and record the count for ``stats[x][y][facet_property]``.

        Parameters
        ----------
        data: dict
            Node/edge data dictionary
        stats: dict
            The stats dictionary
        x: str
            first key
        y: str
            second key
        facet_property: str
            The property to facet on

        Returns
        -------
        Dict
            The stats dictionary

        """
        if facet_property in data:
            if isinstance(data[facet_property], list):
                for k in data[facet_property]:
                    if facet_property not in stats[x][y]:
                        stats[x][y][facet_property] = {}
                    
                    if k in stats[x][y][facet_property]:
                        stats[x][y][facet_property][k]['count'] += 1
                    else:
                        stats[x][y][facet_property][k] = {'count': 1}
                        stats[facet_property].update([k])
            else:
                k = data[facet_property]
                if facet_property not in stats[x][y]:
                    stats[x][y][facet_property] = {}
                
                if k in stats[x][y][facet_property]:
                    stats[x][y][facet_property][k]['count'] += 1
                else:
                    stats[x][y][facet_property][k] = {'count': 1}
                    stats[facet_property].update([k])
        else:
            if facet_property not in stats[x][y]:
                stats[x][y][facet_property] = {}
            if 'unknown' in stats[x][y][facet_property]:
                stats[x][y][facet_property]['unknown']['count'] += 1
            else:
                stats[x][y][facet_property]['unknown'] = {'count': 1}
                stats[facet_property].update(['unknown'])
        return stats
    
    def save(self, file, name: str = None, file_format: str = 'yaml'):
        """
        Save the current GraphSummary to a specified (open) file (device)
        """
        stats = self.get_graph_summary(name)
        if not file_format or file_format == 'yaml':
            yaml.dump(stats, file)
        else:
            dump(stats, file, indent=4, default=gs_default)


def generate_graph_stats(
        graph: BaseGraph,
        graph_name: str,
        filename: str,
        node_facet_properties: Optional[List] = None,
        edge_facet_properties: Optional[List] = None,
) -> None:
    """
    Generate stats from Graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    graph_name: str
        Name for the graph
    filename: str
        Filename to write the stats to
    node_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``

    """
    stats = summarize_graph(graph, graph_name, node_facet_properties, edge_facet_properties)
    with open(filename, 'w') as gsh:
        yaml.dump(stats, gsh)


def summarize_graph(
        graph: BaseGraph,
        name: str = None,
        node_facet_properties: Optional[List] = None,
        edge_facet_properties: Optional[List] = None,
) -> Dict:
    """
    Summarize the entire graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    name: str
        Name for the graph
    node_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``

    Returns
    -------
    Dict
        The stats dictionary

    """
    gs = GraphSummary(name, node_facet_properties, edge_facet_properties)
    return gs.summarize_graph(graph)
