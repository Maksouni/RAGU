"""Compatibility module that forwards to the Memgraph graph adapter."""

from .memgraph_adapter import (
    EDGE_TYPE,
    NODE_LABEL as VERTEX_TYPE,
    MemgraphStorage,
    _entity_from_row,
    _entity_to_attrs,
    _esc,
    _parse_json_list,
    _relation_from_row,
)
