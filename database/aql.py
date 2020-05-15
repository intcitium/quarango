def search(term):
    return '''
    FOR d in v_search_test SEARCH d.name == '%s' RETURN d
    ''' % term


def neighbors(node_key):
    return '''
    FOR v, e, p IN 1 ANY '%s' GRAPH 'test_graph2'
      RETURN {v, e}
    ''' % node_key
