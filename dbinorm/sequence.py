#coding: utf-8

""" Functions for change sequence.
"""

import collections
import logging

log = logging.getLogger()

def reallyiterable(x):
  return not isinstance(x, basestring) and isinstance(x, collections.Iterable)

def set_sequence_next_val(con, sequence, next_val):
    """ Alter sequnce value that next val will be next_val.
        NOTE: there is auto commit, for alter seqence method.
    """
    cur_next_val = con.get_next_val(sequence)
    # req = u'select {SEQUENCE}.nextVal as NEXT_VAL from dual'.format(SEQUENCE = sequence)
    # cur_next_val = con.uexec_get_list(req)[0]['NEXT_VAL']
    increment = next_val - 1 - cur_next_val
    if increment == 0:
        return True
    alt_req =   'alter sequence {SEQUENCE} increment by {INCREMENT}'.format(
        SEQUENCE = sequence, INCREMENT = increment)
    alt_req_1 = 'alter sequence {SEQUENCE} increment by 1'.format(
        SEQUENCE = sequence)
    con.exec_req(alt_req)
    cur_next_val = con.get_next_val(sequence)
    # cur_next_val = con.uexec_get_list(req)
    con.exec_req(alt_req_1)
    log.info("sequence %s next val: %s", sequence, next_val)
    return True

def set_sequence_to_max_id(con, sequence = None, table = None):
    """ Alter sequnce value that next val will be max_id + 1.
        NOTE: there is auto commit, for alter seqence method.
    """
    if sequence == None and table == None:
        raise Exception('Sequence or table should be setted')

    is_seq_iter = reallyiterable(sequence)
    is_tab_iter = reallyiterable(table)
    if (sequence == None or table == None) and \
       (is_seq_iter or is_tab_iter):
        if is_seq_iter:
            for seq in sequence:
                set_sequence_to_max_id(con, sequence = seq, table = None)
        elif is_tab_iter:
            for tab in table:
                set_sequence_to_max_id(con, sequence = None, table = tab)
        return

    if table == None:
        table = sequence_to_table(sequence)
    elif sequence == None:
        sequence = table_to_sequence(table)

    max_id = get_max_id(con, table)
    set_sequence_next_val(con, sequence, max_id + 1)
    log.info("Set sequence %s next val %s.", sequence, max_id + 1)

def get_max_id(con, table_name):
    req = 'select max(id) MAX_ID from {table}'.format(table = con.check_name(table_name))
    r = con.exec_get_list(req)
    return r[0].get("MAX_ID")

def sequence_to_table(sequence):
    if sequence.upper().endswith('_S'):
        table = sequence[:-2]
        return table
    else:
        raise Exception("Sequence not ends with _S.")

def table_to_sequence(table):
    return table + "_S"

