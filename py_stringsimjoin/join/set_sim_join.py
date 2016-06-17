# set similarity join
from six import iteritems
import pandas as pd
import pyprind

from py_stringsimjoin.filter.position_filter import PositionFilter, \
    _find_candidates as find_candidates_position_filter
from py_stringsimjoin.filter.filter_utils import get_prefix_length
from py_stringsimjoin.index.position_index import PositionIndex
from py_stringsimjoin.utils.helper_functions import build_dict_from_table, \
    find_output_attribute_indices, get_output_header_from_tables, \
    get_output_row_from_tables 
from py_stringsimjoin.utils.simfunctions import get_sim_function
from py_stringsimjoin.utils.tokenizers import tokenize
from py_stringsimjoin.utils.token_ordering import \
    gen_token_ordering_for_tables, order_using_token_ordering


def set_sim_join(ltable, rtable,
                 l_key_attr, r_key_attr,
                 l_join_attr, r_join_attr,
                 tokenizer, sim_measure_type, threshold,
                 l_out_attrs, r_out_attrs,
                 l_out_prefix, r_out_prefix,
                 out_sim_score):
    """Perform set similarity join for a split of ltable and rtable"""

    # find column indices of key attr, join attr and output attrs in ltable
    l_columns = list(ltable.columns.values)
    l_key_attr_index = l_columns.index(l_key_attr)
    l_join_attr_index = l_columns.index(l_join_attr)
    l_out_attrs_indices = find_output_attribute_indices(l_columns, l_out_attrs)

    # find column indices of key attr, join attr and output attrs in rtable
    r_columns = list(rtable.columns.values)
    r_key_attr_index = r_columns.index(r_key_attr)
    r_join_attr_index = r_columns.index(r_join_attr)
    r_out_attrs_indices = find_output_attribute_indices(r_columns, r_out_attrs)

    # build a dictionary on ltable
    ltable_dict = build_dict_from_table(ltable, l_key_attr_index,
                                        l_join_attr_index)

    # build a dictionary on rtable
    rtable_dict = build_dict_from_table(rtable, r_key_attr_index,
                                        r_join_attr_index)

    # generate token ordering using tokens in l_join_attr
    # and r_join_attr
    token_ordering = gen_token_ordering_for_tables(
                         [ltable_dict.values(),
                          rtable_dict.values()],
                         [l_join_attr_index,
                          r_join_attr_index],
                         tokenizer, sim_measure_type)

    # Tokenize l_join_attr and cache it. By doing so,
    # we tokenize l_join_attr only once.
    l_join_attr_dict = {}
    for row in ltable_dict.values():
        l_join_attr_dict[row[l_key_attr_index]] = order_using_token_ordering(
            tokenize(str(row[l_join_attr_index]), tokenizer, sim_measure_type),
                                                  token_ordering)

    # Build position index on l_join_attr
    position_index = PositionIndex(ltable_dict.values(),
                                   l_key_attr_index, l_join_attr_index,
                                   tokenizer, sim_measure_type,
                                   threshold, token_ordering)
    position_index.build()

    pos_filter = PositionFilter(tokenizer, sim_measure_type, threshold)
    sim_fn = get_sim_function(sim_measure_type)
    output_rows = []
    has_output_attributes = (l_out_attrs is not None or
                             r_out_attrs is not None)
    prog_bar = pyprind.ProgBar(len(rtable_dict.keys()))

    for r_row in rtable_dict.values():
        r_id = r_row[r_key_attr_index]
        r_string = str(r_row[r_join_attr_index])

        r_join_attr_tokens = tokenize(r_string, tokenizer, sim_measure_type)
        r_ordered_tokens = order_using_token_ordering(r_join_attr_tokens,
                                                      token_ordering)
        r_num_tokens = len(r_ordered_tokens)
        r_prefix_length = get_prefix_length(r_num_tokens,
                                            sim_measure_type,
                                            threshold, tokenizer)     

        candidate_overlap = find_candidates_position_filter(
                                r_ordered_tokens, r_num_tokens, r_prefix_length,
                                pos_filter, position_index)

        for cand, overlap in iteritems(candidate_overlap):
            if overlap > 0:
                l_ordered_tokens = l_join_attr_dict[cand]
                sim_score = sim_fn(l_ordered_tokens, r_ordered_tokens)
                if sim_score >= threshold:
                    if has_output_attributes:
                        output_row = get_output_row_from_tables(
                                         ltable_dict[cand], r_row,
                                         cand, r_id,
                                         l_out_attrs_indices,
                                         r_out_attrs_indices)
                        if out_sim_score:
                            output_row.append(sim_score)
                        output_rows.append(output_row)
                    else:
                        output_row = [cand, r_id]
                        if out_sim_score:
                            output_row.append(sim_score)
                        output_rows.append(output_row)
        prog_bar.update()

    output_header = get_output_header_from_tables(
                        l_key_attr, r_key_attr,
                        l_out_attrs, r_out_attrs,
                        l_out_prefix, r_out_prefix)
    if out_sim_score:
        output_header.append("_sim_score")

    # generate a dataframe from the list of output rows
    output_table = pd.DataFrame(output_rows, columns=output_header)
    return output_table
