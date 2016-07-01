# overlap join
from py_stringsimjoin.filter.overlap_filter import OverlapFilter
from py_stringsimjoin.utils.validation import validate_tokenizer


def overlap_join(ltable, rtable,
                 l_key_attr, r_key_attr,
                 l_join_attr, r_join_attr,
                 tokenizer, threshold, comp_op='>=',
                 allow_missing=False,
                 l_out_attrs=None, r_out_attrs=None,
                 l_out_prefix='l_', r_out_prefix='r_',
                 out_sim_score=True, n_jobs=1, show_progress=True):
    """Join two tables using overlap measure.

    Finds tuple pairs from left table and right table such that the overlap between
    the join attributes satisfies the condition on input threshold. That is, if the comparison
    operator is '>=', finds tuples pairs whose overlap on the join attributes is
    greater than or equal to the input threshold.

    Args:
        ltable (dataframe): left input table.

        rtable (dataframe): right input table.

        l_key_attr (string): key attribute in left table.

        r_key_attr (string): key attribute in right table.

        l_join_attr (string): join attribute in left table.

        r_join_attr (string): join attribute in right table.

        tokenizer (Tokenizer object): tokenizer to be used to tokenize join attributes.

        threshold (float): overlap threshold to be satisfied.

        l_out_attrs (list): list of attributes to be included in the output table from
                            left table (defaults to None).

        r_out_attrs (list): list of attributes to be included in the output table from
                            right table (defaults to None).

        l_out_prefix (string): prefix to use for the attribute names coming from left
                               table (defaults to 'l\_').

        r_out_prefix (string): prefix to use for the attribute names coming from right
                               table (defaults to 'r\_').

        out_sim_score (boolean): flag to indicate if similarity score needs to be
                                 included in the output table (defaults to True).

        n_jobs (int): The number of jobs to use for the computation (defaults to 1).                                                                                            
            If -1 all CPUs are used. If 1 is given, no parallel computing code is used at all, 
            which is useful for debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are used. 
            Thus for n_jobs = -2, all CPUs but one are used. If (n_cpus + 1 + n_jobs) becomes less than 1,
            then n_jobs is set to 1.

        show_progress (boolean): flag to indicate if task progress need to be shown (defaults to True).

    Returns:
        output table (dataframe)
    """

    # check if the input tokenizer is valid
    validate_tokenizer(tokenizer)

    # set return_set flag of tokenizer to be True, in case it is set to False
    revert_tokenizer_return_set_flag = False
    if not tokenizer.get_return_set():
        tokenizer.set_return_set(True)
        revert_tokenizer_return_set_flag = True

    overlap_filter = OverlapFilter(tokenizer, threshold, comp_op, allow_missing)
    output_table =  overlap_filter.filter_tables(ltable, rtable,
                                                 l_key_attr, r_key_attr,
                                                 l_join_attr, r_join_attr,
                                                 l_out_attrs, r_out_attrs,
                                                 l_out_prefix, r_out_prefix,
                                                 out_sim_score, n_jobs,
                                                 show_progress)

    # revert the return_set flag of tokenizer, in case it was modified.
    if revert_tokenizer_return_set_flag:
        tokenizer.set_return_set(False)

    return output_table
