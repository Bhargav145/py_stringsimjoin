# jaccard join
from joblib import delayed
from joblib import Parallel
import pandas as pd

from py_stringsimjoin.join.set_sim_join import set_sim_join
from py_stringsimjoin.utils.helper_functions import split_table, \
                                                    get_num_processes_to_launch
from py_stringsimjoin.utils.validation import validate_attr, \
    validate_comp_op_for_sim_measure, validate_key_attr, validate_input_table, \
    validate_threshold, validate_tokenizer, validate_output_attrs


def jaccard_join(ltable, rtable,
                 l_key_attr, r_key_attr,
                 l_join_attr, r_join_attr,
                 tokenizer, threshold, comp_op='>=',
                 l_out_attrs=None, r_out_attrs=None,
                 l_out_prefix='l_', r_out_prefix='r_',
                 out_sim_score=True, n_jobs=1, show_progress=True):
    """Join two tables using Jaccard similarity measure.

    Finds tuple pairs from left table and right table such that the Jaccard similarity between
    the join attributes satisfies the condition on input threshold. That is, if the comparison
    operator is '>=', finds tuples pairs whose Jaccard similarity on the join attributes is
    greater than or equal to the input threshold.

    Args:
        ltable (dataframe): left input table.

        rtable (dataframe): right input table.

        l_key_attr (string): key attribute in left table.

        r_key_attr (string): key attribute in right table.

        l_join_attr (string): join attribute in left table.

        r_join_attr (string): join attribute in right table.

        tokenizer (Tokenizer object): tokenizer to be used to tokenize join attributes.

        threshold (float): Jaccard similarity threshold to be satisfied.

        comp_op (string): Comparison operator. Supported values are '>=', '>' and '='
                          (defaults to '>=').  

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

    Returns:
        output table (dataframe)
    """

    # check if the input tables are dataframes
    validate_input_table(ltable, 'left table')
    validate_input_table(rtable, 'right table')

    # check if the key attributes and join attributes exist
    validate_attr(l_key_attr, ltable.columns,
                  'key attribute', 'left table')
    validate_attr(r_key_attr, rtable.columns,
                  'key attribute', 'right table')
    validate_attr(l_join_attr, ltable.columns,
                  'join attribute', 'left table')
    validate_attr(r_join_attr, rtable.columns,
                  'join attribute', 'right table')

    # check if the input tokenizer is valid
    validate_tokenizer(tokenizer)

    # check if the input threshold is valid
    validate_threshold(threshold, 'JACCARD')

    # check if the comparison operator is valid
    validate_comp_op_for_sim_measure(comp_op, 'JACCARD')

    # check if the output attributes exist
    validate_output_attrs(l_out_attrs, ltable.columns,
                          r_out_attrs, rtable.columns)

    # check if the key attributes are unique and do not contain missing values
    validate_key_attr(l_key_attr, ltable, 'left table')
    validate_key_attr(r_key_attr, rtable, 'right table')

    # computes the actual number of jobs to launch.
    n_jobs = get_num_processes_to_launch(n_jobs)

    if n_jobs == 1:
        output_table = set_sim_join(ltable, rtable,
                                    l_key_attr, r_key_attr,
                                    l_join_attr, r_join_attr,
                                    tokenizer, 'JACCARD',
                                    threshold, comp_op,
                                    l_out_attrs, r_out_attrs,
                                    l_out_prefix, r_out_prefix,
                                    out_sim_score, show_progress)
        output_table.insert(0, '_id', range(0, len(output_table)))
        return output_table
    else:
        r_splits = split_table(rtable, n_jobs)
        results = Parallel(n_jobs=n_jobs)(delayed(set_sim_join)(
                                              ltable, r_splits[job_index],
                                              l_key_attr, r_key_attr,
                                              l_join_attr, r_join_attr,
                                              tokenizer, 'JACCARD',
                                              threshold, comp_op,
                                              l_out_attrs, r_out_attrs,
                                              l_out_prefix, r_out_prefix,
                                              out_sim_score,
                                      (show_progress and (job_index==n_jobs-1)))
                                          for job_index in range(n_jobs))
        output_table = pd.concat(results)
        output_table.insert(0, '_id', range(0, len(output_table)))
        return output_table
