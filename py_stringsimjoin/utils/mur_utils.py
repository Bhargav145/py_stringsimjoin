import pandas as pd

def ration_output(generator_function, size):
    reservoir = []
    for result in generator_function:
        reservoir.append(result)
        if len(reservoir) > size:
            yield reservoir
            reservoir = []
    if len(reservoir):
        yield reservoir

def consume_into_dataframe(generator_function, header):
    output_rows = None
    for rows in generator_function:

        if output_rows is None:
            output_rows = rows
        else:
            output_rows.extend(rows)
    data_frame = pd.DataFrame(output_rows, columns=header)
    return data_frame


def consume_into_csvfile(file_name, generator_function, header=None, index=False, mode=None):
    if header is not None:
        pd.DataFrame(columns=header).to_csv(file_name, index=index, mode='w+')
    for rows in generator_function:
        if mode is None:
            mode='a+'
        pd.DataFrame(rows).to_csv(file_name, index=index, mode=mode, header=False)
    return True






