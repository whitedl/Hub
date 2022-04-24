"""
Example of derivative sync plugins.
"""

required = ['pandas', 'random', 'datetime']

import meerschaum as mrsm
from meerschaum.utils.packages import lazy_import

# Lazy import so that it doesn't need to be loaded every time mrsm is called,
# only when it's needed
pd = lazy_import('pandas')

def register(pipe: mrsm.Pipe, **kw):
    return { 'columns': { 'datetime': 'timestamp' } }

def fetch(pipe: mrsm.Pipe, **kw):
    """Gets the data for the parent pipe.

    Args:
        pipe (mrsm.Pipe): The parent pipe.

    Returns:
        pandas.DataFrame: The dataframe of new data.
    """
    import random
    import datetime

    now = datetime.datetime.now()
    df = pd.DataFrame()

    # Generate 3 rows of random data
    for i in range(3):
        data = {
            # Ensure distinct timestamps
            'timestamp': now + datetime.timedelta(seconds=i),
            'random1': random.randint(1, 100),
            'random2': random.randint(101, 200)
        }
        # Populate df row-by-row
        df_new = pd.DataFrame([data])
        df = pd.concat([df, df_new], ignore_index=True)

    return df

def sync(pipe: mrsm.Pipe, **kw):
    """Syncs parent pipe, creates child pipe, derives 
    new data from the parent and syncs it with the child.

    Args:
        pipe (mrsm.Pipe): The parent pipe.

    Returns:
        mrsm.Pipe: The child pipe.
    """
    # Only continue if we're dealing with the parent pipe.
    if pipe.location_key is not None:
        return True, "Success"

    # Fetch data
    parent_data = fetch(pipe, **kw)
    # Sync parent pipe
    pipe.sync(parent_data, **kw)
    # Create child pipe, carrying over parent pipe's information
    # and setting the child pipe's location key to 'deriv_1'
    child_pipe = mrsm.Pipe(pipe.connector_keys, pipe.metric_key, 'deriv_1', columns = pipe.columns)
    # Get child data
    child_data = get_child_data(parent_data)
    # Add the fetched and additional data to the child pipe
    return child_pipe.sync(child_data, **kw)

def get_child_data(parent_data: pd.DataFrame):
    """Derives new data from parent data.

    Args:
        parent_data (pandas.DataFrame): The new parent data.

    Returns:
        pandas.DataFrame: The child data, including the original data and the derived data.
    """
    # pandas.DataFrame.assign(...) is useful for deriving data.
    return parent_data.assign(
        # deriv_random1 and deriv_random2 will be the names of the new columns.
        deriv_random1=lambda row: row.random1 * 2,
        deriv_random2=lambda row: row.random2 + 0.5
    )