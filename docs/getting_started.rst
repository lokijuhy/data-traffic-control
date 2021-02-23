Getting Started
===============


Register a project
-------------------------------------
``datatc`` will remember for you where the data for each of your projects is stored, so that you don't have to.
The first time you use ``datatc`` with a project, register it with :class:`datatc.data_manager.DataDirectory`.
You only have to do this once- ``datatc`` saves the information in ``~/.data_map.yaml``
so you never have to memorize that long file path again.

>>> DataDirectory.register_project('project_name', '/path/to/project/data/dir/')

Example:

>>> DataDirectory.register_project('mridle', '/home/user/data/mridle/data')

Once you've registered a project, you can establish a ``DataDirectory`` on the fly by referencing it's name.

>>> dd = DataDirectory.load('mridle')

The ``DataDirectory`` object, ``dd``, will be your gateway to all file discovery, load, and save operations.


----


Explore the data directory
--------------------------

``DataDirectory`` makes it easier to interact with your project's data directory.
With ``ls()``, you can print out the file structure:

>>> dd.ls()
raw/
    EntityViews/
        7 mixed items
    data_extracts/
        2019-11-23_Extract_3days/
            1 mixed items
        2020-01-05_Extract_3days_withHistory_v2/
            2 mixed items
        2020-02-04_Extract_3months/
            3 mixed items
    db-connection.R
    query.sql

You can also print the contents of a subdirectory by navigating to it via the ``[]`` operators:

>>> dd['data_extracts'].ls(full=True)
data_extracts/
    2019-11-23_Extract_3days/
        2019-11-23_Extract_3days.xlsx
    2020-01-05_Extract_3days_withHistory_v2/
        2020-01-05_Extract_3days_withHistory_v2.xlsx
        2020-01-05_Extract_3days_withHistory_v2_sql.sql
    2020-02-04_Extract_3months/
        2020-02-04_Extract_3months.sql
        2020-02-04_Extract_3months.xlsx
        3_month_export.csv


----


Loading data files
------------------

To load a file, navigate the file system using ``[]`` operators, and then call ``.load()``.

>>> raw_df = dd['data_extracts']['2020-02-04_Extract_3months']['2020-02-04_Extract_3months.xlsx'].load()


Don't worry about what format the file is in- ``datatc`` will intuit how to load the file.


Shortcuts for loading data files *faster*
''''''''''''''''''''''''''''''''''''''''''

latest
..........
If you use timestamps to version your data files, you can load the latest file or subdirectory within a directory with ``.latest()``:

>>> raw_df = dd['data_extracts'].latest()['2020-02-04_Extract_3months.xlsx'].load()

select
..........

To help you navigate those long finicky file names, ``DataDirectory`` provides a ``.select('hint')`` method to search for files matching a substring.

>>> raw_df = dd['data_extracts']['2020-02-04_Extract_3months'].select('xlsx').load()

Combing *latest* and *select*, the file load in the previous can be reduced to the following:

>>> raw_df = dd['data_extracts'].latest().select('xlsx').load()

Loading irregular data files
''''''''''''''''''''''''''''''''''''''''''

... my file needs special arguments to load
................................................
If your file needs special parameters to load it, specify them in ``load``, and they will be passed on to the internal loading function.
For example, if your csv file is actually pipe separated and has a non-default encoding, you can specify so:

>>> raw_df = dd['queries']['batch_query.csv'].load(sep='|', encoding='utf-16')

... my file type isn't recognized by `datatc`
................................................
If ``datatc`` doesn't recognize the file type, you can give it a type hint of which loader to use.
For example, ``datatc`` doesn't have a specific interface for reading tab separated files,
but if you tell it to treat it as a csv and instruct it to use tab as the separator, it will load it right up:

>>> raw_df = dd['queries']['batch_query.tsv'].load(data_interface_hint='csv', sep='\t')

... I want to load my file my own way
................................................
If you ever want to do your own load, and not use the build in ``.load()``, you can also use ``dd[...]['filename'].path``
to get the path to the file for use in a separate loading operation.


----


Saving data files
------------------

To save a file, navigate with ``dd`` to the position in the file system where you'd like to save your file using the ``[]`` operators,
and then call ``.save(data_object, file_name)``.

For example:

.. code-block:: python

    dd['processed_data'].save(processed_df, 'processed.csv')


----


Working with `SelfAwareData`
--------------------------------------------

``SelfAwareData`` helps you remember how your datasets were generated.

You have 2 options for turning your dataset into a ``SelfAwareData``:

1. Load from a file:

>>> my_sad = SelfAwareData.load_from_file('~/path/to/data.csv')

When you establish a ``SelfAwareData`` from a file, it will track the file that the ``SelfAwareData`` originated from.

2. Create on the fly from a live data object:

>>> my_sad = SelfAwareData(raw_df)

Starting a ``SelfAwareData`` this way will not track how the data originated.

Your data is now accessible via ``my_sad.data``.

Transform
'''''''''

When you apply a transform to your dataset, use the built-in `transform` method to track the transform.

>>> new_sad = my_sad.transform(transform_func)

If you need to specify arguments to your ``transform_func``, do so as keyword arguments in the ``transform`` function:

>>> new_sad = my_sad.transform(transform_func, num_bins=12)

.. note::
    Note on Tracking Git Metadata: To ensure traceability, ``SelfAwareData`` checks that there are no uncommitted changes in the repo before running the transform.
    If there are uncommitted changes, `datatc`` raises a ``RuntimeError``. If you would like to override this check, specify ``enforce_clean_git = False`` in ``transform()``.

.. note::
    If the ``transform_func`` you pass to ``transform()`` is written in a file in a git repo, then `datatc` will include the git hash of the repo where ``transform_func`` is written.
    If the ``transform_func`` is not in a file (for example, is written on the fly in a notebook or in an interactive session),
    the user may specify the module to get a git hash from via ``get_git_hash_from=module``.


`SelfAwareData` objects automatically track their own metadata
.................................................................

By using the `SelfAwareData.transform` method, metadata about the transformation is automatically tracked, including:

* the timestamp of when the transformation was run
* the git hash of the repo where ``transform_func`` is located
* the code of the transform used to transform the data
* the arguments to the transform function

To access metadata, you can print the transform steps:

>>> new_sad.print_steps()
--------------------------------------------------------------------------------
Step  0               2021-01-27 21:46          no_git_hash
--------------------------------------------------------------------------------
def transform_step_1(input_df):
    df = input_df.copy()
    df['col_1'] = df['col_1'] * -1
    return df
--------------------------------------------------------------------------------
Step  1               2021-01-27 21:46          no_git_hash
--------------------------------------------------------------------------------
def transform_step_2(input_df):
    df = input_df.copy()
    df['col_2'] = df['col_2']**2
    return df

To access the data programatically, use ``SelfAwareData.get_info()``:

>>> new_sad.get_info()
[
    {
        'timestamp': '2021-01-27_21-46-52',
      'tag': '',
      'code': "def transform_step_1(input_df):\n    df = input_df.copy()\n    df['col_1'] = df['col_1'] * -1\n    return df\n",
      'kwargs': {},
      'git_hash': 'no_git_hash'
    },
    {
        'timestamp': '2021-01-27_21-46-55',
        'tag': '',
        'code': "def transform_step_2(input_df):\n    df = input_df.copy()\n    df['col_2'] = df['col_2']**2\n    return df\n",
        'kwargs': {},
        'git_hash': 'no_git_hash'
    }
]


Save
''''

There are 2 ways to save ``SelfAwareData`` objects.

1. If you are using ``DataDirectory``, then saving your ``SelfAwareData`` works the same as saving any other file with ``DataDirectory``.

>>> dd['directory'].save(sad, output_file_name)

2. You can also save ``SelfAwareData``, independently, without using ``DataDirectory``.

>>> sad.save(output_file_path)


Load
'''''''

Loading `SelfAwareData` works the same as loading any other data file with ``DataDirectory``.

>>> sad = dd['feature_sets']['my_feature_set.csv'].load()

This load returns you a `SelfAwareData` object. This object contains not only the data you transformed and saved, but also the transformation function itself.

To access the data:

>>> sad.data

To view the code of the data's transformation function:

>>> sad.print_steps()

To rerun the same transformation function on a new data object:

>>> sad.rerun(new_df)

Loading `SelfAwareData` objects without ``DataDirectory``
.......................................................

You can also load ``SelfAwareData`` objects without going through ``DataDirectory``:

>>> sad = SelfAwareData.load(file_path)

However, ``SelfAwareData`` objects are saved to the file system as directories with long names, like ``sad_dir__2021-01-01_12-00__transform_1``.
When you interact with ``SelfAwareData`` via ``DataDirectory``, you can reference them like normal files (``transform_1.csv``), however, referencing them outside of ``DataDirectory`` is not as easy.

Loading `SelfAwareData` objects in dependency-incomplete environments
.............................................................................

If the `SelfAwareData` object is moved to a different environment where the dependencies for the code transform are not met,
use

>>> sad = SelfAwareDataDirectory.load(load_function=False)

to avoid a ``ModuleNotFoundError``.

``SelfAwareData`` Example
'''''''''''''''''''''''''

Here's a toy example of working with ``SelfAwareData``:

.. code-block:: python

    from datatc import DataDirectory, SelfAwareData

    dd = DataDirectory.load('datatc_demo')

    raw_sad = SelfAwareData.load_from_file(dd['raw']['iris.csv'].path)

    def petal_area(df):
        df['petal_area'] = df['petal_length'] * df['petal_width']
        return df

    area_sad = raw_sad.transform(petal_area, 'compute_petal_area')

    dd['processed'].save(area_sad, 'area.csv')

----


Working with File Types via `DataInterface`
------------------------------------------------

`DataInterface` provides a standard interface for interacting with all file types: ``save()`` and ``load()``. This abstracts away the exact saving and loading operations for specific file types.

If you want to work with a file type that `datatc` doesn't know about yet, you can create a `DataInterface` for it:

 1. Create a ``DataInterface`` that subclasses from ``DataInterfaceBase``, and implement the ``_interface_specific_save`` and ``_interface_specific_load`` functions.

 2. Register your new `DataInterface` with `DataInterfaceManager`:

    >>> DataInterfaceManager.register_data_interface(MyNewDataInterface)
