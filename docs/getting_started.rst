Getting Started
===============


Register a project with `DataManager`
-------------------------------------
`datatc` will remember for you where the data for each of your projects is stored, so that you don't have to. The first time you use `datatc` with a project, register it with :class:`datatc.data_manager.DataManager`. You only have to do this once- ``DataManager`` saves the information in ``~/.data_map.yaml`` so you never have to memorize that long file path again.

>>> DataManager.register_project('project_name', '/path/to/project/data/dir/')

Example:

>>> DataManager.register_project('mridle', '/home/user/data/mridle/data')

Once you've registered a project, you can establish a `DataManager` on the fly by referencing it's name.

>>> dm = DataManager('mridle')

The `DataManager` object, ``dm``, will be your gateway to all file discovery, load, and save operations.


----


Explore the data directory
--------------------------

`DataManager` makes it easier to interact with your project's data directory. With ``ls()``, you can print out the file structure:

>>> dm.ls()
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

>>> dm['data_extracts'].ls(full=True)
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

>>> raw_df = dm['data_extracts']['2020-02-04_Extract_3months']['2020-02-04_Extract_3months.xlsx'].load()


Don't worry about what format the file is in- `DataManager` will intuit how to load the file.


Shortcuts for loading data files *faster*
''''''''''''''''''''''''''''''''''''''''''

latest
..........
If you use timestamps to version your data files, you can load the latest file or subdirectory within a directory with ``.latest()``:

>>> raw_df = dm['data_extracts'].latest()['2020-02-04_Extract_3months.xlsx'].load()

select
..........

To help you navigate those long finicky file names, `DataManager` provides a ``.select('hint')`` method to search for files matching a substring.

>>> raw_df = dm['data_extracts']['2020-02-04_Extract_3months'].select('xlsx').load()

Combing *latest* and *select*, the file load in the previous can be reduced to the following:

>>> raw_df = dm['data_extracts'].latest().select('xlsx').load()

Loading irregular data files
''''''''''''''''''''''''''''''''''''''''''

... my file needs special arguments to load
................................................
If your file needs special parameters to load it, specify them in ``load``, and they will be passed on to the internal loading function.
For example, if your csv file is actually pipe separated and has a non-default encoding, you can specify so:

>>> raw_df = dm['queries']['batch_query.csv'].load(sep='|', encoding='utf-16')

... my file type isn't recognized by `datatc`
................................................
If `DataManager` doesn't recognize the file type, you can give it a type hint of which loader to use. For example, `DataManager` doesn't have a specific interface for reading tab separated files, but if you tell it to treat it as a csv, it will load it right up:

>>> raw_df = dm['queries']['batch_query.tsv'].load(data_interface_hint='csv')

... I want to load my file my own way
................................................
If you ever want to do your own load, and not use the build in ``.load()``, you can also use ``dm[...]['filename'].path`` to get the path to the file for use in a separate loading operation.


----


Saving data files
------------------

To save a file, navigate with ``dm`` to the position in the file system where you'd like to save your file using the ``[]`` operators, and then call ``.save(data_object, file_name)``.

For example:

.. code-block:: python

    dm['processed_data'].save(processed_df, 'processed.csv')



----


Saving and Loading `SelfAwareData`
--------------------------------------------
Save
'''''''
`datatc` helps you remember how your datasets were generated.

Anytime you want `datatc` to help keep track of what transform function was used to create a dataset, pass that transform function and the input data to ``.save()``, like this:

.. code-block:: python

    .save(input_data, output_file, transform_func)

`datatc` will run the transform func on your input data, and save not only the resulting dataset, but also metadata about how the dataset was generated.

 Here's a toy example:

.. code-block:: python

    def my_transform(df):
        df['new_feature'] = df['input_column'] * 2
        return df

    dm['feature_sets'].save(input_df, 'my_feature_set.csv', my_transform)


This uses `datatc`'s `SelfAwareData` functionality to save a dataset *and* the code that generated it.

This line of code:
  * consumes `input_df`
  * applies `my_transform`
  * saves the result as `my_feature_set.csv`
  * also stamps the code contained in `my_transform` alongside the dataset for easy future reference


`SelfAwareData` objects automatically track their own metadata
.................................................................

`datatc` also automatically tracks metadata about the data transformation, including:
* the timestamp of when the transformation was run
* the git hash of the repo where ``transform_func`` is located

This metadata is visible when you ``ls`` a directory containing transformed data files:

>>> dm['feature_sets'].ls()
feature_sets/
    four_features.csv   (2020-04-24 17:40, 06ef971)
    seven_features.csv  (2020-06-17 18:37, c61a1a6)
    nine_features.csv   (2020-08-19 18:42, 25a173d)

And you can access the metadata programmatically:

>>> dm['feature_sets'].latest().get_info()
{
    'timestamp': '2020-08-19 18:42',
    'git_hash': '25a173d',
    'tag': 'nine_features',
    'data_type': 'csv'
}


Note on Tracking Git Metadata
................................
By default, when you save a transformed dataset via a ``transform_func``, `datatc` will include the git hash of the repo where ``transform_func`` is located.
This workflow assumes that the ``transform_func``` is written in a file and imported into the active coding environment for use in creating a ``SelfAwareData`` object.
If the ``transform_func`` is not in a file (for example, is written on the fly in a notebook or in an interactive session),
the user may specify the module under development to get a git hash from via ``get_git_hash_from=module``.

To ensure traceability, `datatc` checks that there are no uncommitted changes in the repo before proceeding with creating the `SelfAwareData`.
If there are uncommitted changes, `datatc`` raises a ``RuntimeError``. If you would like to override this check, specify ``enforce_clean_git = False``.


Load
'''''''

Loading `SelfAwareData` works the same as loading any other data file with DataManager.

>>> td = dm['feature_sets']['my_feature_set.csv'].load()

This load returns you a `SelfAwareData` object. This object contains not only the data you transformed and saved, but also the transformation function itself.

To access the data:

>>> td.data

To view the code of the data's transformation function:

>>> td.view_code()

To rerun the same transformation function on a new data object:

>>> td.rerun(new_df)


Loading `SelfAwareData` objects in dependency-incomplete environments
.............................................................................

If the `SelfAwareData` object is moved to a different environment where the dependencies for the code transform are not met,
use

>>> td = SelfAwareDataDirectory.load(load_function=False)

to avoid a ``ModuleNotFoundError``.


----


Working with File Types via `DataInterface`
------------------------------------------------

`DataInterface` provides a standard interface for interacting with all file types: ``save()`` and ``load()``. This abstracts away the exact saving and loading operations for specific file types.

If you want to work with a file type that `datatc` doesn't know about yet, you can create a `DataInterface` for it:

 1. Create a ``DataInterface`` that subclasses from ``DataInterfaceBase``, and implement the ``_interface_specific_save`` and ``_interface_specific_load`` functions.

 2. Register your new `DataInterface` with `DataInterfaceManager`:

    >>> DataInterfaceManager.register_data_interface(MyNewDataInterface)
