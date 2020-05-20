# data-traffic-control
Whhrrrr... Voooooosh... That's the sound of your data coming and going exactly where it belongs

## Usage

### Installation

Clone this repo, `cd` into the newly created `data-traffic-control` directory, and run `pip install -e .` to install the `datatc` package.

### Import

```python
from datatc import DataManager
```

### Register a project
`datatc` will remember for you where the data for each of your projects is stored, so that you don't have to. The first time you use `datatc` with a project, register it with `DataManager`. You only have to do this once- `DataManager` saves the information in `~/.data_map.yaml` so you never have to memorize that long file path again. 

```python
DataManager.register_project('project_name', '/path/to/project/data/dir/')
```

Example:

```python
DataManager.register_project('mridle', '/home/user/data/mridle/data')
```

### `DataManager`

Once you've registered a project, you can establish a `DataManager` on the fly by referencing it's name.

```python
dm = DataManager('mridle')
```

`DataManager` makes it easier to interact with your project's data directory. It can print out the file structure:

```python
# this will default to give a summary
dm.ls()

# raw/
#     EntityViews/
#         7 mixed items
#     data_extracts/
#         2019-11-23_Extract_3days/
#             1 mixed items
#         2020-01-05_Extract_3days_withHistory_v2/
#             2 mixed items
#         2020-02-04_Extract_3months/
#             3 mixed items
#     db-connection.R
#     query.sql
```

You can also print the contents of a subdirectory:
```python
dm['data_extracts'].ls(full=True)

# data_extracts/
#     2019-11-23_Extract_3days/
#         2019-11-23_Extract_3days.xlsx
#     2020-01-05_Extract_3days_withHistory_v2/
#         2020-01-05_Extract_3days_withHistory_v2.xlsx
#         2020-01-05_Extract_3days_withHistory_v2_sql.sql
#     2020-02-04_Extract_3months/
#         2020-02-04_Extract_3months.sql
#         2020-02-04_Extract_3months.xlsx
#         3_month_export.csv
```


### Loading data files

To load a file, navigate the file system using `[]` operators, and then call `.load()`. 

```python
raw_df = dm['data_extracts']['2020-02-04_Extract_3months']['2020-02-04_Extract_3months.xlsx'].load()
```

Don't worry about what format the file is in- `DataManager` will inutit how to load the file. 

If `DataManager` doesn't recognize the file type, you can give it a type hint of which loader to use. For example, `DataManager` doesn't have a specific interface for reading sql files, but if you tell it to treat it as a txt, it will load it right up:

```python
raw_df = dm['queries']['batch_query.sql'].load(data_interface_hint='txt')
```

To help you navigate those long finicky file names, `DataManager` provides a `.select('hint')` method to search for files matching a substring. 

The above example could also be accessed with the following command, which navigates to the latest extract directory and selects the xlsx file:

```python
raw_df = dm['data_extracts']['2020-02-04_Extract_3months'].select('xlsx').load()
```

or even:

```python
raw_df = dm['data_extracts'].select('2020-01-13').select('xlsx').load()
```

You can load the latest file or subdirectory within a directory with `.latest()`:
```python
raw_df = dm['data_extracts'].latest().select('xlsx').load()
```

If you ever want to do your own load, and not use the build in `.load()`, you can also use `dm[...]['filename'].path` to get the path to the file for use in a separate loading operation.


## Saving and Loading SavedDataTransforms
`datatc` helps you remember how your datasets were generated. 
Anytime you want `datatc` to help keep track of what transform function was used to create a dataset,
pass that transform function and the input data to `.save()`, like this: 

`.save(input_data, output_file, transform_func)`.

`datatc` will run the transform func on your input data, and save not only the resulting dataset,
 but also metadata about how the dataset was generated. 
 
 Here's a toy example:

```python
def my_transform(df):
    df['new_feature'] = df['input_column'] * 2
    return df

dm['feature_sets'].save(input_df, 'my_feature_set.csv', my_transform)
```
This uses `datatc`'s Saved Data Transform functionality to save a dataset and the code that generated it.
This line of code:
  * consumes `input_df`
  * applies `my_transform`
  * saves the result as `my_feature_set.csv`
  * also stamps the code contained in `my_transform` alongside the dataset for easy future reference

Afterwards, you can not only load the dataset, but also view and re-use the code that generated that dataset:
```python
sdt = dm['feature_sets'].latest().load()

# view and use the data
sdt.data.head()

# view the code that generated the dataset
sdt.view_code()

# rerun the transform on a new dataset
more_transformed_df = sdt.rerun(new_df)
```

If the Saved Data Transform is moved to a different environment where the dependencies for the code transform are not met,
use

    sdt = TransformedDataDirectory.load(load_function=False)

to avoid a `ModuleNotFoundError`.


## Working with File Types via `DataInterface`

`DataInterface` provides a standard interface for interacting with all file types: `save()` and `load()`. This abstracts away the exact saving and loading operations for specific file types.

If you want to work with a file type that `datatc` doesn't know about yet, you can create a `DataInterface` for it. In `datatc/data_interface`:

 1. Create a `DataInterface` that subclasses from `DataInterfaceBase`, and implement the `_interface_specific_save` and `_interface_specific_load` functions.
 1. Register your new `DataInterface` with `DataInterfaceManager` by adding it to the `DataInterfaceManager.registered_interfaces` dictionary.
