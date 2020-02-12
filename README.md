# data-traffic-control
Whhrrrr... Voooooosh... That's the sound of your data coming and going exactly where it belongs

## Usage

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
DataManager.register_project('mridle', '/opt/data/radiology_resource_allocation/raw')
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
#     MR_Termine_CIP_EntityViews/
#         7 mixed items
#     dicom_examples/
#         1 mixed items
#     rdsc_extracts/
#         2019-12-24_RIS_Extract_3days_withHistory/
#             1 mixed items
#         2020-01-13_RIS_Extract_3days_withHistory_v2/
#             2 mixed items
#         2020-02-04_RIS_deID_3months/
#             3 mixed items
#     2019-07-10_RIS-required-fields-for-resource-allocation.xlsx
#     GE_query-module_de.pdf
#     GE_query-module_en.pdf
#     RIS_Extract_top20000_with_headers.xlsx
#     Radiologie-Dashboards_Dokumentation.docx
#     db-connection.R
#     query_by_marc_bovet.sql
```

You can also print the contents of a subdirectory:
```python
dm['rdsc_extracts'].ls(full=True)

# rdsc_extracts/
#     2019-12-24_RIS_Extract_3days_withHistory/
#         2019-12-24_RIS_Extract_3days_withHistory.xlsx
#     2020-01-13_RIS_Extract_3days_withHistory_v2/
#         2020-01-13_RIS_Extract_3days_withHistory_v2.xlsx
#         2020-01-13_RIS_Extract_3days_withHistory_v2_sql.sql
#     2020-02-04_RIS_deID_3months/
#         2020-02-04_RIS_deID_3months.sql
#         2020-02-04_RIS_deID_3months.xlsx
#         3_month_export.csv
```


### Loading data files

To load a file, navigate the file system using `[]` operators, and then call `.load()`. 

```python
raw_df = dm['rdsc_extracts']['2020-01-13_RIS_Extract_3days_withHistory_v2']['2020-01-13_RIS_Extract_3days_withHistory_v2.xlsx'].load()
```

Don't worry about what format the file is in- `DataManager` will inutit how to load the file. 

To help you navigate those long finicky file names, `DataManager` provides a `.select('hint')` method to search for files matching a substring. 

The above example could also be access with the following command, which navigates to the extract directory and selects the xlsx file:

```python
raw_df = dm['rdsc_extracts']['2020-01-13_RIS_Extract_3days_withHistory_v2'].select('xlsx').load()

```

or even:

```python
raw_df = dm['rdsc_extracts'].select('2020-01-13').select('xlsx').load()

```
