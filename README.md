# Knowledge Graph Creation Pipeline

The `pipeline.py` script is designed to transform a CSV file into a knowledge graph and upload it to a GraphDB repository. This process includes data cleaning, dataset splitting, RDF mapping, and finally uploading the RDF data to a specified repository.

## Overview of Operations

1. **Data Cleaning**:

   - **Date Splitting**: Converts date fields to the ISO 8601 standard.
   - **Unit Normalization**: Replaces missing or dimensionless values in the `unit` column with "Dimensionless".
   - **Quality Formatting**: Standardizes entries in the `quality` column.

2. **Dataset Splitting**:

   - Large CSV files are split into smaller chunks to ensure successful processing and uploading. This is particularly important for handling large datasets that may cause failures during subsequent stages.

3. **RDF Mapping**:

   - Utilizes the RML (RDF Mapping Language) to convert the cleaned CSV data into RDF format according to the specified RML mapping rules.

4. **Uploading to GraphDB**:
   - The generated RDF files are uploaded to the specified GraphDB repository, making the knowledge graph accessible for querying and further analysis.

## Configuration File

Before running the script, create a configuration file in JSON format. This file defines the paths and settings for each stage of the pipeline:

```json
{
  "clean_data": {
    "input": "path/to/input_csv_file.csv",
    "output": "path/to/cleaned_csv_file.csv"
  },
  "split_dataset": {
    "dataset_path": "path/to/cleaned_csv_file.csv",
    "n_chunks": 3,
    "output_dir": "path/to/chunks_directory/"
  },
  "mapping": {
    "rml_path": "path/to/rml_mapping_file.ttl",
    "output_path": "path/to/rdf_output_directory/",
    "mapper_path": "path/to/rml_mapper.jar"
  },
  "upload_to_graphDB": {
    "graphDB_url": "http://localhost:7200",
    "graphDB_repo": "repository_name"
  }
}
```

### Configuration Details

- **`clean_data.input`**: Path to the raw CSV file that needs to be cleaned.
- **`clean_data.output`**: Path where the cleaned CSV file will be saved.
- **`split_dataset.dataset_path`**: Path to the cleaned CSV file for splitting.
- **`split_dataset.n_chunks`**: Number of chunks to split the dataset into.
- **`split_dataset.output_dir`**: Directory where the split CSV chunks will be stored.
- **`mapping.rml_path`**: Path to the RML mapping file, in case of multiple RML files, it could be the directory that contains them.
- **`mapping.output_path`**: Directory where the generated RDF files will be saved.
- **`mapping.mapper_path`**: Path to the RMLMapper JAR file.
- **`upload_to_graphDB.graphDB_url`**: URL of the GraphDB instance.
- **`upload_to_graphDB.graphDB_repo`**: Name of the GraphDB repository where the data will be uploaded.

### Important Notes

- Ensure the directories specified in `split_dataset.output_dir` and `mapping.output_path` only contain the relevant CSV chunks and RDF files, respectively. Including other files might cause unexpected behavior or errors during processing.
- Download the RMLMapper JAR file from [RML.io](https://github.com/RMLio/rmlmapper-java/releases/download/v7.0.0/rmlmapper-7.0.0-r374-all.jar) and specify its path in the `mapper_path` field.

## Running the Script

To execute the pipeline, use the following command:

```bash
python pipeline.py -a -s path/to/config_file.json
```

The `-a` flag indicates that all stages of the pipeline should be executed. If you want to run a specific stage, you can use the following flags:

- `-c` for data cleaning
- `-d` for dataset splitting
- `-m` for RDF mapping
- `-u` for uploading to GraphDB
- `-h` for help

For example, to run only the data cleaning stage, use the following command:

```bash
python pipeline.py -c -s path/to/config_file.json
```

Replace `path/to/config_file.json` with the actual path to your configuration file.

## Customization

The data cleaning operations provided in this script are tailored to a specific dataset. If your dataset requires different or additional cleaning steps, you'll need to customize the `clean_data` function located in the `utils.py` file.
