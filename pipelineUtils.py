"""_summary_ = "This module contains functions to perform RML mapping using the RMLMapper JAR file.
Functions:
    - execute_rml(rml_file: str, output_file: str) -> None
    - mapping(rml_dir: str, output_dir: str) -> None
    - upload(rdf_file_path: str, graphdb_url: str, repository: str) -> None
    - upload_rdf_data_to_graphdb(rdf_file_path: str, graphdb_url: str, repository: str) -> None
    - convert_to_iso8601(date_str: str) -> str
    - clean_data(dataset_path: str, output_dir: str) -> pd.DataFrame
    - split_dataset(dataset_path: str, n_chunks: int, output_dir: str) -> None
    - load_config(config_file: str) -> dict
"""

import os
import subprocess
import requests
import pandas as pd
from datetime import datetime
import json
import logging
import re


def execute_rml(
    csv_file: str, rml_file: str, output_file: str, rml_mapper_path: str
) -> None:
    """
    Executes an RML mapping by injecting the CSV file path into the RML file
    and running the RMLMapper Java application.

    Args:
        csv_file (str): The path to the CSV file to be used in the RML mapping.
        rml_file (str): The path to the RML file template containing the mapping rules.
        output_file (str): The path where the output RDF file should be saved.
        rml_mapper_path (str): The path to the RMLMapper JAR file.

    Returns:
        None

    Raises:
        subprocess.CalledProcessError: If the RML mapping process fails.
    """
    # ----------------- ADD CSV PATH TO RML FILE -----------------
    # Read the content of the RML file
    with open(rml_file, "r") as file:
        rml_content = file.read()

    # Replace the placeholder "{csv_file_path}" in the RML content with the actual CSV file path
    rml_content = rml_content.replace("{csv_file_path}", csv_file)

    # Write the modified RML content to a temporary file for processing
    tmp_rml_file = "tmp_mapping.rml.ttl"
    with open(tmp_rml_file, "w") as file:
        file.write(rml_content)

    # ----------------------------------------------------------

    # ----------------- RUN RML MAPPING --------------------------

    # Arguments to pass to the Java command for running the RMLMapper
    args = [
        "-Xms512m",  # Initial Java heap size
        "-Xmx10g",  # Maximum Java heap size
        "-XX:+UseG1GC",  # Use the G1 garbage collector for better memory management
        f"-jar {rml_mapper_path}",  # Specify the path to the RMLMapper JAR file
        f"-m {tmp_rml_file}",  # Specify the RML mapping file to use
        f"-o {output_file}",  # Specify the output file path
    ]

    # Construct the complete Java command to run the RMLMapper
    command = f"java {' '.join(args)}"

    # Run the Java command in a subprocess and capture the output
    try:
        subprocess.run(command, capture_output=True, text=True, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        # Print the error if the command fails
        logging.error(f"Error: {e.returncode}, {e.stderr}")

    # Clean up the temporary RML file after execution
    os.remove(tmp_rml_file)


# ---------------------------------------------------------------------------------------------------------------


def mapping(
    csv_file_path: str, rml_path: str, output_dir: str, mapper_path: str
) -> None:
    """
    Applies RML mapping to either a single CSV file or multiple CSV files within a directory.
    It uses the `execute_rml` function to perform the mapping for each file.

    Args:
        csv_file_path (str): The path to a single CSV file or a directory containing CSV files.
        rml_path (str): The path to the RML file template containing the mapping rules.
        output_dir (str): The directory where the output RDF files should be saved.
        mapper_path (str): The path to the RMLMapper JAR file.

    Returns:
        None

    Raises:
        FileNotFoundError: If the provided CSV file path does not exist.

    Example:
        mapping(
            "../data/tmp/",
            "../data/mapper/mapper.rml.ttl",
            "../data/knowledge-graph/",
            "../rmlmapper.jar",
        )
    """
    # Check if the provided CSV file path is a directory
    if os.path.isdir(rml_path):
        for j, rml_file in enumerate(os.listdir(rml_path)):
            if os.path.isdir(csv_file_path):
                # ---------------------- RML and CSV are folders----------------------
                for i, csv_file in enumerate(os.listdir(csv_file_path)):
                    # Execute the RML mapping for each CSV file in the directory
                    logging.info(f"Processing file: {csv_file}, RML: {rml_file}")
                    execute_rml(
                        f"{csv_file_path}{csv_file}",
                        f"{rml_path}{rml_file}",
                        f"{output_dir}output_{i}_{j}.ttl",
                        mapper_path,
                    )
            # Check if the provided path is a single file
            elif os.path.isfile(csv_file_path):
                # ---------------------- RML is a folder and CSV a file ----------------------
                # Execute the RML mapping for the single CSV file
                execute_rml(
                    csv_file_path, f"{rml_path}{rml_file}", output_dir, mapper_path
                )
            else:
                # If the path is neither a file nor a directory, print an error message
                logging.error("Invalid CSV file path")
    elif os.path.isfile(rml_path):
        # ---------------------- RML is a file and CSV a folder ----------------------

        if os.path.isdir(csv_file_path):
            # ---------------------- RML and CSV are folders----------------------
            for i, csv_file in enumerate(os.listdir(csv_file_path)):
                # Execute the RML mapping for each CSV file in the directory
                logging.info(f"Processing file: {csv_file}, RML: {rml_file}")
                execute_rml(
                    f"{csv_file_path}{csv_file}",
                    rml_path,
                    f"{output_dir}output_{i}.ttl",
                    mapper_path,
                )
        # Check if the provided path is a single file
        elif os.path.isfile(csv_file_path):
            # ---------------------- RML and CSV are files ----------------------
            # Execute the RML mapping for the single CSV file
            execute_rml(csv_file_path, rml_path, output_dir, mapper_path)
        else:
            # If the path is neither a file nor a directory, print an error message
            logging.error("Invalid CSV file path")


# ---------------------------------------------------------------------------------------------------------------


def upload(graphdb_url: str, repository_id: str, rdf_file_path: str) -> None:
    """
    Uploads RDF data in Turtle format to a specific named graph in a GraphDB repository.

    Args:
        graphdb_url (str): The base URL of the GraphDB instance (e.g., http://localhost:7200).
        repository_id (str): The ID of the repository in GraphDB where the data will be uploaded.
        rdf_file_path (str): Path to the Turtle file containing RDF data.
    Returns:
        None
    """
    # Construct the full URL for the POST request
    url = f"{graphdb_url}/repositories/{repository_id}/statements"

    # Set the headers
    headers = {"Content-Type": "text/turtle"}

    # Read the Turtle file
    with open(rdf_file_path, "r") as file:
        rdf_data = file.read()

    # Send the POST request
    response = requests.post(url, headers=headers, data=rdf_data)
    if response.status_code == 204:
        logging.info(
            f"RDF file {rdf_file_path} uploaded to repository {repository_id}."
        )
    else:
        logging.error(
            f"Failed to upload RDF file {rdf_file_path}. Response: {response.text}"
        )


# ---------------------------------------------------------------------------------------------------------------


def upload_rdf_data_to_graphdb(
    rdf_file_path: str, graphdb_url: str, repository: str
) -> None:
    """
    Uploads RDF data from a file or directory to a GraphDB repository.

    Args:
        rdf_file_path (str): Path to a directory containing RDF files or a single RDF file.
        graphdb_url (str): Base URL of the GraphDB server.
        repository (str): Name of the GraphDB repository to which the RDF data should be uploaded.

    Returns:
        None
    """
    # Check if the provided path is a directory
    if os.path.isdir(rdf_file_path):
        for file in os.listdir(rdf_file_path):
            logging.info(f"Uploading file: {file}")
            # Upload each RDF file in the directory
            upload(graphdb_url, repository, os.path.join(rdf_file_path, file))
    # Check if the provided path is a file
    elif os.path.isfile(rdf_file_path):
        # Upload the single RDF file
        upload(graphdb_url, repository, rdf_file_path)
    else:
        # If the path is neither a file nor a directory, print an error message
        logging.error("Invalid file path.")


# ---------------------------------------------------------------------------------------------------------------


def convert_to_iso8601(date_str: str) -> str:
    """
    Convert a date string in the format '%Y-%m-%d %H:%M:%S.%f' to ISO 8601 format.

    Args:
        date_str (str): The date string to convert.

    Returns:
        str: The date string in ISO 8601 format.
    """
    # Parse the input date string to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    # Format the datetime object to the desired ISO 8601 format
    return date_obj.isoformat()


# ---------------------------------------------------------------------------------------------------------------

# Function to remove numbers
def remove_numbers(text):
    return re.sub(r"\d+", "", text)


# ---------------------------------------------------------------------------------------------------------------


def clean_data(dataset_path: str, output_path: str) -> None:
    """
    Cleans the dataset by converting dates to ISO 8601 format, replacing specific unit values, and standardizing quality descriptions.

    Args:
        dataset_path (str): The path to the CSV dataset file to clean.
        output_dir (str): The directory where the cleaned dataset should be saved.

    Returns:
        pd.DataFrame: The cleaned dataset.
    """
    # Load the dataset
    data = pd.read_csv(dataset_path, low_memory=False)

    # Convert the 'data_rilevazione' column to ISO 8601 format
    data["data_rilevazione"] = data["data_rilevazione"].apply(convert_to_iso8601)

    # Replace "-" with "Dimensionless" in the 'unit' column
    data["unit"] = data["unit"].replace("-", "Dimensionless")

    # Standardize the quality descriptions
    data["quality"] = data["quality"].replace("Qualità della misura: ", "", regex=True)

    # Apply function to DataFrame column
    data["property"] = data["register_name"].apply(remove_numbers)
    data["property"] = data["property"].str.replace(
        r"^Current.*", "Current", regex=True
    )

    # Display the DataFrame
    data["property"].unique()

    # Save the cleaned dataset to the specified output directory
    data.to_csv(output_path, index=False)


# ---------------------------------------------------------------------------------------------------------------


def split_dataset(dataset_path: str, n_chunks: int, output_dir: str) -> None:
    """
    Splits a large dataset into multiple smaller chunks and saves them as separate CSV files.

    Args:
        dataset_path (str): The path to the CSV dataset file to split.
        n_chunks (int): The number of chunks to split the dataset into.
        output_dir (str): The directory where the chunked files should be saved.

    Returns:
        None
    """
    # Load the dataset
    data = pd.read_csv(dataset_path, low_memory=False)

    # Calculate the size of each chunk
    chunk_size = len(data) // n_chunks

    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Split and save the chunks
    for i in range(n_chunks):
        start_index = i * chunk_size
        # If it's the last chunk, include all remaining rows
        if i == n_chunks - 1:
            chunk = data.iloc[start_index:]
        else:
            chunk = data.iloc[start_index : start_index + chunk_size]

        # Save each chunk as a separate CSV file
        chunk.to_csv(os.path.join(output_dir, f"data_chunk_{i}.csv"), index=False)


# ---------------------------------------------------------------------------------------------------------------


def load_config(config_file: str) -> dict:
    """
    Loads a JSON configuration file.

    Args:
        config_file (str): The path to the JSON configuration file.

    Returns:
        dict: A dictionary containing the configuration settings.
    """
    try:
        with open(config_file, "r") as file:
            config = json.load(file)
            return config
    except FileNotFoundError:
        logging.error(f"Error: The file {config_file} was not found.")
    except json.JSONDecodeError as e:
        logging.error(f"Error: Failed to decode JSON from {config_file}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
