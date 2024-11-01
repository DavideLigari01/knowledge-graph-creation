import logging
import argparse
from pipelineUtils import (
    load_config,
    clean_data,
    split_dataset,
    mapping,
    upload_rdf_data_to_graphdb,
)


def main(config_path: str, clean: bool, split: bool, map: bool, upload: bool):
    """
    Main function to run the data processing steps.

    Args:
        config_path (str): The path to the JSON configuration file.
    """
    # Load configuration
    config = load_config(config_path)
    if not config:
        logging.error("Failed to load configuration. Exiting.")
        return
    if clean:
        # Step 1: Clean the data
        logging.info("Cleaning the data ...")
        clean_data(config["clean_data"]["input"], config["clean_data"]["output"])

    if split:
        # Step 2: Split the dataset
        logging.info("Splitting the dataset ...")
        split_dataset(
            config["split_dataset"]["dataset_path"],
            config["split_dataset"]["n_chunks"],
            config["split_dataset"]["output_dir"],
        )
    if map:
        # Step 3: Perform RML Mapping
        logging.info("Mapping the data ...")
        mapping(
            config["split_dataset"]["output_dir"],
            config["mapping"]["rml_path"],
            config["mapping"]["output_path"],
            config["mapping"]["mapper_path"],
        )
    if upload:
        # Step 4: Upload RDF Data to GraphDB
        logging.info("Uploading RDF data to GraphDB ...")
        upload_rdf_data_to_graphdb(
            config["mapping"]["output_path"],
            config["upload_to_graphDB"]["graphDB_url"],
            config["upload_to_graphDB"]["graphDB_repo"],
        )


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Run data processing pipeline with specified config."
    )

    # Add argument for the configuration file path
    parser.add_argument(
        "-s",
        "--settings",
        type=str,
        required=True,
        help="Path to the JSON configuration file.",
    )
    parser.add_argument(
        "-",
        "--clean",
        action="store_true",
        help="Clean the data.",
    )

    parser.add_argument(
        "-p",
        "--split",
        action="store_true",
        help="split the data.",
    )
    parser.add_argument(
        "-m",
        "--mapping",
        action="store_true",
        help="perform the RML mapping the data.",
    )
    parser.add_argument(
        "-u",
        "--upload",
        action="store_true",
        help="upload the data to GraphDB.",
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Perform all the steps.",
    )

    # Parse the arguments
    args = parser.parse_args()

    if args.all:
        # Run the main function with the provided configuration file path
        main(args.settings, True, True, True, True)
    else:
        main(args.settings, args.clean, args.split, args.mapping, args.upload)
