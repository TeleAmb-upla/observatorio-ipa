import os
import argparse


# TODO: See if we can enable option to log-in with regular user (not service user)
def parse_cli_args():
    """
    Creates an argument parser for the command line interface.

    Returns:
        argparse.ArgumentParser: An argument parser object.
    """
    # Create the parser
    parser = argparse.ArgumentParser(description="OSN Image Processing Automation")

    # Source of environment variables
    parser.add_argument(
        "-t",
        "--toml-file",
        dest="toml_file",
        default=os.getenv("IPA_CONFIG_TOML", ""),
        type=str,
        help="Path to .toml file containing configuration parameters.",
    )

    args = parser.parse_args()

    return vars(args)
