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
        "-e",
        "--env-file",
        dest="env_file",
        default=os.getenv("IPA_ENV_FILE", ".env"),
        type=str,
        help="Path to .env file containing configuration parameters.",
    )

    args = parser.parse_args()

    return vars(args)
