import argparse

def get_args():
    """Allows arguments to be passed into this program through the terminal.
    Returns:
        arguments: Object containing selected responses to given options
    """

    parser = argparse.ArgumentParser(
        description="Does something with MTurk data.")
    parser.add_argument("output_file", type=str,
                        help="Name of output CSV file")
    args = parser.parse_args()
    return args

if (__name__ == "__main__"):
    args = get_args()


