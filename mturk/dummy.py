import pandas as pd
def get_args():
    """Allows arguments to be passed into this program through the terminal.
    Returns:
        arguments: Object containing selected responses to given options
    """

    parser = argparse.ArgumentParser(
        description="Download all biomimicry papers from AskNature modified this month.")
    parser.add_argument("output_file", type=str,
                        help="Name of output CSV file")
    parser.add_argument("app_id", type=str,
                        help="Algolia App ID")
    parser.add_argument("api_key", type=str,
                        help="API Key for Algolia")
    args = parser.parse_args()
    return args

if (__name__ == "__main__"):
    args = get_args()


