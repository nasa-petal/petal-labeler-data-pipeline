import pandas as pd
import argparse
import json
import os

from pandas.core.reshape.merge import merge

def get_arg_parser():
    """Allows arguments to be passed into this program through the terminal.
    Returns:
        argparse.Namespace: Object containing selected options
    """

    def json_path(string):
        if str(string).startswith('https://raw.githubusercontent.com/nasa-petal/'):
            return string
        elif os.path.isfile(string + ".json"):
            return string
        else:
            raise NotADirectoryError(string)

    parser = argparse.ArgumentParser(description="Input document file paths")
    parser.add_argument(
        "golden_path", help="Full path to golden JSON file", type=json_path)
    parser.add_argument(
        "new_file_path", help="Full path to new JSON data", type=json_path)
    parser.add_argument("output_name", help="Name of output file", type=str)

    return parser.parse_args()

def merge_data(new_json: pd.DataFrame, golden: pd.DataFrame):
    """ Will update a row if the assigned petalID already exists within the dataset, or it will add new rows.
    Args:
        new_json : pd.DataFrame
            DataFrame containing records from the newly created JSON file we wish to add.
        golden : pd.DataFrame
            DataFrame containing records from the original golden JSON file.
    Returns:
        pd.DataFrame
            DataFrame containing merged records from the new_json file and the golden DataFrame.
    """

    df_columns = golden.columns
    new_rows = []
    petalIDList = golden["petalID"].to_list()
    for index, row in new_json.iterrows():
        petalID = row.get('petalID', None)
        if petalID in petalIDList:
            # Update existing data
            # get index
            indices = golden[golden["petalID"] == petalID].index.values
            if len(indices):
                found_index = indices[0]
                for series_key in row.keys():
                    print(row[series_key])
                    if series_key in df_columns and len(row[series_key]):
                        golden.at[found_index, series_key] = row[series_key]

        else:
            # Add new row 
            new_rows.append(row)
    if len(new_rows):
        current_index = golden["petalID"].max() + 1
        for new_row in new_rows:
            new_row["petalID"] = current_index
            current_index += 1
        golden = pd.concat([golden, pd.DataFrame(new_rows)], ignore_index=True)
        
    return golden


if __name__ == "__main__":
    args = get_arg_parser()
    try:
        golden = pd.read_json(args.golden_path + ".json")
        new_file = pd.read_json(args.new_file_path + ".json")
    except:
        print("Failed to load files")
        raise

    new_golden = merge_data(new_file, golden)
        
    with open(f"{args.output_name}.json", "w") as golden_file:
        golden_file.write("[\n")
        golden_size = new_golden.shape[0]

        for index, row in new_golden.iterrows():
            golden_file.write("\t")
            golden_file.write(json.dumps(row.to_dict()))

            if(index < golden_size - 1):
                golden_file.write(",\n")

        golden_file.write("\n]")