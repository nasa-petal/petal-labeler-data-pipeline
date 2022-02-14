import pandas as pd
import os
import glob


def merge_dataframes(dataframes: list):
    """Merges a list of DataFrames together.
    Args:
        dataframes : list
            Path + filename of the function map CSV.
    Returns:
        pd.DataFrame
            A DataFrame created by merging all of the input DataFrames.
    """

    merged = pd.concat(dataframes, ignore_index= True)
    return merged

if __name__ == "__main__":
    if os.path.isfile("merged_dataframes.csv"):
        os.remove("merged_dataframes.csv")
    csv_paths = glob.glob("*.csv")
    print("Files merged: ", csv_paths)
    dataframes = [pd.read_csv(path, encoding="utf8") for path in csv_paths]
    json_paths = glob.glob("*.json")
    print("Files merged: ", json_paths)
    dataframes.extend([pd.read_json(path) for path in json_paths])
    merged = merge_dataframes(dataframes)
    merged.to_csv("merged_dataframes.csv", index=False)
    for path in csv_paths:
        os.remove(path)
    for path in json_paths:
        os.remove(path)