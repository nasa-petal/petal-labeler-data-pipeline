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
    dataframes = [pd.read_csv(path) for path in csv_paths]
    merged = merge_dataframes(dataframes)
    merged.to_csv("merged_dataframes.csv", index=False)
    for path in csv_paths:
        os.remove(path)