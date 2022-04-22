import pandas as pd
import re
import nltk
import string
import json
import requests
import traceback
import os
import argparse
import ast

# Global Variables
stopwords = nltk.corpus.stopwords.words('english')
special_characters = string.punctuation


def get_arg_parser():
    """Allows arguments to be passed into this program through the terminal.
    Returns:
        argparse.Namespace: Object containing selected options
    """

    def dir_path(string):
        if os.path.isfile(string):
            return string
        else:
            raise NotADirectoryError(string)

    parser = argparse.ArgumentParser(description="Input document file paths")
    parser.add_argument(
        "csv_path", help="Full path to CSV labeled file", type=dir_path)
    parser.add_argument("output_name", help="Name of output file", type=str)
    return parser.parse_args()


def clean_labels(labels: list):
    """Reformats labels into more model friendly formats.
    Args:
        labels : list
            A formatted list of biomimicry labels.
    Returns:
        list
            List of properly formatted labels.
    """

    clean_labels = [re.sub("\s", "_", label).lower()
                    for label in labels]
    return clean_labels if clean_labels != "" else []


def build_abstract(inverted_ind:dict):
    # Find length
    maxInd = 0
    
    for indices in inverted_ind.values():
        if max(indices) > maxInd:
            maxInd = max(indices)
    abstract_list = ["-"]*(maxInd + 1)
    
    for word in inverted_ind.keys():
        for index in inverted_ind[word]:
            abstract_list[index] = word
    full_abstract = " ".join(abstract_list)
    full_abstract = re.sub(" -(.*)- ", " ", full_abstract)

    return " ".join(abstract_list)


def get_api_data(dataframe: pd.DataFrame):
    """Uses DOIs from the imported DataFrame to make queries to Open ALex.
    Args:
        dataframe : pd.DataFrame
            Dataframe of our labeled data.
    Returns:
        Tuple
            list
                A list consisting of the papers pulled from API in a JSON format.
            list
                A list consisting of only the papers DOIs.
    """

    # Define GET parameters
    url = "https://api.openalex.org/works/doi:"
    urlID = "https://api.openalex.org/works/"

    # Make DOI requests in batches
    paper_dois = dataframe["doi"].tolist()
    valid_dois = []
    api_res = []
    total_size = len(paper_dois)
    for i in range(total_size):
        print("Progress: {0:0.2%}".format(i/total_size))
        if (paper_dois[i] == ""):
            continue
        
        if (len(dataframe.iloc[i].get("paper", "")) > 0):
            r = requests.get(urlID + dataframe.iloc[i]["paper"])
        else:
            r = requests.get(url + paper_dois[i])

        if (r.status_code == 200):
            temp_response = json.loads(r.text)
            found_doi = extract_dois(temp_response.get("doi", ""))
            dataframe.at[i, "doi"] = found_doi
            if (len(found_doi) > 0):
                valid_dois.append(found_doi)
            api_res.append(temp_response)

    return (api_res, valid_dois)


def extract_oa_id(urlID: str):
    if (type(urlID) != str):
        return ""
    return re.search(r"(?<=https://openalex.org/).*", urlID).group()


def parse_list(key_val: str, row: list):
    if (row.get(key_val, False)):
        if (type(row[key_val]) == str):
            return eval(row[key_val])
        else:
            return row[key_val]
    else:
        return []


def convert_to_json(dataframe: pd.DataFrame, api_res: list, api_dois: list):
    """ Creates a list of json objects by merging API data with our own dataframe.
    Args:
        dataframe : pd.DataFrame
            Dataframe of our labeled data.
        api_res : list
            A list of API papers each in JSON format.
        api_dois : list
            A list of API dois.
    Returns:
        list
            List of objects containing our labeled data merged with API data.
    """

    # Define list for newly formatted data
    golden_jsons = []

    # Convert dataframe to json format
    for index, row in dataframe.iterrows():
        try:
            api_index = api_dois.index(
                row["doi"].upper()) if row["doi"].upper() in api_dois else -1
            api_paper = (api_index >= 0 and api_res[api_index]) or {}
            temp_dict = {}

            if (api_index >= 0):
                temp_dict["paper"] = extract_oa_id(api_paper["id"])
                
                # Mesh Terms
                if (len(api_paper["mesh"])):
                    temp_dict["mesh_terms"] = [mesh["descriptor_name"] for mesh
                        in api_paper["mesh"]]
                else:
                    temp_dict["mesh_terms"] = []

                # Venue IDs + Names
                if (api_paper.get("host_venue", False)):
                    # Venues
                    temp_ids = []
                    temp_names = []
                    host_id = extract_oa_id(api_paper["host_venue"]["id"])
                    host_name = api_paper["host_venue"]["display_name"]
                    for alt_ven_info in api_paper["alternate_host_venues"]:
                        if (alt_ven_info["id"]):
                            temp_ids.append(alt_ven_info["id"])
                        if (alt_ven_info["display_name"]):
                            temp_names.append(alt_ven_info["display_name"])

                    temp_ids.append(host_id)
                    temp_names.append(host_name)
                    temp_dict["venue_ids"] = temp_ids
                    temp_dict["venue_names"] = temp_names

                else:
                    temp_dict["venue_ids"] = []
                    temp_dict["venue_names"] = []


                # Author IDs + Names
                if (api_paper.get("authorships", False)):
                    temp_auth_names = []
                    temp_auth_ids = []
                    for author in api_paper["authorships"]:
                        temp_auth_ids.append(extract_oa_id(author["author"]["id"]))
                        temp_auth_names.append(author["author"]["display_name"])



                    temp_dict["author_ids"] = temp_auth_ids
                    temp_dict["author_names"] = temp_auth_names
                else:
                    temp_dict["author_ids"] = []
                    temp_dict["author_names"] = []

                # Reference IDs
                if (api_paper["referenced_works"] and len(api_paper["referenced_works"]) > 0):
                    temp_dict["reference_ids"] = [extract_oa_id(ref) for ref in api_paper["referenced_works"]]
                else:
                    temp_dict["reference_ids"] = []

                # Title + Abstract
                temp_dict["title"] = api_paper.get("title", "")
                if (api_paper["abstract_inverted_index"]):
                    temp_dict["abstract"] = build_abstract(api_paper["abstract_inverted_index"])
                else:
                    temp_dict["abstract"] = ""

                # Open Access
                temp_dict["isOpenAccess"] = bool(api_paper["open_access"]["is_oa"])
                # Full Doc Link
                temp_dict["fullDocLink"] = api_paper["open_access"]["oa_url"]
                
            else:            
                temp_dict["paper"] = row.get("paper", "")
                temp_dict["mesh_terms"] = row.get("mesh_terms", [])
                temp_dict["venue_names"] = []
                temp_dict["venue_ids"] = []
                temp_dict["author_names"] = row.get("author_names", [])
                temp_dict["author_ids"] = []
                temp_dict["reference_ids"] = []
                temp_dict["abstract"] = row["abstract"]
                temp_dict["title"] = row["title"]

            if (isinstance(row["petalID"],(int,float))):
                temp_dict["petalID"] = int(row["petalID"])
            else:
                temp_dict["petalID"] = ""

            temp_dict["doi"] = row["doi"].upper()

            if (len(row["venue_names"]) and row["venue_names"][0] =="["):
                old_ven_names = []
                
                for venue in eval(row["venue_names"]):
                    if venue not in temp_dict["venue_names"]:
                        old_ven_names.append(venue)
                
                temp_dict["venue_names"] += old_ven_names
                
            temp_dict["level1"] = (row["label_level_1"] and clean_labels(
                eval(row["label_level_1"]))) or []
            temp_dict["level2"] = (row["label_level_2"] and clean_labels(
                eval(row["label_level_2"]))) or []
            temp_dict["level3"] = (row["label_level_3"] and clean_labels(
                eval(row["label_level_3"]))) or []
            # temp_dict["ask_level1"] = row.get("ask_label_level_1", [])
            # temp_dict["ask_level2"] = row.get("ask_label_level_2", [])
            # temp_dict["ask_level3"] = row.get("ask_label_level_3", [])
            temp_dict["isBiomimicry"] = row.get("isBiomimicry", "undetermined")
            temp_dict["url"] = row["url"]
            temp_dict["species"] = parse_list("species", row)
            temp_dict["absolute_relevancy"] = parse_list("absolute_relevancy", row)
            temp_dict["relative_relevancy"] = parse_list("relative_relevancy", row)
            temp_dict["mag_terms"] = parse_list("mag_terms", row)

            golden_jsons.append(temp_dict)

        except Exception as error:
            print(traceback.format_exc())
            print(row["doi"])

    return golden_jsons

def clean_labels(labels: list):
    """Reformats labels into more model friendly formats.
    Args:
        labels : list
            A formatted list of biomimicry labels.
    Returns:
        list
            List of properly formatted labels.
    """

    clean_labels = [re.sub("\s", "_", label).lower()
                    for label in labels]
    return clean_labels if clean_labels != "" else []

def extract_dois(doi: str):
    doi_obj = re.search(r'\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&\'<>])\S)+)\b', doi)
    if (doi_obj):
        return doi_obj.group().upper()
    else:
        return ""

if __name__ == "__main__":
    args = get_arg_parser()
    dataframe = pd.read_csv(args.csv_path, encoding="utf8")
    dataframe = dataframe.dropna(subset="url").fillna("")
    dataframe = dataframe.sort_values("petalID", axis=0, ascending=True)
    dataframe["doi"] = dataframe["doi"] \
        .apply(extract_dois)
    (api_res, api_dois) = get_api_data(dataframe)
    golden_jsons = convert_to_json(dataframe, api_res, api_dois)
    
    if not os.path.isdir("../FinalFile"):
        os.system("mkdir ../FinalFile")
        
    # Write json data to a json file
    with open(f"{args.output_name}.json", "w") as golden_file:
        golden_file.write("[\n")
        golden_size = len(golden_jsons)

        for index in range(golden_size):
            golden_file.write("\t")
            golden_file.write(json.dumps(golden_jsons[index]))

            if(index < golden_size - 1):
                golden_file.write(",\n")

        golden_file.write("\n]")
