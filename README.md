# Overview
<p>
As the dataset for PeTaL's labeler grows, so does the effort needed to maintain and update it. This repository contains a data pipeline used to circumvent this issue.
</p>

## Pipeline Summary
<p>
The pipeline is implemented through the Data Version Control (DVC) tool's workflow features. Each of these directories represent one or more of the stages within our workflow.
</p>

## Stages

### Visualization
```
    +-----------------+
    |  pullAskNature  |
    +-----------------+
             *
             *
             *
        +---------+
        | getDOIs |
        +---------+
             *
             *
             *
+--------------------------+
| convertAskNatureTaxonomy |
+--------------------------+
             *
             *
             *
        +---------+
        | combine |
        +---------+
             *
             *
             *
        +---------+
        | convert |
        +---------+
```

### Directory Stage Associations
```
AskNature
├─ pullAskNature
│  └─ algolia-downloader.py -> pullAskNature
├─ doi_scraper
│  └─ get_dois.py -> getDOIs
└─ taxonomy
   └─ taxonomy_converter.py -> convertAskNatureTaxonomy

LabeledData
├─ combine_csvs.py -> combine
└─ convert_with_mag.py -> convert
```
## Stage Descriptions
- pullAskNature
     - This stage of the pipeline makes a call to the AskNature website's database hosted on Algolia. Algolia's api allows us to grab every paper updated or published within time frame. From here we extract the labels associated with the paper, its source URL and the doi.

- getDOIs
    - In this stage, we go through all of the source URLs we have pulled and we scrape a DOI for them if we do not already have it.

- convertAskNatureTaxonomy
    - The labels that AskNature assigns to its papers have a similar structure to PeTaL's. Because of this we can map their labels to the counter-part in our taxonomy. This stage takes a CSV which contains this function mapping information and programmatically converts their labels to ours.
    
    - If it fails to map their label to ours, it pulls the paper out into a separate CSV (currently *papers_to_label.csv*) which can then be manually reviewed.

- combine
    - This stage serves as the consolidation point. All sources of data, such as AskNature, will end up here. 

    - Once triggered, this stage will simply combine all of the CSVs of labeled papers within the 'LabeledData' directory, remove them and produce a new csv containing the merged data.

- convert
    - This is the final stage of the pipeline. When this stage is run, it will pass all of the papers from the previously mentioned merged dataset through the MAG API. To fill in any of the missing fields where possible.

    - Once this is complete, the stage finaly converts this data into a JSON file following the schema utilized within the golden.json file.

## How to Run
This pipeline would ideally be run automatically through a service like GitHub Actions and would not need to be manually triggered. However, some cases may call for this, such as testing new stages on your local environment.

### Install Requirements

Before you attempt to run this locally, you will first need to make sure you have the requirements through:
<br/>``pip install -r requirements.txt``

You will also need to ensure you have the correct NLTK packages, which you can do by running:
<br/> ``python -m nltk.downloader stopwords``
<br/> ``python -m nltk.downloader punkt``

Once this is satisfied, you will be able to follow one of the following methods to trigger the pipeline in different ways:

- Force Run:
    - ``dvc repro -f *STAGE_NAME*``
        - This command will force the pipeline to run the given stage even if none of the stage dependencies have changed.
        - \**STAGE_NAME*\* can be excluded to force run every stage.

- Run Pipeline from a Specific Stage
    - ``dvc repro --downstream *STAGE_NAME*``
        - This command will run the pipeline normally starting from stage \**STAGE_NAME*\*.

- Force Run Pipeline from a Specific Stage
    - ``dvc repro --force-downstream *STAGE_NAME*``
        - This command will force all stages to run starting at \**STAGE_NAME*\* even if none of the relevant stage dependencies have changed.

We can also view the current pipeline structure through ``dvc dag`` which will produce the results seen within the visualization section earlier.

If you need more information, you can refer to DVC's documents here: https://dvc.org/doc/start
