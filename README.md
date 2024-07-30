# Islam West Africa Collection (IWAC)

## Project Overview

[![DOI](https://zenodo.org/badge/664653958.svg)](https://zenodo.org/doi/10.5281/zenodo.10390351)

The *Islam West Africa Collection* (IWAC) is a comprehensive, open-access digital database focused on Islam and Muslims in West Africa. Directed by [Frédérick Madore](https://frederickmadore.com/), this project is supported by the [Leibniz-Zentrum Moderner Orient (ZMO)](https://www.zmo.de/en) and funded by the Berlin Senate Department for Science, Health and Care.

### Key Features:
- 8,800+ documents including archival materials, newspaper articles, and multimedia content
- Coverage of Burkina Faso, Benin, Niger, Nigeria, Togo, and Côte d'Ivoire
- Multilingual content (French, Hausa, Arabic, Dendi, English)
- 850+ indexed academic references
- 2,400+ indexed events, languages, locations, organizations, people, and topics

## Access the Collection

- [English Website](https://islam.zmo.de/s/westafrica/)
- [Site en français](https://islam.zmo.de/s/afrique_ouest/)

## Repository Structure

### Chatbot

The IWAC Chat Explorer is an AI-powered web application that implements an augmented retrieval strategy to explore the *Islam West Africa Collection*.

Key Features:
- Interactive chat interface for querying the IWAC dataset
- Augmented retrieval system combining document search and AI-generated responses
- Context-aware responses using the Claude 3.5 Sonnet model from Anthropic
- Source attribution for AI-generated responses

For detailed information on setup, usage, and technical details, please see the [IWAC Chat Explorer README](./Chatbot/README.md).

### Metadata
Contains CSV and JSON-LD files with detailed metadata for all items in the IWAC.

#### CSV Files (`Metadata/CSV/`)
- Audio-visual documents metadata ("audio-visual_documents.csv")
- Bibliographical references metadata ("references.csv")
- Documents metadata ("documents.csv")
- Events metadata ("index_events.csv")
- Images metadata ("images.csv")
- Issues of Islamic newspapers/magazines/bulletins metadata ("issues.csv")
- Locations metadata ("index_locations.csv")
- Newspaper articles metadata ("newspaper_articles.csv")
- Organizations metadata ("index_organizations.csv")
- Persons metadata ("index_persons.csv")
- Topics metadata ("index_topics.csv")

CSV Format: Comma-delimited, double-quoted, '/' as escape character, '|' as multi-value separator

#### JSON-LD Files (`Metadata/JSON-LD/`)
All metadata is also available in JSON-LD format, with filenames corresponding to the CSV files.

#### Resource Templates (`Metadata/Resource Templates/`)
Contains Omeka S resource templates in JSON format. These templates define the structure and properties for different types of resources in the IWAC.

#### Metadata Schema
The metadata uses a mix of schemas including Dublin Core, Bibliographic Ontology, Friend of a Friend, and Geonames.

### TimelineJS
Spreadsheets used to create [digital exhibits](https://islam.zmo.de/s/westafrica/page/exhibits) with [Timeline JS](https://timeline.knightlab.com/).

### Data Visualizations

This repository includes several visualisations created using Python and Plotly, showcasing trends and insights from the IWAC data. View them in the `Visualisations` folder or on the [project website](https://iwac.frederickmadore.com/s/westafrica/page/digital-humanities/).
