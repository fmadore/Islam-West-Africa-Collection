# Islam West Africa Collection
Directed by [Frédérick Madore](https://frederickmadore.com/), the *Islam West Africa Collection* (IWAC) is a collaborative, open-access digital database that currently contains over 5,000 archival documents, newspaper articles, Islamic publications of various kinds, audio and video recordings, and photographs on Islam and Muslims in Burkina Faso, Benin, Niger, Nigeria, Togo and Côte d'Ivoire. Most of the documents are in French, but some are also available in Hausa, Arabic, Dendi, and English. The site also indexes over 800 references to relevant books, book chapters, book reviews, journal articles, dissertations, theses, reports and blog posts. This project, hosted by the [Leibniz-Zentrum Moderner Orient (ZMO)](https://www.zmo.de/en) and funded by the Berlin Senate Department for Science, Health and Care, is a continuation of the award-winning [*Islam Burkina Faso Collection*](https://web.archive.org/web/20231207083222/https://islam.domains.uflib.ufl.edu/s/bf/page/home) created in 2021 in collaboration with [LibraryPress@UF](https://librarypress.domains.uflib.ufl.edu/).

English website: [https://islam.zmo.de/s/westafrica/](https://islam.zmo.de/s/westafrica/)

Site en français: [https://islam.zmo.de/s/afrique_ouest/](https://islam.zmo.de/s/afrique_ouest/)

## Folder "Metadata"
### Contains:
- Audio-visual documents metadata ("audio-visual_documents.csv")
- Bibliographical references metadata ("references.csv")
- Documents metadata ("documents.csv")
- Events metadata ("index_events.csv")
- Images metadata ("images.csv")
- Issues of Islamic newspapers/magazines/bulletins metadata ("issues.csv")
- Locations metadata ("index_locations.csv")
- Newspaper articles metadata ("newspaper_articles.csv")

### CSV files format:
- Delimiter: ,
- Enclosure: "
- Escape: /
- Multi-value separator: |

### Metadata:
- Metadata names or headers are Rdf names
- The metadata is a mix a "Dublin Core", "Bibliographic Ontology", "Friend of a Friend", and "Geonames"
- Format of uri: Uri and label separated by a space

## Folder "Jupyter notebooks"
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fmadore/Islam-West-Africa-Collection/HEAD)

To access and run the Jupyter notebooks in this repository, simply click on the Binder button shown above. This will launch a virtual environment within your browser, allowing you to open and run the notebooks seamlessly, without the need for any installation.
There are 4 notebooks in the folder:
1. [Sentiment_analysis.ipynb](https://github.com/fmadore/Islam-West-Africa-Collection/blob/main/Jupyter%20notebooks/Textual%20analysis/Sentiment%20analysis/Sentiment_analysis.ipynb)
2. [Spatial_analysis.ipynb](https://github.com/fmadore/Islam-West-Africa-Collection/blob/main/Jupyter%20notebooks/Spatial%20analysis/Spatial_analysis.ipynb)
3. [Temporal_analysis.ipynb](https://github.com/fmadore/Islam-West-Africa-Collection/blob/main/Jupyter%20notebooks/Textual%20analysis/Temporal%20analysis/Temporal_analysis.ipynb)
4. [Topic_modelling.ipynb](https://github.com/fmadore/Islam-West-Africa-Collection/blob/main/Jupyter%20notebooks/Textual%20analysis/Topic%20modelling/Topic_modelling.ipynb)

## Folder "TimelineJS"
It contains a copy of the spreadsheets that were used to create the [digital exhibits](https://islam.zmo.de/s/westafrica/page/exhibits) using [Timeline JS](https://timeline.knightlab.com/).
