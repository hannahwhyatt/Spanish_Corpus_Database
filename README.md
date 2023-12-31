# Spanish Corpus Database
This repo is based on a corpus of Spanish language web pages (~2 billion words) found on [corpusdata.org](https://www.corpusdata.org/spanish.asp) ([download](https://www.corpusdata.org/formats.asp)).
The data are labelled with the country origin for each webpage source, allowing some analysis to be made on word frequency by region, which has been explored by the creators: https://www.corpusdelespanol.org/web-dial/.

I've so far done two things with this data:
1. Converted the text files into tables in an SQL database, code for which is found in the "create_database" folder.
2. Visualised the regionality in this data in a Dash; see the repo [here](https://github.com/hannahwhyatt/corpusapp/).

