# Catalan referendum: Explain #Catalonia 2017

This repository contains data and script from the analysis of tweets scraped during Catalan Referendum of 2017.
The full analysis is published here (http://www.mauropelucchi.com/labs/referendumcat/index.html)

# Tweets

The tweets (about 420,000) are available under the data folder.
We have collected tweets from 29 September to 2 October with reference to keywords such 
as "catalonia","referendum","barcelona","barca", ...

# Script

The catalogna2017.scala script refers to all the analysis. We have followed this macro-step: deduplication, tokenization, cleaning stopwords, lemmazation and finally polarization.
The cluster are created with LDA analysis.

The dictonary for polarization are from ML-SentiCON:
'ML-SentiCON: Cruz, Fermín L., José A. Troyano, Beatriz Pontes, F. Javier Ortega. Building layered, multilingual sentiment lexicons at synset and lemma levels, Expert Systems with Applications, 2014.


# Output

The output data are organized in CSV files:
- users
- relations (relations between users computed from mentions and retweets)
- terms (the list of the first 100 words based on the frequency)

# Stopwords

Stopwords (catalan, spanish and english) are from https://github.com/stopwords-iso(https://github.com/stopwords-iso)


