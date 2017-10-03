# ML-SENTICON: Multilingual, layered sentiment lexicons at lemma level

From [http://journal.sepln.org/sepln/ojs/ojs/index.php/pln/article/view/5041](http://journal.sepln.org/sepln/ojs/ojs/index.php/pln/article/view/5041)
we have taked the dictionary to polarized every terms.

This resource contains lemma-level sentiment lexicons at lemma level for English, Spanish, Catalan, Basque and Galician.
For each lemma, it provides an estimation of polarity (from very negative -1.0 to very positive +1.0),
and a standard deviation.

# List of files
- senticon.xx.xml: lemma-level lexicons, where "xx" is the language identifier (en: English, es: Spanish, ca: Catalan, eu: Basque, gl: Galician).
Each lexicon contains 8 layers.
Each layer is supposed to contain all the previous layers, but only new lemmas are included in the corresponding xml elements.

# Thanks
Cruz, Fermín L., José A. Troyano, Beatriz Pontes, F. Javier Ortega. Building layered, multilingual sentiment lexicons at synset and lemma levels, Expert Systems with Applications, 2014.
