Code used for Facebook's Keyword Extraction Competition hosted by Kaggle.
===============================================================================
Goal was to build a model to predict tags for StackOverflow posts.

Uses a vector-space model search engine, where term vectors are created from StackOverflow questions, and indexed under tags. Term vectors are created for unseen posts, and relevance is scored based on cosine similarity. The highest scoring tags above a threshold are output.
