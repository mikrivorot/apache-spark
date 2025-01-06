## Overview

In this folder I experiment with three similarity metrics for movies recommendation: 

###  Pearson Correlation

Execution script:
```
python -m recommendation_algorithms.movies_pearson_similarity 50
```

Theory:

![alt text](../docs/screenshots/pearson_correlation.png)


Result (first 5) on `scoreThreshold = 0.5` and `numberOfSharedRatings = 100`:

![alt text](../docs/screenshots/pearson_correlation_recommendations_1.png)


Result (first 10) on `scoreThreshold = 0.3` and `numberOfSharedRatings = 100`:

![alt text](../docs/screenshots/pearson_correlation_recommendations_2.png)


TODO: add `score` explanation

###  [TODO] Cosine Similarity (Adjusted)
```
python -m recommendation_algorithms.movies_adjusted_cosine_similarity 50
```

###  [TODO] Euclidean Distance
```
python -m recommendation_algorithms.movies_euclidean_distance 50
```