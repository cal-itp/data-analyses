# Google Machine Learning Course

## Numerical Data

https://developers.google.com/machine-learning/crash-course/numerical-data

### Before creating feature vectors, we recommend studying numerical data in two ways:
- Visualize your data in plots or graphs.
- Get statistics about your data.

**Visualize your data**

Graphs can help you find anomalies or patterns hiding in the data. Therefore, before getting too far into analysis, look at your data graphically, either as scatter plots or histograms. View graphs not only at the beginning of the data pipeline, but also throughout data transformations. Visualizations help you continually check your assumptions.

We recommend working with pandas for visualization:

Working with Missing Data (pandas Documentation)
Visualizations (pandas Documentation)

Note that certain visualization tools are optimized for certain data formats. A visualization tool that helps you evaluate protocol buffers may or may not be able to help you evaluate CSV data.

**Statistically evaluate your data**

Beyond visual analysis, we also recommend evaluating potential features and labels mathematically, gathering basic statistics such as:

- mean and median
- standard deviation
- the values at the quartile divisions: the 0th, 25th, 50th, 75th, and 100th percentiles. The 0th percentile is the minimum value of this column; the 100th percentile is the maximum value of this column. (The 50th percentile is the median.)

**Find outliers**

An outlier is a value distant from most other values in a feature or label. Outliers often cause problems in model training, so finding outliers is important.

When the delta between the `0th and 25th percentiles` differs significantly from the delta between the `75th and 100th percentiles`, the dataset probably contains outliers.

Examine the delta between the `mean and standard deviation`. Typically you want to see the standard deviation to be smaller than the mean.

Note: Don't over-rely on basic statistics. Anomalies can also hide in seemingly well-balanced data.
Outliers can fall into any of the following categories:

- The outlier is due to a mistake. For example, perhaps an experimenter mistakenly entered an extra zero, or perhaps an instrument that gathered data malfunctioned. You'll generally delete examples containing mistake outliers.
- The outlier is a legitimate data point, not a mistake. In this case, will your trained model ultimately need to infer good predictions on these outliers?
  - If yes, keep these outliers in your training set. After all, outliers in certain features sometimes mirror outliers in the label, so the outliers could actually help your model make better predictions. Be careful, extreme outliers can still hurt your model.
  - If no, delete the outliers or apply more invasive feature engineering techniques, such as clipping.
  
  **Log scaling**

Log scaling computes the logarithm of the raw value. In theory, the logarithm could be any base; in practice, log scaling usually calculates the natural logarithm (ln).


Log scaling is helpful when the data conforms to a power law distribution. Casually speaking, a power law distribution looks as follows:

- Low values of X have very high values of Y.
- As the values of X increase, the values of Y quickly decrease. Consequently, high values of X have very low values of Y.

Movie ratings are a good example of a power law distribution. In the following figure, notice:

- A few movies have lots of user ratings. (Low values of X have high values of Y.)
- Most movies have very few user ratings. (High values of X have low values of Y.)
Log scaling changes the distribution, which helps train a model that will make better predictions.

> UPT?

**Clipping**

Clipping is a technique to minimize the influence of extreme outliers. In brief, clipping usually caps (reduces) the value of outliers to a specific maximum value. Clipping is a strange idea, and yet, it can be very effective.

How can you minimize the influence of those extreme outliers? Well, the histogram is not an even distribution, a normal distribution, or a power law distribution. What if you simply cap or clip the maximum value of roomsPerPerson at an arbitrary value, say 4.0?

Clipping the feature value at 4.0 doesn't mean that your model ignores all values greater than 4.0. Rather, it means that all values that were greater than 4.0 now become 4.0. This explains the peculiar hill at 4.0. Despite that hill, the scaled feature set is now more useful than the original data.

**Wait a second! Can you really reduce every outlier value to some arbitrary upper threshold? When training a model, yes.**

You can also clip values after applying other forms of normalization. For example, suppose you use Z-score scaling, but a few outliers have absolute values far greater than 3. In this case, you could:

- Clip Z-scores greater than 3 to become exactly 3.
- Clip Z-scores less than -3 to become exactly -3.

> clip upt z_scores?

**Quantile Bucketing**
Quantile bucketing creates bucketing boundaries such that the number of examples in each bucket is exactly or nearly equal. Quantile bucketing mostly hides the outliers.

To illustrate the problem that quantile bucketing solves, consider the equally spaced buckets shown in the following figure, where each of the ten buckets represents a span of exactly 10,000 dollars. Notice that the bucket from 0 to 10,000 contains dozens of examples but the bucket from 50,000 to 60,000 contains only 5 examples. Consequently, the model has enough examples to train on the 0 to 10,000 bucket but not enough examples to train on for the 50,000 to 60,000 bucket.

In contrast, the following figure uses quantile bucketing to divide car prices into bins with approximately the same number of examples in each bucket. Notice that some of the bins encompass a narrow price span while others encompass a very wide price span.

![image.png](attachment:c0358906-bd46-4f3c-bc00-be75b5ce5cc7.png)

## Categorical Data

The term dimension is a synonym for the number of elements in a feature vector. Some categorical features are low dimensional. For example:

![image.png](attachment:dc6dd627-840c-4dbc-b69b-e4aeef8684fe.png)

`When a categorical feature has a low number of possible categories, you can encode it as a vocabulary.` With a vocabulary encoding, the model treats each possible categorical value as a separate feature. During training, the model learns different weights for each category.

For example, suppose you are creating a model to predict a car's price based, in part, on a categorical feature named car_color. Perhaps red cars are worth more than green cars. Since manufacturers offer a limited number of exterior colors, car_color is a low-dimensional categorical feature. The following illustration suggests a vocabulary (possible values) for car_color:

>Is the number of unique `modes` considered "low dimensional"?
>Is the number of unique cities or UZA considered "high dimensional"?

**Feature crosses**

Feature crosses are created by crossing (taking the Cartesian product of) two or more categorical or bucketed features of the dataset. Like polynomial transforms, feature crosses allow linear models to handle nonlinearities. Feature crosses also encode interactions between features.

For example, consider a leaf dataset with the categorical features:

- edges, containing values smooth, toothed, and lobed
- arrangement, containing values opposite and alternate
Assume the order above is the order of the feature columns in a one-hot representation, so that a leaf with smooth edges and opposite arrangement is represented as {(1, 0, 0), (1, 0)}.

The feature cross, or Cartesian product, of these two features would be:

{Smooth_Opposite, Smooth_Alternate, Toothed_Opposite, Toothed_Alternate, Lobed_Opposite, Lobed_Alternate}

where the value of each term is the product of the base feature values, such that:

- Smooth_Opposite = edges[0] * arrangement[0]
- Smooth_Alternate = edges[0] * arrangement[1]
- Toothed_Opposite = edges[1] * arrangement[0]
- Toothed_Alternate = edges[1] * arrangement[1]
- Lobed_Opposite = edges[2] * arrangement[0]
- Lobed_Alternate = edges[2] * arrangement[1]

For example, if a leaf has a lobed edge and an alternate arrangement, the feature-cross vector will have a value of 1 for Lobed_Alternate, and a value of 0 for all other terms:

`{0, 0, 0, 0, 0, 1}`

>Should we feature-cross mode and service?
>Something like "directly operated - bus", "purchased transportation - bus", "directly operated - demand response", "purchased - demand resonse".

## Clustering Advance Course Notes

To cluster your data, you'll follow these steps:

**1. Prepare data.**

you must normalize, scale, and transform feature data before training or fine-tuning a model on that data. In addition, before clustering, check that the prepared data lets you accurately calculate similarity between examples.

**2. Create similarity metric.**

Before a clustering algorithm can group data, it needs to know how similar pairs of examples are. You can quantify the similarity between examples by creating a similarity metric, which requires a careful understanding of your data.

**3. Run clustering algorithm**

A clustering algorithm uses the similarity metric to cluster data. Google course uses k-means.

**4. Interpret results and adjust your clustering.**

Because clustering doesn't produce or include a ground "truth" against which you can verify the output, it's important to check the result against your expectations at both the cluster level and the example level. If the result looks odd or low-quality, experiment with the previous three steps. Continue iterating until the quality of the output meets your needs.

### Normalizing - Log transforms

When a dataset perfectly conforms to a power law distribution, `where data is heavily clumped at the lowest values`, **use a log transform**. See [Log scaling](https://developers.google.com/machine-learning/crash-course/numerical-data/normalization#log_scaling) to review the steps