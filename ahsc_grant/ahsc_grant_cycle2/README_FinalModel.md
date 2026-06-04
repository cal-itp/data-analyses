# Negative Binomial Model For Ridership Modeling
## What is the Goal of This Model?

The goal of this model is to understand and predict **average daily transit boardings** at bus stops using characteristics of the surrounding area and transit service.

In simple terms, the model tries to answer questions like:

- Which stops tend to have higher ridership?
- How does population affect transit usage?
- Do areas with more low-income households or people without cars use transit more?
- Does service frequency increase ridership?

The model helps identify the factors most strongly associated with transit demand.

---
## Why Use a Negative Binomial Model?

Ridership data is a type of **count data** because it represents the number of people boarding transit.

A common starting model for count data is the **Poisson model**, but Poisson assumes that:
- the average and variance of the data are roughly equal.

In real-world transit ridership data, this assumption usually does not hold because:
- some stops have extremely high ridership,
- many stops have low ridership,
- variability is much larger than the average.

This issue is called **overdispersion**.

The **Negative Binomial model** is better because it can handle this extra variability more realistically.

---

## Simple Analogy: Why Negative Binomial Works Better

A Binomial model is like asking: “If I flip a coin 10 times, how many heads will I get?” The number of trials is fixed, so the variability is limited and predictable. This works well for situations where the data does not vary too much.

A Negative Binomial model is more like asking: “How many coin flips will it take to get the 3rd head?” Here, the number of trials can vary widely. Some outcomes happen quickly, while others take much longer. Transit ridership behaves similarly because some bus stops have consistently low or moderate riders, while a few stops can have extremely large ridership counts. The Negative Binomial model is better suited for this type of highly variable data.

---

## Variables Used in the Model

The model predicts:

- **Average Daily Boardings**
  (`average_daily_boardings`)

using the following explanatory variables:

| Variable | Meaning |
|---|---|
| `n_routes` | Number of transit routes serving the stop |
| `n_arrivals` | Number of vehicle arrivals |
| `land_use_index` | Composite index based on population, jobs and non-work destinations |
| `workers_with_no_car_adj` | Number of workers without access to a car |
| `poverty_pop_adj` | Low-income population |
| `disabled_pop_adj` | Disabled population |

---

```python
