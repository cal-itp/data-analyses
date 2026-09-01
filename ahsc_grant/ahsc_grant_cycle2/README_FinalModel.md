# Ordinary Least Squares (OLS) Model for Transit Ridership Modeling

## What is the Goal of This Model?

The goal of this model is to understand and explain variation in **average daily transit boardings** at bus stops using characteristics of transit service, surrounding land use, demographic context, and transit agency differences.

The model estimates how these factors are associated with changes in ridership, with the dependent variable defined as the **natural logarithm of average daily boardings** (`log_boardings`). Using the log form allows the model to better handle skewed ridership distributions and interpret results in proportional terms.

---

# Why Use an OLS Model with Log-Transformed Ridership?

Ordinary Least Squares (OLS) regression is used here to estimate linear relationships between predictors and the log of ridership.

Transforming ridership using the natural logarithm helps:

- reduce the influence of extremely high-ridership stops,
- stabilize variance across observations,
- improve model fit,
- enable interpretation of effects in percentage terms.

Instead of modeling raw boardings directly, the model explains **relative differences in ridership levels** across stops.

---

# Variables Used in the Model

## Dependent Variable
- **`log_boardings`**: Natural logarithm of average daily boardings

## Independent Variables

| Variable | Meaning |
|----------|--------|
| `log_arrivals` | Log of transit vehicle arrivals (service frequency) |
| `land_use_index` | Composite measure of surrounding population, employment, and destinations |
| `households_no_vehicle_adj` | Households without access to a vehicle |
| `total_youth_adj` | Youth population near the stop |
| `inc_total_lowincome_adj` | Low-income population |
| `organization_name` | Transit agency fixed effects (dummy variables) |

---

# Land Use Index

The **Land Use Index** measures the intensity of activity around each stop by combining four normalized components:

- Total population (`total_pop_adj`)
- Workplace employment from LODES **WAC** (`jobs_tot_work_adj`)
- Resident workers from LODES **RAC** (`jobs_tot_home_adj`)
- Non-work destinations (`poi_total_adj`), including retail, services, education, healthcare, and recreation

Each component is first rescaled using **min-max normalization**:

$$
x_{norm} = \frac{x - x_{min}}{x_{max} - x_{min}}
$$

This ensures all variables are on a comparable 0–1 scale.

The index is then calculated as:

$$
\text{Land Use Index} =
\frac{
Population_{norm}
+
WAC_{norm}
+
RAC_{norm}
+
NonWorkDestinations_{norm}
}{4}
$$

Higher values indicate areas with more people, jobs, and activity destinations—conditions that generally support higher transit demand.

---

# Organization Fixed Effects

The model includes **organization fixed effects** using dummy variables for `organization_name`.

These control for systematic differences across transit agencies such as:

- service design,
- operating practices,
- fare structure,
- network characteristics,
- regional travel behavior.

Each coefficient represents how an agency differs from a reference agency after accounting for all other variables.

---

# Model Performance

| Statistic | Value |
|----------|------:|
| Observations | 14,198 |
| R-squared | 0.474 |
| Adjusted R-squared | 0.474 |
| F-statistic | 984.0 |
| p-value (F-statistic) | < 0.001 |

### Interpretation

The model explains approximately **47.4% of the variation** in log-transformed ridership.

The model is highly statistically significant overall, indicating that the included variables jointly provide strong explanatory power.

---

# Interpreting the Coefficients

Because the dependent variable is in logarithmic form, interpretation depends on the predictor type:

- **Log–log relationship (`log_arrivals`)** → elasticity (percent change)
- **Linear predictors** → approximate percent change in ridership
- **Dummy variables (agencies)** → difference from reference agency

For linear variables, the exact percent change is:

$$
100 \times \left(e^{\beta} - 1\right)
$$

---

# Key Findings

## Service Frequency (log_arrivals)

- **Coefficient: 1.2078 (highly significant)**

Service frequency is the strongest driver in the model.
A 1% increase in vehicle arrivals is associated with approximately a **1.21% increase in ridership**, holding all else constant.

---

## Land Use Index

- **Coefficient: 5.6223 (highly significant)**

Because the dependent variable is in logs, this effect is multiplicative.

### Key numeric interpretation:

- A **0.1 increase** in the Land Use Index is associated with:

$$
e^{0.1 \times 5.6223} - 1 \approx 75\% \text{ higher ridership}
$$

- A **0.01 increase** is associated with about:

$$
e^{0.01 \times 5.6223} - 1 \approx 5.8\% \text{ higher ridership}
$$

- Moving from the **lowest to highest land use (0 → 1)** implies:

$$
e^{5.6223} \approx 277\times \text{ higher ridership}
$$

### Interpretation

Even small increases in land-use intensity are associated with large increases in transit ridership, showing that more dense and activity-rich areas strongly support higher demand.

---

## Household Vehicles

- **Coefficient: -0.0003 (marginal, p ≈ 0.05)**

Very small and weak relationship.

After controlling for service and land use, vehicle availability shows little independent explanatory power.

---

## Youth Population

- **Coefficient: 0.0002 (significant)**

Areas with more young residents tend to generate higher transit demand, though the per-person effect is small.

---

## Low-Income Population

- **Coefficient: -0.000085 (not significant)**

No statistically meaningful relationship after controlling for other factors.

---

# Transit Agency Effects (Fixed Effects)

Large differences exist between agencies even after controlling for service and land use.

Examples:

- Strong positive effects: **Caltrain**, **BART**
- Moderate positive effects: **SacRT Bus**, **SDMTS**, **OCTA**
- Slight negative or weaker effects: **Riverside Transit**

These differences reflect structural and operational variation across transit systems not captured by other variables.

---

# Overall Conclusion

This model shows that transit ridership is primarily driven by:

1. **Service frequency (strongest factor)**
2. **Land use intensity around stops**
3. **Transit agency system effects**


Overall, the model explains about **47% of variation in ridership**, indicating that while core structural factors are captured well, additional influences such as stop-level design, network connectivity, and temporal variation likely also play important roles.
