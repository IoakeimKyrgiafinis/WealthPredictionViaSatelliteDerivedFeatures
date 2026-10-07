# Global Wealth Estimation from Satellite Imagery

Predicting relative wealth across the developing world using satellite-derived features and DHS survey data — **no ground truth required at prediction time.**

A machine-learning pipeline that predicts **relative household wealth** at 25 km resolution from publicly available satellite and geospatial data, trained and validated against DHS (Demographic and Health Survey) cluster locations across 45 countries in Africa, Asia, and Latin America.

---

## 1. What this project does

The model learns the relationship between satellite-observable features (nightlights, vegetation, climate, terrain, land cover) and the **relative wealth score** of DHS survey clusters. It then applies this learned relationship to a global 25 km grid to produce maps of predicted relative wealth.

**Target definition.** The label is the DHS wealth score (`hv271`/`v191`), z-scored **within each survey** so that the target is *relative* wealth inside a country, not absolute wealth. This removes the between-country offset that would otherwise dominate the signal (a "rich" cluster in Malawi is not comparable in absolute terms to a "rich" cluster in Jordan).

**Output.** A standardised score (`y`), where 0 = the survey mean and +1 = one standard deviation above it. A value of +1 in one country is **not** comparable to +1 in another country — the maps show *within-country* relative wealth.

---

## 2. Data

| Source | Role | Notes |
|---|---|---|
| DHS HR/IR microdata (`.dta`/`.sav`) | Wealth labels | Cluster-level mean of `hv270/hv271` (HR) or `v190/v191` (IR fallback) |
| DHS GPS cluster shapefiles | Coordinates | 33,509 clusters, 46 countries |
| Earth Engine feature exports | Predictors | ~40 pixels per cluster (500 m buffer), one row per pixel |
| Earth Engine 25 km grid CSVs | Prediction surface | 8 region files, 161,101 unique grid points globally |

### Features (33 total)

- **Spectral reflectance:** red, NIR, blue, green, SWIR1, SWIR2 (mean + std each)
- **Vegetation indices:** NDVI, EVI (mean, std, min, max)
- **Land surface temperature:** day and night (mean + std)
- **Climate/terrain:** rainfall (mean + std), elevation, slope, LAI (mean + std)
- **Nightlights:** mean, max, std, plus a `nl_viirs` sensor flag (DMSP pre-2012 vs VIIRS from 2012 onward — the two sensors are on different scales)
- **Land cover:** dominant `lc_type1` class (categorical, mode not mean)

### Label matching and quality control

GPS surveys are matched to wealth tables by country and survey year (with a ±2 year tolerance and calendar adjustments for Nepal's Bikram Sambat and Afghanistan's Solar Hijri years). The match report (`survey_match_report.csv`) flags low match shares and year gaps. Surveys with fewer than **30 labelled clusters** are dropped (within-survey z-scores are unstable below that). Clusters with missing or `(0,0)` coordinates are removed.

**Result:** 30,521 clusters across 45 countries and 46 surveys.

---

## 3. Why validation matters here

The Earth Engine exports contain ~40 pixel rows per cluster, all carrying the **same wealth label**. A random split over pixel rows therefore puts pixels from the same cluster on both sides of the train/test divide — the model memorises the cluster and the R² is inflated. This is the origin of the old headline number of **R² = 0.69**; it is a leaky baseline, not a real result.

Everything honest is done at **one row per cluster**. Four validation regimes are reported, from most to least optimistic:

| Regime | What it tests | R² | MAE | Mean trees |
|---|---|---|---|---|
| Random **pixel** split (leaky) | Memorising clusters already seen | **0.685** | — | 5000 |
| Random **cluster** split | Unseen clusters, but neighbours in training | **0.594** | 0.470 | 3296 |
| Spatial blocks (2°) | Unseen nearby regions | **0.438** | 0.554 | 1055 |
| Leave-one-country-out (LOCO) | A completely new country | **0.247** | 0.645 | 164 |

![Performance falls as validation gets stricter](figures/validation_regimes.png)

**How to read these:**
- Use the **spatial-block** number for "filling gaps between surveyed places in covered regions."
- Use the **LOCO** number for "applying the model in a new country."

All early stopping uses an **inner split of the training fold** (grouped by the same unit as the outer split); the outer test fold is never used to choose the model. Each fold is scored exactly once per cluster via out-of-fold predictions.

### Monte Carlo country hold-out (50 random splits)

To put confidence intervals around the LOCO result, ~80% of countries are used for training and ~20% (~9 countries) are held out, 50 times:

| Metric | Mean (sd; 5–95%) |
|---|---|
| R² | 0.265 (0.161; 0.009 – 0.487) |
| MAE | 0.649 (0.078; 0.531 – 0.774) |
| Median Spearman (per country) | 0.638 (0.084; 0.518 – 0.766) |
| Hit rate (predicted poorest 20% truly in bottom 20%) | 0.392 (0.059; 0.301 – 0.479) — chance = 0.20 |
| Trees | median 89, range 6–604 |

![Monte Carlo country hold-out R² distribution](figures/monte_carlo_r2_hist.png)

Each country is held out between 4 and 17 times across the 50 splits.

---

## 4. Ranking quality (the practically relevant metric)

R² penalises country-level calibration offsets, which are often correctable downstream. What matters for targeting is **ranking**:

| Regime | Poorest 20% → truly bottom 20% | Poorest 20% → truly bottom 40% | Exact quintile | Within one quintile |
|---|---|---|---|---|
| Spatial blocks | 0.445 | 0.717 | 0.393 | 0.781 |
| Leave-one-country-out | 0.387 | 0.640 | 0.355 | 0.724 |
| **Chance** | 0.200 | 0.400 | 0.200 | 0.520 |

Even under LOCO, the predicted poorest quintile contains a true bottom-quintile cluster ~2× as often as chance, and lands in the true bottom 40% ~1.6× as often as chance.

---

## 5. Per-country performance (LOCO)

The full table is in `results_loco_by_country.csv`. **Median Spearman across countries: 0.61.**

Countries with **low Spearman** (below 0.2 — do not trust predictions there):

| Country | n | R² | Spearman | Bias | Likely cause |
|---|---|---|---|---|---|
| NI (Nicaragua) | 411 | −0.478 | −0.417 | −0.623 | Outside training distribution |
| EG (Egypt) | 1460 | −0.353 | −0.141 | +0.460 | Outside distribution; strong country offset |
| CO (Colombia) | 4252 | −0.314 | −0.055 | +0.319 | Outside distribution |
| JO (Jordan) | 725 | −0.397 | +0.034 | +0.599 | Outside distribution |
| PE (Peru) | 971 | −0.088 | +0.113 | −0.253 | Outside distribution |
| HN (Honduras) | 1123 | +0.029 | +0.137 | +0.022 | Outside distribution |

Countries with **strong Spearman** (0.8+): CM, SN, ML, GA, LS, AO, NG, CI, TZ, GH.

**A negative R² with a decent Spearman means a calibration offset, not a ranking failure** — the model orders clusters correctly but is shifted up or down relative to the local mean. This is visible in the `bias` column.

---

## 6. Feature importance and ablation

### Final model — top features (share of total gain)

![Final model: top features](figures/feature_importance.png)

| Feature | Share |
|---|---|
| `nl_mean` (nightlights mean) | 41.7% |
| `lst_night_std` | 6.6% |
| `nl_viirs` (sensor flag) | 5.0% |
| `rain_mean` | 4.7% |
| `evi_mean` | 4.1% |
| `nl_std` | 3.8% |
| `nl_max` | 3.7% |
| `rain_std` | 3.7% |
| `elev` | 3.0% |
| `lc_type1` | 2.4% |

Nightlights dominate (~54% of gain across `nl_mean`, `nl_viirs`, `nl_std`, `nl_max`). The sensor flag matters because DMSP and VIIRS nightlights are on different scales.

### Ablation (LOCO R²)

| Feature set | LOCO R² |
|---|---|
| All features | **0.247** |
| No climate proxies | 0.218 |
| Nightlights only (+ sensor flag) | 0.190 |
| No nightlights | 0.185 |

Removing climate proxies costs ~0.03 R²; nightlights alone get most of the way; removing nightlights costs ~0.06 R². No single group is redundant.

### Relative-feature experiment

Because the target is within-survey relative wealth, features were re-expressed as **within-survey percentile ranks** (evaluation only — using this on the grid would require assigning every grid point to a country). This improved LOCO R² from **0.247 → 0.385**, confirming that a meaningful part of the transfer gap is a feature-scale mismatch across countries, not a failure to learn the underlying signal.

---

## 7. The final model and the global map

**Training:** one row per cluster (30,521 rows), all 45 countries, all 33 features.

**Tree count:** set to the mean chosen by the LOCO folds (**163**), not the full 5,000. The map is applied to places the model has not seen, and a smaller model is the safer choice — more trees mostly sharpen memorisation of known locations. The LOCO folds picked very small models (median ~89 trees) because early stopping fires early when the validation countries are genuinely new.

**Imputation on the grid** mirrors training:
- Nightlights and LAI → **0** (dark / no vegetation)
- Everything else → the **training median** (not the grid median)
- `nl_viirs` is always **1** (the grid is a 2022 snapshot, VIIRS era)

**Training-domain mask.** Every grid point is predicted and drawn on the full-world map. Points are also flagged `in_domain` if they lie within **500 km** of a DHS cluster. **50,992 of 161,101 grid points (31.7%)** fall inside that zone. Outside it (USA, Europe, Australia, China, etc.) the model is extrapolating — read the full map there with caution.

### Full-world map (all grid points, including extrapolation)

![Global Predicted Wealth – 25 km Grid](figures/global_predicted_wealth_25km_.png)

### Restricted to the training domain (within 500 km of a DHS cluster)

![Predicted relative wealth – only within 500 km of DHS data](figures/global_predicted_wealth_25km_domainonly.png)

**Outputs:**
- `global_predicted_wealth_25km.png` — full-world map
- `global_predicted_wealth_25km_domain_only.png` — only points within 500 km of DHS data
- `global_predictions_25km.csv` — per-point predictions with `km_to_dhs` and `in_domain`

**Known limitation.** Training features are **buffer means** (~40 pixels per cluster); grid features are **single-point samples**. Re-exporting the grid with the same buffer and reducer would remove this mismatch and is the single highest-value fix available.

---

## 8. Repository structure

```
.
├── figures/
│   ├── feature_importance.png
│   ├── global_predicted_wealth_25km_.png
│   ├── global_predicted_wealth_25km_domainonly.png
│   ├── monte_carlo_r2_hist.png
│   └── validation_regimes.png
├── 01_dhs_feature_extraction.js
├── 02_global_grid_export.js
├── povertyest_final (1).ipynb
├── requirements.txt
├── LICENSE
└── README.md
```

### Pipeline stages

1. **Setup** — paths, feature lists, hyperparameters (`HIGH_PARAMS`), random seed.
2. **DHS wealth labels** — unzip, read HR/IR, aggregate to cluster, match GPS, write `dhs_labels_v2.csv`.
3. **Cluster-level dataset** — collapse ~40 pixel rows per cluster to one row (mean for continuous, mode for land cover), merge labels, drop tiny surveys, z-score within survey.
4. **Evaluation helpers** — `run_regime` (nested early stopping, out-of-fold predictions), `quintile_report`, `per_group_table`.
5. **Validation regimes** — random cluster split, spatial blocks, LOCO, plus the leaky pixel baseline.
6. **Results summary** — regime table, quintile hit-rates, per-country LOCO.
7. **Optional experiments** — ablation and relative-feature LOCO.
8. **Final model** — train on all clusters with the LOCO-derived tree count.
9. **Global prediction** — load 25 km grid, deduplicate, impute, predict, mask to training domain, draw maps.
10. **Monte Carlo country hold-out** — 50 random country splits for confidence intervals.
