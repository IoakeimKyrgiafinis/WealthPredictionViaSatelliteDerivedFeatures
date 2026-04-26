// ============================================================
// 01_dhs_feature_extraction.js
//
// Extracts 34 satellite-derived features for each DHS survey
// cluster. Each point gets features from a ±1 year window
// centered on its own survey year — critical for a pooled
// multi-round dataset spanning 2003–2024.
//
// Strategy: build one composite per unique survey year (fast),
// then use reduceRegions() per year subset (vectorized).
// This avoids rebuilding composites 33k times inside a .map().
//
// Output: one CSV per survey year in DHS_FEATURES_BY_YEAR/
// Merge all CSVs in Python after download.
// ============================================================

// ============================================================
// 0. SETTINGS
// ============================================================
var bufferMeters = 2000;   // 2km buffer around each cluster
var halfWindow   = 1;      // ±1 year composite window

// ============================================================
// 1. STATIC ASSETS (same for all years)
// ============================================================
var topo = ee.Image('USGS/SRTMGL1_003').rename('elev')
  .addBands(ee.Terrain.slope(
    ee.Image('USGS/SRTMGL1_003')).rename('slope'));

var lc = ee.Image('MODIS/061/MCD12Q1/2018_01_01')
  .select('LC_Type1').rename('lc_type1').toFloat();

// ============================================================
// 2. LOAD & PRE-BUFFER DHS POINTS
// ============================================================
var dhs = ee.FeatureCollection(
  'projects/lightemissionsproject/assets/dhs_points'
);

// Buffer the entire collection once up front
var dhsBuffered = dhs.map(function(f) {
  return f.setGeometry(f.geometry().buffer(bufferMeters));
});

print('DHS count:', dhs.size());

// Verify survey years present in asset
var years = dhs.aggregate_array('DHSYEAR').distinct().sort();
print('All survey years:', years);

// ============================================================
// 3. COMPOSITE BUILDER
// Builds a stacked image of all 34 features for a given year.
// Handles:
//   - DMSP nightlights for years < 2012
//   - VIIRS nightlights for years >= 2012
// ============================================================
var buildComposite = function(year) {
  var y     = ee.Number(year);
  var start = ee.Date.fromYMD(y.subtract(halfWindow), 1, 1);
  var end   = ee.Date.fromYMD(y.add(halfWindow), 12, 31);

  // ---- Surface reflectance (MOD09A1) ----
  var mod09 = ee.ImageCollection('MODIS/061/MOD09A1')
    .filterDate(start, end)
    .select(['sur_refl_b01','sur_refl_b02','sur_refl_b03',
             'sur_refl_b04','sur_refl_b06','sur_refl_b07']);
  var mod09Img = mod09.mean().rename([
    'red_mean','nir_mean','blue_mean',
    'green_mean','swir1_mean','swir2_mean'
  ]).addBands(
    mod09.reduce(ee.Reducer.stdDev()).rename([
      'red_std','nir_std','blue_std',
      'green_std','swir1_std','swir2_std'
    ])
  );

  // ---- NDVI / EVI (MOD13A2) ----
  var mod13 = ee.ImageCollection('MODIS/061/MOD13A2')
    .filterDate(start, end).select(['NDVI','EVI']);
  var mod13Img = mod13.mean().rename(['ndvi_mean','evi_mean'])
    .addBands(mod13.min().rename(['ndvi_min','evi_min']))
    .addBands(mod13.max().rename(['ndvi_max','evi_max']))
    .addBands(mod13.reduce(ee.Reducer.stdDev())
      .rename(['ndvi_std','evi_std']));

  // ---- Land Surface Temperature (MOD11A2) ----
  var mod11 = ee.ImageCollection('MODIS/061/MOD11A2')
    .filterDate(start, end).select(['LST_Day_1km','LST_Night_1km']);
  var mod11Img = mod11.mean().rename(['lst_day_mean','lst_night_mean'])
    .addBands(mod11.reduce(ee.Reducer.stdDev())
      .rename(['lst_day_std','lst_night_std']));

  // ---- Rainfall (CHIRPS) ----
  var chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
    .filterDate(start, end);
  var chirpsImg = chirps.mean().rename('rain_mean')
    .addBands(chirps.reduce(ee.Reducer.stdDev()).rename('rain_std'));

  // ---- Leaf Area Index (MCD15A3H) ----
  // Available from 2003 onwards
  var mcd15 = ee.ImageCollection('MODIS/061/MCD15A3H')
    .filterDate(start, end).select('Lai');
  var laiImg = mcd15.mean().rename('lai_mean')
    .addBands(mcd15.reduce(ee.Reducer.stdDev()).rename('lai_std'));

  // ---- Nightlights: VIIRS >= 2012, DMSP < 2012 ----
  // VIIRS starts April 2012. For earlier rounds use DMSP-OLS.
  // Note: DMSP and VIIRS are on different scales — include a
  // sensor flag in your model if comparing across eras.
  var useVIIRS = y.gte(2012);
  var viirsCol = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
    .filterDate(start, end).select('avg_rad');
  var dmspCol  = ee.ImageCollection('NOAA/DMSP-OLS/NIGHTTIME_LIGHTS')
    .filterDate(start, end).select('stable_lights');

  var nlImg = ee.Image(ee.Algorithms.If(
    useVIIRS,
    viirsCol.mean().rename('nl_mean')
      .addBands(viirsCol.max().rename('nl_max'))
      .addBands(viirsCol.reduce(ee.Reducer.stdDev()).rename('nl_std')),
    dmspCol.mean().rename('nl_mean')
      .addBands(dmspCol.max().rename('nl_max'))
      .addBands(dmspCol.reduce(ee.Reducer.stdDev()).rename('nl_std'))
  ));

  return mod09Img
    .addBands(mod13Img)
    .addBands(mod11Img)
    .addBands(chirpsImg)
    .addBands(laiImg)
    .addBands(nlImg)
    .addBands(topo)
    .addBands(lc);
};

// ============================================================
// 4. ONE EXPORT JOB PER SURVEY YEAR
//
// Key optimization: composites are built once per year (18x),
// not once per point (33,000x). Each year subset uses
// reduceRegions() — GEE's vectorized tiled reducer —
// returning one row per DHS cluster with mean over 2km buffer.
//
// All 18 jobs run in parallel on GEE's servers.
// Update this list if your survey years differ.
// ============================================================
var uniqueYears = [
  2003, 2005, 2009,
  2010, 2011, 2012, 2013, 2014, 2015,
  2016, 2017, 2018, 2019, 2020, 2021,
  2022, 2023, 2024
];

uniqueYears.forEach(function(y) {

  // Filter to this year's DHS clusters only
  var subset = dhsBuffered.filter(
    ee.Filter.eq('DHSYEAR', y)
  );

  // Build composite for this year's ±1 window
  var composite = buildComposite(y);

  // reduceRegions — one row per cluster, mean over 2km buffer
  var sampled = composite.reduceRegions({
    collection: subset,
    reducer:    ee.Reducer.mean(),
    scale:      500,
    tileScale:  4   // reduces memory per tile for large collections
  });

  Export.table.toDrive({
    collection:  sampled,
    description: 'dhs_features_year_' + y,
    folder:      'DHS_FEATURES_BY_YEAR',
    fileFormat:  'CSV'
  });

}); // closes forEach
