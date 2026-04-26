// ============================================================
// 02_global_grid_export.js
//
// Samples a 25km global grid of satellite features for
// prediction. Uses the same 34 features as the DHS extraction
// script, built from a single reference year composite.
//
// Grid year: 2022 (recent, all sensors available)
// Coverage: 50°S–50°N (CHIRPS rainfall coverage zone),
//           aligned with DHS training data distribution.
//
// Output: one CSV per region in GEE_25km_V2/
// Merge all CSVs in Python after download.
// ============================================================

// ============================================================
// 0. SETTINGS
// ============================================================
var gridScale  = 25000;  // 25km grid
var gridYear   = 2022;   // reference year for global prediction
var halfWindow = 1;      // ±1 year composite window

// ============================================================
// 1. STATIC ASSETS
// ============================================================
var topo = ee.Image('USGS/SRTMGL1_003').rename('elev')
  .addBands(ee.Terrain.slope(
    ee.Image('USGS/SRTMGL1_003')).rename('slope'));

var lc = ee.Image('MODIS/061/MCD12Q1/2018_01_01')
  .select('LC_Type1').rename('lc_type1').toFloat();

// ============================================================
// 2. BUILD 2022 COMPOSITE
// ============================================================
var y     = ee.Number(gridYear);
var start = ee.Date.fromYMD(y.subtract(halfWindow), 1, 1);
var end   = ee.Date.fromYMD(y.add(halfWindow), 12, 31);

// ---- Surface reflectance ----
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

// ---- NDVI / EVI ----
var mod13 = ee.ImageCollection('MODIS/061/MOD13A2')
  .filterDate(start, end).select(['NDVI','EVI']);
var mod13Img = mod13.mean().rename(['ndvi_mean','evi_mean'])
  .addBands(mod13.min().rename(['ndvi_min','evi_min']))
  .addBands(mod13.max().rename(['ndvi_max','evi_max']))
  .addBands(mod13.reduce(ee.Reducer.stdDev())
    .rename(['ndvi_std','evi_std']));

// ---- Land Surface Temperature ----
var mod11 = ee.ImageCollection('MODIS/061/MOD11A2')
  .filterDate(start, end).select(['LST_Day_1km','LST_Night_1km']);
var mod11Img = mod11.mean().rename(['lst_day_mean','lst_night_mean'])
  .addBands(mod11.reduce(ee.Reducer.stdDev())
    .rename(['lst_day_std','lst_night_std']));

// ---- Rainfall (CHIRPS covers 50°S–50°N) ----
var chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
  .filterDate(start, end);
var chirpsImg = chirps.mean().rename('rain_mean')
  .addBands(chirps.reduce(ee.Reducer.stdDev()).rename('rain_std'));

// ---- Leaf Area Index ----
var mcd15 = ee.ImageCollection('MODIS/061/MCD15A3H')
  .filterDate(start, end).select('Lai');
var laiImg = mcd15.mean().rename('lai_mean')
  .addBands(mcd15.reduce(ee.Reducer.stdDev()).rename('lai_std'));

// ---- Nightlights (VIIRS — 2022 is well within VIIRS era) ----
var viirsCol = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
  .filterDate(start, end).select('avg_rad');
var nlImg = viirsCol.mean().rename('nl_mean')
  .addBands(viirsCol.max().rename('nl_max'))
  .addBands(viirsCol.reduce(ee.Reducer.stdDev()).rename('nl_std'));

// ---- Stack all bands ----
var allBands = mod09Img
  .addBands(mod13Img)
  .addBands(mod11Img)
  .addBands(chirpsImg)
  .addBands(laiImg)
  .addBands(nlImg)
  .addBands(topo)
  .addBands(lc);

// ============================================================
// 3. WATER MASK
// MODIS MOD44W — removes ocean and permanent water only.
// Keeps deserts, tundra, and arid land (unlike Hansen mask).
// ============================================================
var waterMask = ee.Image('MODIS/006/MOD44W/2015_01_01')
  .select('water_mask').eq(0);  // 0 = land, 1 = water

var gridMasked = allBands.updateMask(waterMask);

// ============================================================
// 4. REGION DEFINITIONS
// 8 regions covering the developing world within CHIRPS range.
// North Africa added as separate region for better Sahara coverage.
// Europe/North America extended to capture full CHIRPS range.
// ============================================================
var regions = {
  'africa':        ee.Geometry.Rectangle([-20, -35,  55,  38]),
  'north_africa':  ee.Geometry.Rectangle([-20,  15,  55,  38]),
  'asia_west':     ee.Geometry.Rectangle([ 25,  -5,  90,  80]),
  'asia_east':     ee.Geometry.Rectangle([ 90,  -5, 180,  80]),
  'latin_america': ee.Geometry.Rectangle([-85, -60, -30,  15]),
  'oceania':       ee.Geometry.Rectangle([110, -50, 180,    5]),
  'europe':        ee.Geometry.Rectangle([-25,  35,  45,  82]),
  'north_america': ee.Geometry.Rectangle([-180,   5, -50,  85])
};

// ============================================================
// 5. EXPORT FUNCTION
// dropNulls: true — pixels missing any band are excluded.
// Sparse retrievals over hyper-arid areas are handled in Python
// by imputing rain/LAI/nightlight nulls with 0.
// ============================================================
var exportRegion = function(region, name) {
  var sampled = gridMasked.sample({
    region:     region,
    scale:      gridScale,
    projection: 'EPSG:4326',
    geometries: true,
    dropNulls:  true
  });
  Export.table.toDrive({
    collection:  sampled,
    description: 'global_25km_v2_' + name,
    folder:      'GEE_25km_V2',
    fileFormat:  'CSV'
  });
};

// ============================================================
// 6. SUBMIT ALL 8 REGIONS AT ONCE
// All jobs run in parallel on GEE's servers.
// ============================================================
exportRegion(regions['africa'],        'africa');
exportRegion(regions['north_africa'],  'north_africa');
exportRegion(regions['asia_west'],     'asia_west');
exportRegion(regions['asia_east'],     'asia_east');
exportRegion(regions['latin_america'], 'latin_america');
exportRegion(regions['oceania'],       'oceania');
exportRegion(regions['europe'],        'europe');
exportRegion(regions['north_america'], 'north_america');
