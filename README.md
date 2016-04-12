# CalVAD reducer

This package helps collate and reduce imputation results.

It can collect based on raw imputations, or from the couchdb cache of
imputations.

# Usage notes, April 2016

At this time (actually, as of July 2015...) this is used in
calvad_precache_areas/lib/doneGeoQuery

That was refactored out of the monolithic geo_bbox July 2015


# older note
At this time (June 2015) this is used in calvad_link_level, geo_bbox,
and calvad_outliers.  Note that the use is geo_bbox was refactored
into the code in couch_cacher, mostly.

And it is really run from the code in
`geo_bbox/lib/collate_imputations.js`
