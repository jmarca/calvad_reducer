var _ = require('lodash')
var process_collated_record = require('./process_record')
var couchCache = require('calvad_couch_cacher').couchCache

function  accumulator(processor){
    if(processor === undefined){
        processor = new process_collated_record()
    }
    this.process=function(data,callback){
        // return null // cruft ??
        var coucher = couchCache({time_agg:'hourly'
                                 ,'spatial_agg':'detector'});

        // the data are times, then freeways (or possibly detectors)
        _.each(data
              ,function(timehash,tkey){
                   _.each(timehash
                         ,function(fwyhash,fkey){
                              if( fwyhash !== undefined ){
                                  coucher.stash(fwyhash,tkey)
                                  _.each(fwyhash
                                        ,function(d){
                                             processor(d)
                                             return null
                                         })
                              }
                              return null
                          });
                   return null
               });
        coucher.save(callback)
        return null
    }
    return this
}
module.exports=accumulator
