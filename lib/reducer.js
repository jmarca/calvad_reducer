/*global console require process module */

var _ = require('lodash')
var couchCache = require('calvad_couch_cacher').couchCache
var globals = require('./globals.js');
var vo = globals.variable_order

var env = process.env
var FREEWAY_TRUNCATE = +env.FREEWAY_TRUNCATE || 15

function reducer(){
    var _stash = {}


    // basic idea is to pass in records from a detector and sum up
    // with the appropriate freeway, time
    function check_tkey(tkey){
        if(_stash[tkey] === undefined){
            _stash[tkey] = {};
        }
    }

    function check_fkey(tkey,fkey,len){
        if(_stash[tkey][fkey] === undefined){
            var init = {'len':0
                       ,'fwy':fkey
                       ,'lanemiles':0
                       ,'detectors':[]
                       ,'data':[]};
            _stash[tkey][fkey]=_.clone(init) // ensure copy, not reference
            for(var i = 0; i < len; i++){
                _stash[tkey][fkey].data[i]=0;
            }
        }
    }

    // accumulate is used to pull together lots of data records at
    // once. Another side effect is that it is used to put together a
    // call to couchCache to save via bulkdocs
    function accumulate(data){
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
                                             process_collated_record(tkey,fkey,d)
                                             return null
                                         })
                              }
                              return null
                          });
                   return null
               });
        console.log('accumulated')
        coucher.save()
        return null
    }



    // this will take a new record, and properly add it to the current
    // data for that freeway, timestamp, in the _stash
    // initialize with tkey,fkey, then throw a record at the function you get back
    function process_collated_record(tkey,fkey,record){
            var type;
            if(/wim/.test(record.detector_id) ){
                type = 'wim';
            }else{
                type = 'vds';
            }
            check_tkey(tkey)
            check_fkey(tkey,fkey,record.data.length)

            var memo = _stash[tkey][fkey];
            var nums=record.data;

            // BIG HACK
            // the value is parameterized via the environment
            // variable FREEWAY_TRUNCATE

            if(record.len && record.len > FREEWAY_TRUNCATE){
                record.len = FREEWAY_TRUNCATE
            }


            // these variables must be multiplied by length of segment (VMT, etc)


            _.each([,vo.n
                   ,vo.heavyheavy
                   ,vo.not_heavyheavy]
                  ,function(i){
                       memo.data[i]+=(nums[i]*record.len)
                   });
            // the m vars need to be multiplied by the proper VMT value
            memo.data[vo.o]          += nums[vo.o] * nums[vo.n]*record.len
            memo.data[vo.hh_weight]  += nums[vo.hh_weight] * nums[vo.heavyheavy]*record.len
            memo.data[vo.hh_axles]   += nums[vo.hh_axles]  * nums[vo.heavyheavy]*record.len
            memo.data[vo.nhh_weight] += nums[vo.nhh_weight]* nums[vo.not_heavyheavy]*record.len
            memo.data[vo.nhh_axles]  += nums[vo.nhh_axles] * nums[vo.not_heavyheavy]*record.len

            // ditto the hm vars, but they have to be harmonically meaned
            memo.data[vo.hh_speed] += nums[vo.heavyheavy]*record.len/nums[vo.hh_speed]
            memo.data[vo.nhh_speed] += nums[vo.not_heavyheavy]*record.len/nums[vo.nhh_speed]
            memo.data[vo.wgt_spd_all_veh_speed] += nums[vo.n]*record.len/nums[vo.wgt_spd_all_veh_speed]

            memo.len += record.len
            memo.lanemiles += record.len * record.lanes
        if(_.indexOf(memo.detectors,record.detector_id) === -1 ){
            memo.detectors.push(record.detector_id)
            memo.detectors.sort()
        }

        _stash[tkey][fkey] = memo;

    }

    function stash_out(featurehash,spatialagg){
        spatialagg = spatialagg || 'freeway'
        // must merge wim and vds here when and if I split them above


            featurehash.header = _.flatten(['ts',spatialagg,'n','hh','not_hh'
                                           ,globals.n_weighted_variables
                                           ,globals.hh_weighted_variables
                                           ,globals.nh_weighted_variables
                                           ,'miles'
                                           ,'lane_miles'
                                           ,'detector_count'
                                           ,'detectors'])
        if(featurehash.properties === undefined ) featurehash.properties={}
        featurehash.properties.data = []
            var sortedtimes = _.keys(_stash)
            sortedtimes.sort()
            var freeways = [];
            _.forEach(sortedtimes,function(t){
                // value here is a hash, with keys equal to the freeways
                freeways = _.union(freeways
                                  ,_.keys(_stash[t]));
                return null;
            });
            freeways.sort()

            // the data are times, then freeways
            _.each(sortedtimes
                  ,function(tkey){
                       _.each(freeways
                             ,function(fkey){
                                  if( _stash[tkey][fkey] !== undefined ){

                                      // extract the mean values

                                      // the m vars need to be divided by the propoer total VMT value
                                      _stash[tkey][fkey].data[vo.o]          /=  _stash[tkey][fkey].data[vo.n]
                                      _stash[tkey][fkey].data[vo.hh_weight]  /=  _stash[tkey][fkey].data[vo.heavyheavy]
                                      _stash[tkey][fkey].data[vo.hh_axles]   /=  _stash[tkey][fkey].data[vo.heavyheavy]
                                      _stash[tkey][fkey].data[vo.nhh_weight] /=  _stash[tkey][fkey].data[vo.not_heavyheavy]
                                      _stash[tkey][fkey].data[vo.nhh_axles]  /=  _stash[tkey][fkey].data[vo.not_heavyheavy]

                                      // ditto the hm vars, but they have to be harmonically meaned
                                      _stash[tkey][fkey].data[vo.hh_speed]  = _stash[tkey][fkey].data[vo.heavyheavy]/_stash[tkey][fkey].data[vo.hh_speed]
                                      _stash[tkey][fkey].data[vo.nhh_speed] = _stash[tkey][fkey].data[vo.not_heavyheavy]/_stash[tkey][fkey].data[vo.nhh_speed]
                                      _stash[tkey][fkey].data[vo.wgt_spd_all_veh_speed] = _stash[tkey][fkey].data[vo.n]/_stash[tkey][fkey].data[vo.wgt_spd_all_veh_speed]


                                      // console.log(JSON.stringify(data[tkey][fkey]))
                                      // console.log(JSON.stringify(_stash))
                                      // throw new Error('die die die')

                                      // push this time, freeway aggregate output onto the result
                                      featurehash
                                   .properties
                                      .data
                                      .push(_.flatten([tkey
                                                      ,fkey
                                                      ,+_stash[tkey][fkey].data[vo.n].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.heavyheavy].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.not_heavyheavy].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.o].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.wgt_spd_all_veh_speed].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.hh_weight].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.hh_axles].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.hh_speed].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.nhh_weight].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.nhh_axles].toFixed(2)
                                                      ,+_stash[tkey][fkey].data[vo.nhh_speed].toFixed(2)
                                                      ,+_stash[tkey][fkey].len.toFixed(2)
                                                      ,+_stash[tkey][fkey].lanemiles.toFixed(2)
                                                      ,_stash[tkey][fkey].detectors.length
                                                      ,_stash[tkey][fkey].detectors
                                                      ]));
                                  }
                                  return null;
                              });
                       return null;
                   });
        return featurehash
    }

    function reset(){
        _.each(_stash
               ,function(v,k){
                   _.each(v
                          ,function(vv,kk){
                              delete _stash[k][kk]
                          });
                   delete _stash[k]
               });
        _stash={}
        return null
    };
    accumulate.stash_out = stash_out
    accumulate.reset = reset
    accumulate.process_collated_record = process_collated_record
    return accumulate

}

module.exports=reducer
