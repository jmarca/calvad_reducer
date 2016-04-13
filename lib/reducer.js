/*global console require process module */
"use strict"
var _ = require('lodash')
var couchCache = require('calvad_couch_cacher').couchCache
var globals = require('./globals.js')
var vo = globals.variable_order

var env = process.env
var FREEWAY_TRUNCATE = +env.FREEWAY_TRUNCATE || 15

function reducer(opts){
    var _stash = {}
    // need aggregation level in opts, default to monthly
    var time_agg = opts.time_agg || 'monthly'
    var spatial_agg = opts.spatial_agg || 'freeway'

    // sanity checks
    if(_.indexOf(['monthly','weekly','daily','hourly'],time_agg) === -1) throw new Error('bad agg level '+opts.time_agg)

    if(_.indexOf(['freeway','detector'],spatial_agg) === -1) throw new Error('bad agg level '+opts.spatial_agg)



    function make_ts_key(doc){
        var tsres = globals.tspat.exec(doc.ts)
        if(time_agg === 'monthly') return [tsres[1],tsres[2]].join("-")
        if(time_agg === 'daily') return [tsres[1],tsres[2],tsres[3]].join("-")
        if(time_agg === 'hourly') return [tsres[1],tsres[2],tsres[3]].join("-") + ' ' + [tsres[4],'00'].join(":")

        if(time_agg === 'weekly') throw new Error('weekly aggregates not implemented ye')
        return null
    }
    function make_fwy_key(doc){
        if(spatial_agg === 'detector') return doc.detector_id
        if(spatial_agg === 'freeway')  return doc.fwy
        return null
    }


    // basic idea is to pass in records from a detector and sum up
    // with the appropriate freeway, time
    function check_tkey(tkey){
        if(_stash[tkey] === undefined){
            _stash[tkey] = {}
        }
        return null
    }

    function check_fkey(tkey,fkey,len){
        var i
        if(_stash[tkey][fkey] === undefined){
            _stash[tkey][fkey]={'len':0
                               ,'fwy':fkey
                               ,'lanemiles':0
                               ,'detectors':[]
                               ,'data':new Array(len)}
            for(i = 0; i < len; i++){
                _stash[tkey][fkey].data[i]=0
            }
        }
        return null
    }

    // accumulate is used to pull together lots of data records at
    // once. Another side effect is that it is used to put together a
    // call to couchCache to save via bulkdocs
    function accumulate(data,callback){
        // return null // cruft ??
        var coucher = couchCache({time_agg:'hourly'
                                 ,'spatial_agg':'detector'})

        // the data are times, then freeways (or possibly detectors)
        _.each(data
              ,function(timehash,tkey){
                   _.each(timehash
                         ,function(fwyhash,fkey){
                              if( fwyhash !== undefined ){
                                  coucher.stash(fwyhash,tkey)
                                  _.each(fwyhash
                                        ,function(d){
                                             process_collated_record(d)
                                             return null
                                         })
                              }
                              return null
                          })
                   return null
               })
        coucher.save(callback)
        return null
    }



    // this will take a new record, and properly add it to the current
    // data for that freeway, timestamp, in the _stash
    // initialize with tkey,fkey, then throw a record at the function you get back
    function process_collated_record(record){
        // make tkey, make fkey
        var tkey = make_ts_key(record)
        var fkey =  make_fwy_key(record)
        var l
        // if we get bugs later, maybe try deep=true
        // reducer is passed in on initialization


        // type is never used.  comment out for now.  delete later
        // var type
        // if(/wim/.test(record.detector_id) ){
        //     type = 'wim'
        // }else{
        //     type = 'vds'
        // }

        check_tkey(tkey)
        check_fkey(tkey,fkey,record.data.length)


        // BIG HACK
        // the value is parameterized via the environment
        // variable FREEWAY_TRUNCATE

        if(record.len && record.len > FREEWAY_TRUNCATE){
            record.len = FREEWAY_TRUNCATE
        }

        // these variables must be multiplied by length of segment (VMT, etc)
        l = +record.len
        _.each([vo.n
               ,vo.heavyheavy
               ,vo.not_heavyheavy]
              ,function(i){
                   _stash[tkey][fkey].data[i]+=(record.data[i]*l)
               })
        // the m vars need to be multiplied by the proper VMT value
        _stash[tkey][fkey].data[vo.o]          += record.data[vo.o] * record.data[vo.n]* l
        _stash[tkey][fkey].data[vo.hh_weight]  += record.data[vo.hh_weight] * record.data[vo.heavyheavy]* l
        _stash[tkey][fkey].data[vo.hh_axles]   += record.data[vo.hh_axles]  * record.data[vo.heavyheavy]* l
        _stash[tkey][fkey].data[vo.nhh_weight] += record.data[vo.nhh_weight]* record.data[vo.not_heavyheavy]* l
        _stash[tkey][fkey].data[vo.nhh_axles]  += record.data[vo.nhh_axles] * record.data[vo.not_heavyheavy]* l

        // ditto the hm vars, but they have to be harmonically meaned
        _stash[tkey][fkey].data[vo.hh_speed] += record.data[vo.heavyheavy]* l/record.data[vo.hh_speed]
        _stash[tkey][fkey].data[vo.nhh_speed] += record.data[vo.not_heavyheavy]* l/record.data[vo.nhh_speed]
        _stash[tkey][fkey].data[vo.wgt_spd_all_veh_speed] += record.data[vo.n]* l/record.data[vo.wgt_spd_all_veh_speed]

        _stash[tkey][fkey].len += l
        _stash[tkey][fkey].lanemiles += l * record.lanes
        _stash[tkey][fkey].detectors.push(record.detector_id)

    }

    function stash_out(featurehash,sa){
        var spatialagg = sa || spatial_agg
        var sortedtimes,freeways
        // must merge wim and vds here when and if I split them above
        //console.log('output stash')
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
        sortedtimes = _.keys(_stash)
        //console.log('sort times')
        sortedtimes.sort()
        freeways = []
        //console.log('loop and populate freeways')
        _.forEach(sortedtimes,function(t){
            // value here is a hash, with keys equal to the freeways
            freeways = _.union(freeways
                               ,_.keys(_stash[t]))
            return null
        })
        //console.log('sort freeeways')
        freeways.sort()
        //console.log(freeways)
        // the data are times, then freeways
        //console.log('loop over times, then freeways')
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


                                  _stash[tkey][fkey].detectors.sort()
                                  _stash[tkey][fkey].detectors = _.uniq(_stash[tkey][fkey].detectors,true)

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
                                                      ]))
                              }
                              return null
                          })
                   return null
               })
        return featurehash
    }

    function reset(){
        _.each(_stash
               ,function(v,k){
                   _.each(v
                          ,function(vv,kk){
                              delete _stash[k][kk]
                          })
                   delete _stash[k]
               })
        _stash={}
        return null
    }
    accumulate.stash_out = stash_out
    accumulate.reset = reset
    accumulate.process_collated_record = process_collated_record
    return accumulate

}

module.exports=reducer
