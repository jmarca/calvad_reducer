/*global console require process module */

var _ = require('lodash')
var globals = require('./globals.js');
var vo = globals.variable_order
var process_record = require('./process_record')
var accumulator = require('./accumulator')

var env = process.env

// function reducer(opts){
//     var _stash = {}
//     // need aggregation level in opts, default to monthly
//     var time_agg = opts.time_agg || 'monthly'
//     if(_.indexOf(['monthly','weekly','daily','hourly'],time_agg) === -1) throw new Error('bad agg level '+opts.time_agg)

//     var spatial_agg = opts.spatial_agg || 'freeway'
//     if(_.indexOf(['freeway','detector'],spatial_agg) === -1) throw new Error('bad agg level '+opts.spatial_agg)


//     // basic idea is to pass in records from a detector and sum up
//     // with the appropriate freeway, time
//     function _check_tkey(tkey){
//         if(_stash[tkey] === undefined){
//             _stash[tkey] = {};
//         }
//         return null
//     }

//     function _check_fkey(tkey,fkey,len){
//         if(_stash[tkey][fkey] === undefined){
//             _stash[tkey][fkey]={'len':0
//                                ,'fwy':fkey
//                                ,'lanemiles':0
//                                ,'detectors':[]
//                                ,'data':new Array(len)}
//             for(var i = 0; i < len; i++){
//                 _stash[tkey][fkey].data[i]=0;
//             }
//         }
//         return null
//     }
//     // function check_fkey(tkey){
//     //     if(_stash[tkey] === undefined){
//     //         _stash[tkey] = {};
//     //     }
//     //     return null
//     // }

//     function check_tkey(tkey,fkey,len){
//         if(_stash[tkey] === undefined){
//             _stash[tkey]={'len':0
//                          ,'fwy':fkey
//                          ,'lanemiles':0
//                          ,'detectors':[]
//                          ,'data':new Array(len)}
//             for(var i = 0; i < len; i++){
//                 _stash[tkey].data[i]=0;
//             }
//         }
//         return null
//     }

//     // accumulate is used to pull together lots of data records at
//     // once. Another side effect is that it is used to put together a
//     // call to couchCache to save via bulkdocs


//     var stick = []
//     // this will take a new record, and properly add it to the current
//     // data for that freeway, timestamp, in the _stash
//     // initialize with tkey,fkey, then throw a record at the function you get back

//     // this.process_collated_record=function(record){
//     //     stick.push(new record_holder(record))
//     //     return null
//         // // make tkey, make fkey
//         // var tkey = make_ts_key(record)
//         // var fkey =  make_fwy_key(record)
//         // // if we get bugs later, maybe try deep=true
//         // // reducer is passed in on initialization

//         // var type;
//         // if(/wim/.test(record.detector_id) ){
//         //     type = 'wim';
//         // }else{
//         //     type = 'vds';
//         // }
//         // check_tkey(tkey,fkey,record.data.length)


//         // // BIG HACK
//         // // the value is parameterized via the environment
//         // // variable FREEWAY_TRUNCATE

//         // if(record.len && record.len > FREEWAY_TRUNCATE){
//         //     record.len = FREEWAY_TRUNCATE
//         // }

//         // // these variables must be multiplied by length of segment (VMT, etc)
//         // var l = +record.len
//         // _.each([vo.n
//         //        ,vo.heavyheavy
//         //        ,vo.not_heavyheavy]
//         //       ,function(i){
//         //            _stash[tkey].data[i]+= (record.data[i]*l)
//         //        });
//         // // the m vars need to be multiplied by the proper VMT value
//         // _stash[tkey].data[vo.o]          += record.data[vo.o] * record.data[vo.n]* l
//         // _stash[tkey].data[vo.hh_weight]  += record.data[vo.hh_weight] * record.data[vo.heavyheavy]* l
//         // _stash[tkey].data[vo.hh_axles]   += record.data[vo.hh_axles]  * record.data[vo.heavyheavy]* l
//         // _stash[tkey].data[vo.nhh_weight] += record.data[vo.nhh_weight]* record.data[vo.not_heavyheavy]* l
//         // _stash[tkey].data[vo.nhh_axles]  += record.data[vo.nhh_axles] * record.data[vo.not_heavyheavy]* l

//         // // ditto the hm vars, but they have to be harmonically meaned
//         // _stash[tkey].data[vo.hh_speed] += record.data[vo.heavyheavy]* l/record.data[vo.hh_speed]
//         // _stash[tkey].data[vo.nhh_speed] += record.data[vo.not_heavyheavy]* l/record.data[vo.nhh_speed]
//         // _stash[tkey].data[vo.wgt_spd_all_veh_speed] += record.data[vo.n]* l/record.data[vo.wgt_spd_all_veh_speed]

//         // _stash[tkey].len += l
//         // _stash[tkey].lanemiles += l * record.lanes
//         // _stash[tkey].detectors.push(record.detector_id)

//     }

//     this.stash_out=function(featurehash,sa){
//         var spatialagg = sa || spatial_agg
//         // must merge wim and vds here when and if I split them above
//         console.log('output stash')
//         featurehash.header = _.flatten(['ts',spatialagg,'n','hh','not_hh'
//                                        ,globals.n_weighted_variables
//                                        ,globals.hh_weighted_variables
//                                        ,globals.nh_weighted_variables
//                                        ,'miles'
//                                        ,'lane_miles'
//                                        ,'detector_count'
//                                        ,'detectors'])
//         if(featurehash.properties === undefined ) featurehash.properties={}
//         featurehash.properties.data = []


//         // var sortedtimes = _.keys(_stash)
//         // console.log('sort times')
//         //     sortedtimes.sort()
//         //     var freeways = [];
//         // console.log('loop and populate freeways')
//         //     _.forEach(sortedtimes,function(t){
//         //         // value here is a hash, with keys equal to the freeways
//         //         freeways = _.union(freeways
//         //                           ,_.keys(_stash[t]));
//         //         return null;
//         //     });
//         // // the data are keyed by time.  I dropped freeways
//         // console.log('loop over times, sum and mean and such')
//         // _.each(sortedtimes
//         //       ,function(tkey){
//         //            if( _stash[tkey] !== undefined ){

//         //                // extract the mean values

//         //                // the m vars need to be divided by the propoer total VMT value
//         //                _stash[tkey].data[vo.o]          /=  _stash[tkey].data[vo.n]
//         //                _stash[tkey].data[vo.hh_weight]  /=  _stash[tkey].data[vo.heavyheavy]
//         //                _stash[tkey].data[vo.hh_axles]   /=  _stash[tkey].data[vo.heavyheavy]
//         //                _stash[tkey].data[vo.nhh_weight] /=  _stash[tkey].data[vo.not_heavyheavy]
//         //                _stash[tkey].data[vo.nhh_axles]  /=  _stash[tkey].data[vo.not_heavyheavy]

//         //                // ditto the hm vars, but they have to be harmonically meaned
//         //                _stash[tkey].data[vo.hh_speed]  = _stash[tkey].data[vo.heavyheavy]/_stash[tkey].data[vo.hh_speed]
//         //                _stash[tkey].data[vo.nhh_speed] = _stash[tkey].data[vo.not_heavyheavy]/_stash[tkey].data[vo.nhh_speed]
//         //                _stash[tkey].data[vo.wgt_spd_all_veh_speed] = _stash[tkey].data[vo.n]/_stash[tkey].data[vo.wgt_spd_all_veh_speed]


//         //                // console.log(JSON.stringify(data[tkey]))
//         //                // console.log(JSON.stringify(_stash))
//         //                // throw new Error('die die die')


//         //                var detectors = _.unique(_stash[tkey].detectors,true)
//         //                detectors.sort()
//         //                // push this time, freeway aggregate output onto the result
//         //                featurehash
//         //                .properties
//         //                .data
//         //                .push([tkey
//         //                      ,_stash[tkey].fwy
//         //                      ,+_stash[tkey].data[vo.n].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.heavyheavy].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.not_heavyheavy].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.o].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.wgt_spd_all_veh_speed].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.hh_weight].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.hh_axles].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.hh_speed].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.nhh_weight].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.nhh_axles].toFixed(2)
//         //                      ,+_stash[tkey].data[vo.nhh_speed].toFixed(2)
//         //                      ,+_stash[tkey].len.toFixed(2)
//         //                      ,+_stash[tkey].lanemiles.toFixed(2)
//         //                      ,_stash[tkey].detectors.length].concat(detectors))

//         //            }
//         //            return null;
//         //        });
//         return featurehash
//     }

//     function reset(){
//         _.each(_stash
//                ,function(v,k){
//                    _.each(v
//                           ,function(vv,kk){
//                               delete _stash[k][kk]
//                           });
//                    delete _stash[k]
//                });
//         _stash={}
//         return null
//     };
//     this.reset = reset
//     return this

// }

    exports.reducer=function(){}
exports.process_record=process_record
exports.accumulator=accumulator
