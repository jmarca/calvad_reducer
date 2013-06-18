/**
 * reducer
 *
 * var myreducer = new reducer()
 *
 * you can then say
 *
 * couchCache.get(reducer.collect)
 *
 * and then use the getter to get a detector's data
 * and it is just stuffed as is into this object. then
 *
 * var data = myreducer.stash_out()
 *
 * myreducer.reset()
 *
 * and you can loop.  la de da
 *
 */

var globals = require('./globals.js');
var vo = globals.variable_order
var _ = require('lodash')

var env = process.env
var FREEWAY_TRUNCATE = +env.FREEWAY_TRUNCATE || 15


function reducer(opts){
    var _stash = {'header':_.flatten(['ts',spatialagg,'n','hh','not_hh'
                                     ,globals.n_weighted_variables
                                     ,globals.hh_weighted_variables
                                     ,globals.nh_weighted_variables
                                     ,'miles'
                                     ,'lane_miles'
                                     ,'detector_count'
                                     ,'detectors'])
                 ,'data':[]
                 }
    var spatialagg = 'freeway'
    if(!opts) opts = {}
    if (opts.spatialagg){
        spatialagg = opts.spatialagg
    }

    // for this reducer, I just want the records.  do nothing else
    this.process_collated_record = function(record){
        // don't care about count and imputations fields
        console.log(record)
        var data = [record.ts
                   ,record.fwy // for now, hardcode this
                   ,+record.data[vo.n].toFixed(2)
                   ,+record.data[vo.heavyheavy].toFixed(2)
                   ,+record.data[vo.not_heavyheavy].toFixed(2)
                   ,+record.data[vo.o].toFixed(2)
                   ,+record.data[vo.wgt_spd_all_veh_speed].toFixed(2)
                   ,+record.data[vo.hh_weight].toFixed(2)
                   ,+record.data[vo.hh_axles].toFixed(2)
                   ,+record.data[vo.hh_speed].toFixed(2)
                   ,+record.data[vo.nhh_weight].toFixed(2)
                   ,+record.data[vo.nhh_axles].toFixed(2)
                   ,+record.data[vo.nhh_speed].toFixed(2)
                   ,+record.len.toFixed(2)
                   ,+(record.len * record.lanes).toFixed(2)
                   ,1 // detectors length is one, as there is just one detector
                   ,record.detector_id
                   ]
        console.log(data)
        _stash.data.push(data)
        return this
    }
    this.stash_out = function(){
        return _stash
    }
    this.reset = function(){
        _stash.data = []
        return this
    }
    return this
}
module.exports=reducer
