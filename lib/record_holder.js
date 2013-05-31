var _ = require('lodash')
var globals = require('./globals.js');
var vo = globals.variable_order
function make_ts_key(doc,time_agg){
    if(time_agg===undefined) time_agg = 'hourly'
    var tsres = globals.tspat.exec(doc.ts);
    if(time_agg === 'monthly') return [tsres[1],tsres[2]].join("-")
    if(time_agg === 'daily') return [tsres[1],tsres[2],tsres[3]].join("-")
    if(time_agg === 'hourly') return [tsres[1],tsres[2],tsres[3]].join("-") + ' ' + [tsres[4],'00'].join(":")

    if(time_agg === 'weekly') throw new Error('weekly aggregates not implemented ye')
    return null
}

function make_fwy_key(doc,spatial_agg){
    if(spatial_agg===undefined) spatial_agg = 'freeway'
    if(spatial_agg === 'detector') return doc.detector_id
    if(spatial_agg === 'freeway')  return doc.fwy
    return null
}
var env = process.env
var FREEWAY_TRUNCATE = +env.FREEWAY_TRUNCATE || 15

function record_holder(record){
    this.tkey = make_ts_key(record)
    this.fkey = make_fwy_key(record)
    this.type ='vds'
    if(/wim/.test(record.detector_id) ){
        this.type = 'wim';
    }
    // BIG HACK
    // the value is parameterized via the environment
    // variable FREEWAY_TRUNCATE

    this.len = +record.len
    if(this.len && this.len > FREEWAY_TRUNCATE){
        this.len = FREEWAY_TRUNCATE
    }
    this.store = new Array(_.keys(vo).length)
    this.store[vo.n]             = (record.data[vo.n]             *this.len)
    this.store[vo.heavyheavy]    = (record.data[vo.heavyheavy]    *this.len)
    this.store[vo.not_heavyheavy]= (record.data[vo.not_heavyheavy]*this.len)

    // the m vars need to be multiplied by the proper VMT value
    this.store[vo.o]          = record.data[vo.o] * record.data[vo.n]* this.len
    this.store[vo.hh_weight]  = record.data[vo.hh_weight] * record.data[vo.heavyheavy]* this.len
    this.store[vo.hh_axles]   = record.data[vo.hh_axles]  * record.data[vo.heavyheavy]* this.len
    this.store[vo.nhh_weight] = record.data[vo.nhh_weight]* record.data[vo.not_heavyheavy]* this.len
    this.store[vo.nhh_axles]  = record.data[vo.nhh_axles] * record.data[vo.not_heavyheavy]* this.len

    // ditto the hm vars, but they have to be harmonically meaned
    this.store[vo.hh_speed] = record.data[vo.heavyheavy]* this.len/record.data[vo.hh_speed]
    this.store[vo.nhh_speed] = record.data[vo.not_heavyheavy]* this.len/record.data[vo.nhh_speed]
    this.store[vo.wgt_spd_all_veh_speed] = record.data[vo.n]* this.len/record.data[vo.wgt_spd_all_veh_speed]

    this.lanemiles += this.len * record.lanes
    this.detector = record.detector_id
    return this
}
module.exports=record_holder
