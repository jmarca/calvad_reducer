/*global console require process module */

/**
 * This is the reducer for HPMS data
 *
 * It expects a record or row from the psql table, joining both the
 * estimated geometry as well as the data fields (some anyway), and
 * will merge these into a single object for the set.
 *
 * The basic idea is to calculate volume estimates by time of day,
 * etc, by incorporating the peak factors that are in the HPMS data.
 *
 * In addition, use the single unit and combination trucks in flow
 * percentages (both for peak period and daily average) to estimate
 * the peak and off peak truck flows.
 *
 * The factors are
 *
 *    single unit trucks in peak period
 *    single unit trucks in peak period
 *    combination trucks in peak period
 *    combination trucks in peak period
 *
 * The HPMS manual (2006) states that:
 *
 *
 *    Items 81 ­ 84 Percent Trucks in Peak and Average Traffic Flow
 *
 *    Items 81 ­ 84 provide information on truck use. Ideally, these
 *    items would be updated whenever Item 33 (AADT) is updated. Code
 *    peak % the same as average or estimated peak, if no better data
 *    are available. Some routes, such as urban commuter or
 *    recreational routes may exhibit noteworthy differences in truck
 *    percentages between peak and average.  These differences could
 *    have a significant impact on route capacity.  FHWA vehicle
 *    classes 4, 5, 6 and 7 are single­unit trucks and classes 8, 9,
 *    10, 11, 12 and 13 are combination­unit trucks.  Generally,
 *    single­unit trucks have at least six wheels with no trailers and
 *    combination­unit trucks have any variety of trailer
 *    combinations.
 *
 * So as an estimate, single unit trucks will be classed as not heavy
 * heavy duty, and combination trucks will be classed as heavy heavy
 * duty
 *
 * Occupancy cannot be determined.
 *
 * Speeds cannot be determined.  We do know capacity though, as well
 * as design speed and speed limit, so perhaps I can estimate speed
 * using the BPR function, in a later iteration.  For the moment, the
 * speed estimates are left blank.
 *
 * The weight of truck types cannot be determined
 *
 * The number of axles cannot be determined, but can be estimated from
 * the truck type, as it says that "single unit trucks have at least
 * six wheels", so assign 3 axles to those and 5 to the other.  The
 * round integer values should be a tip off that this isn't measured.
 *
 * So what we *can* compute without too many issues are
 *
 *    n
 *    heavyheavy
 *    not_heavyheavy
 *
 * What we can guesstimate are
 *
 *    wgt_spd_al_veh_speed
 *    count_all_veh_speed (same as n)
 *
 * What I am not even guessing are
 *
 *    o
 *    hh_speed
 *    hh_weight
 *    hh_axles
 *    nhh_speed
 *    nhh_weight
 *    nhh_axles
 *
 * When this routine is done, the data accumulated should be
 * completely compatible with that accumulated from the vds/wim
 * accumulator.  Then the two sets need to be merged together with a
 * second reduce function.
 *
 * The reduce here will group by roadway type.  This  parallels the
 * decision on the freeways that group, well, by freeways and by name.
 *
 */

 /**
 *
 * In addition, there is a wrinkle that must be addressed.  The
 * vds/wim data overlap these data on some freeways.  Therefore, the
 * freeway links need to be accumulated such that they can be merged
 * with the vds/wim data.  I need freeway name, from milepost, to
 * milepost, direction to do that.
 *
 * Or else do it in the imputation process
 *
 * Or else discard the information
 *
 * But I don't want to discard the information because it is helpful
 * to determine the volumes and scaling that I should use for the long
 * sections of estimates, and for freeways with no measurements.
 *
 *
 */
var _ = require('lodash')
var couchCache = require('calvad_couch_cache').couchCache;
var globals=require('./globals.js');
globals.group(6) // set for grouping by lane
var vo = globals.variable_order

var env = process.env

function reducer(){
    var _stash = {}


    // unlike the vds and wim data, the hpms data arrive without yet
    // being converted, so do that

    // big full stop.  Time is nonsense.  I have a yearly value.  So
    // skip that for now, stick with annual average daily traffic, and
    // time is just the year_record field (the year)



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
                       ,'data':[]
                       ,'hpms_record':{}
                       }
            _stash[tkey][fkey]=_.clone(init) // ensure copy, not reference
            for(var i = 0; i < len; i++){
                _stash[tkey][fkey].data[i]=0;
            }
        }
    }

    // not sure it makes sense to call couchdb with each record
    var coucher = couchCache()
    coucher.reset()


    /**
     * accumulate(row)
     *
     * This function does the work of calculating the measures of
     * interest from the SQL record, and then accumulating them.
     *
     */
    function accumulate(row){

        // I expect one row at a time, unlike the vds/wim case
        var tkey // nonsensical for AADT, at this time, just the year
        tkey =  row.year_record

        // eventually, I'm thinking peak or off peak.
        // but these are not defined in the data and have to be
        // estimated from the surrounding VDS/WIM sites.
        // which is a different exercise
        //
        check_tkey(tkey)
        var fkey
        // in the vds/wim case, this is the freeway.  Here I am going to use the
        // roadway class.  If it is a freeway I'll append the route name too
        fkey = row.f_system
        if((/shwy/i).test(row.locality) ){
            // have a highway, not a road
            fkey += '_'+row.route_number
        }
        check_fkey(tkey,fkey,_.size(vo))

        // slot the vo variables here, for processing elsewhere
        _.each(vo,
               function(val,key){
                   row[key]=null
               });
        // some are not null
        row.n=row.aadt
        row.count_al_veh_speed=row.aadt
        row.wgt_spd_all_veh_speed=row.speed_limit
        row.heavyheavy=row.aadt*row.avg_combination
        row.not_heavyheavy=row.aadt*row.avg_single_unit
        row.hh_speed=row.speed_limit
        row.nhh_speed=row.speed_limit

        row.imputations=null

        coucher.stash(row,tkey)


        process_collated_record(tkey,fkey)(row)


        // _stash[tkey][fkey].detectors.sort();
        // _stash[tkey][fkey].detectors = _.uniq(_stash[tkey][fkey].detectors,true)

    }

    // this will take a new record, and properly add it to the current
    // data for that freeway, timestamp, in the _stash
    // initialize with tkey,fkey, then throw a record at the function you get back
    function process_collated_record(tkey,fkey){
        return function(record){
            var type='hpms';

            var memo = _stash[tkey][fkey];

            // these variables must be multiplied by length of segment (VMT, etc)

            _.each([vo.n
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
            memo.detectors.push(record.detector_id)
            _stash[tkey][fkey] = memo;
        }
    }

    function stash(){
        // must merge wim and vds here when and if I split them above
        return _stash;
    }

    function done(){
        coucher.save()
    }
    function reset(){
        done()
        _.each(_stash
               ,function(v,k){
                   _.each(v
                          ,function(vv,kk){
                              delete _stash[k][kk]
                          });
                   delete _stash[k]
               });
        _stash={}
        console.log('cleared')
        return null
    };
    accumulate.stash = stash
    accumulate.reset = reset
    accumulate.process_collated_record = process_collated_record
    return accumulate

}

module.exports=reducer
