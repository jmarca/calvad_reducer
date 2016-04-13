/* global require console process describe it */

var should = require('should')

var _ = require('lodash')

var queue = require('d3-queue').queue

var reducer = require('../.').reducer
var couch_cacher = require('calvad_couch_cacher').couchCache
var path = require('path')
var rootdir = path.normalize(process.cwd())
var config_file = rootdir+'/test.config.json'
var config={}
var config_okay = require('config_okay')
before(function(done){

    config_okay(config_file,function(err,c){
        if(err){
            throw new Error('node.js needs a good croak module')
        }
        config = Object.assign(config,c)
        return done()
    })
    return null

})


describe('instantiate reducer',function(){
    it('should have its four methods',function(done){
        var accumulate =  reducer({})
        accumulate.should.be.an.instanceOf(Function)
        accumulate.should.have.property('stash_out')
        accumulate.should.have.property('reset')
        accumulate.should.have.property('process_collated_record')
        done()
    })
})

describe('use reducer',function(){
    it('should accumulate data')
    it('should process a collated record')
    it('should reset')

})
var ts = new Date(2008,6,25,13,0).getTime()/ 1000
var endts =  new Date(2008,6,25,15,0).getTime()/1000
describe('collate from couchCache get',function(){
    it('can get hourly',function(done){

        var cacher = couch_cacher(
            Object.assign({years:2008},config.couchdb)
        )

        var accum = reducer({'time_agg':'hourly'
                            ,'spatial_agg':'detector'});
        var getter = cacher.get(accum.process_collated_record)
        var q = queue(2)
        q.defer(getter,
                {'properties':{'detector_id':'1013410'
                                         ,'ts':ts
                                         ,'endts':endts
                              }}
               )
        q.defer(getter,
               {'properties':{'detector_id':'1010510'
                                                        ,'ts':ts
                                                        ,'endts':endts
                             }}
               )
        q.await(function(e,a,b){
            //stash must be non empty
            var featurehash = {
                'properties' : {'document': '1013410 and 1010510'
                               }}
            var stash = accum.stash_out(featurehash)
            stash.should.have.property('header')
            stash.header.should.be.an.instanceOf(Array)
            stash.properties.should.have.property('data')
            stash.properties.data.should.be.an.instanceOf(Array)
            stash.properties.data.length.should.eql(6)
            stash.properties.data[0].length.should.eql(stash.header.length)
            var have_1013410 =false
            var have_1010510 =false
            _.each(stash.properties.data,function(d){
                have_1013410 = have_1013410 || d[d.length-1]==='1013410'
                have_1010510 = have_1010510 || d[d.length-1]==='1010510'
            });
            have_1013410.should.be.ok
            have_1010510.should.be.ok
            done()
        })

    })
    // it('can get hourly',function(done){
    //     var cacher = couch_cacher(
    //         Object.assign({years:2008},config.couchdb)
    //     )
    //     var accum = reducer({'time_agg':'daily'
    //                         ,'spatial_agg':'detector'});
    //     var getter = cacher.get(accum.process_collated_record)
    //     async.parallel([function(cb){
    //                         var feature = {'properties':{'detector_id':'1013410'
    //                                                     ,'ts':ts
    //                                                     ,'endts':endts
    //                                                     }}

    //                         getter(feature
    //                               ,function(e,d){
    //                                    //console.log(d)
    //                                    cb(e)
    //                                });
    //                     }
    //                    ,function(cb){
    //                         var feature = {'properties':{'detector_id':'1010510'
    //                                                     ,'ts':ts
    //                                                     ,'endts':endts
    //                                                     }}
    //                         getter(feature
    //                               ,function(e,d){
    //                                    cb(e)
    //                                })
    //                     }]
    //                   ,function(e,r){
    //                        //stash must be non empty
    //                        var featurehash = {
    //                            'properties' : {'document': '1013410 and 1010510'
    //                                           }}
    //                        var stash = accum.stash_out(featurehash)
    //                        stash.should.have.property('header')
    //                        stash.header.should.be.an.instanceOf(Array)
    //                        stash.properties.should.have.property('data')
    //                        stash.properties.data.should.be.an.instanceOf(Array)
    //                        stash.properties.data.length.should.eql(2) // daily agg
    //                        stash.properties.data[0].length.should.eql(stash.header.length)
    //                        var have_1013410 =false
    //                        var have_1010510 =false
    //                        _.each(stash.properties.data,function(d){
    //                            have_1013410 = have_1013410 || d[d.length-1]==='1013410'
    //                            have_1010510 = have_1010510 || d[d.length-1]==='1010510'
    //                        });
    //                        have_1013410.should.be.ok
    //                        have_1010510.should.be.ok
    //                        done()
    //                    })

    // })
})
