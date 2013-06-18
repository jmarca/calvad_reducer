/* global require console process describe it */

var should = require('should')

var _ = require('lodash')
var async = require('async')

var reducer = require('../.').simple_reducer

var cacher = require('calvad_couch_cacher').couchCache()


describe('instantiate reducer',function(){
    it('should have its four methods',function(done){
        var accumulate = new reducer({})

        accumulate.should.be.an.instanceOf(Object)
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
    it('can get hourly #1',function(done){
        var accum = new reducer({});
        var getter = cacher.get(accum.process_collated_record)
        async.parallel([function(cb){
                            var feature = {'properties':{'detector_id':'1013410'
                                                        ,'ts':ts
                                                        ,'endts':endts
                                                        }}

                            getter(feature
                                  ,function(e,d){
                                       cb(e)
                                   });
                        }
                       ]
                      ,function(e,r){
                           //stash must be non empty
                           var featurehash = {
                               'properties' : {'document': '1013410'
                                              }}
                           var stash = accum.stash_out()
                           console.log(stash)
                           stash.should.have.property('header')
                           stash.header.should.be.an.instanceOf(Array)
                           stash.should.have.property('data')
                           stash.data.should.be.an.instanceOf(Array)
                           stash.data.length.should.eql(3)
                           _.each(stash.data
                                 ,function(row){
                                      row.should.have.property('length'
                                                              ,stash.header.length)
                                      row[row.length - 1].should.eql('1013410')
                                  });
                           done()
                       })

    })
})
