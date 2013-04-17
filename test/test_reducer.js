/* global require console process describe it */

var should = require('should')

var _ = require('lodash')

var reducer = require('../.')


describe('instantiate reducer',function(){
    it('should have its four methods',function(done){
        var accumulate = reducer()
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
