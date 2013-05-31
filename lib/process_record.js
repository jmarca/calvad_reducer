var record_holder = require('./record_holder')
// this will take a new record, and properly add it to the current
// data for that freeway, timestamp, in the _stash
// initialize with tkey,fkey, then throw a record at the function you get back
function process_collated_record(){
    this.stick = []
    this.put=function(record){
        this.stick.push(new record_holder(record))
    }
    return this
}

module.exports=process_collated_record