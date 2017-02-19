const runner = require('../index.js')
const kafkaClient = require('kafkaesque')

const config = require('./config.json')

function warmUpKafka(config) {
    kafkaClient(config.source)

    const interator = 100
    while(iterator--) {
        kafkaClient.produce('topic'+iterator, 'message'+iterator)
    }
}

warmupKafka(config)
