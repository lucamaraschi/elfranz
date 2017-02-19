'use strict;'

const kafka = require('kafkaesque')
const elastic = require('elasticsearch')

const source = {
    brokers:[
        host: 'localhost',
        port: 9092
    ]
}
const destination = {
    host: 'localhost:9200',
    log: 'trace'
} 

function Elfranz(opts) {
    if(!(this instanceof Elfranz)) {
        return new Elfranz(opts) 
    }

    this.source = opts.source
    this.destination = opts.destination

    this.kafkaClient = kafka(this.source)
    this.elastiClient = elastic.Client(this.destination)
}

Elfranz.prototype.subscribeToKafkaTopic = function (topic) {
   this.kafkaClient.poll({topic: this.topic, partition: 0}, poll) 
}

function poll(err, partition) {
    partition.on('message', (message, commit) => {
        // add stack of transformers for the message and publish to elasticearch 
        
        this.elastiClient.create({
            index: 'messagesFromKafka',
            type: 'kafkaMessage',
            id: document.id,
            body: document.body
        }, (error, response) => {
            if (error) return console.error(error) 
            
            if (response) {
                return commit() 
            }
        })
    })
    
    parition.on('error', (err) => {
        // handle error ;-) 
    })
}

module.exports = Elfranz
