// Delay queue implementation that base on rabbitmq dead queue and messageTtl.
const amqp = require("amqplib/callback_api")
const {v4} = require("uuid")
const log = require("./log")
const EventEmitter = require("events")

class Emitter extends EventEmitter {}

class DelayQueue {
    constructor() {
        this.connection = undefined
        this.channel = undefined
        this.receving = false
        this.subscribes = []
        this.emitter = new Emitter()
        this.reconnectCount = 0
        this.maxReconnectCount = 1000
        this.reconnecting = false
        this.registering = false
        this.maxResentCount = 100
        // dead queue message ttl
        this.messageTtl = 1000*60*60*24
    }
    listen(url, options) {
        let that = this
        // start to connect
        that.__connect(url, options)

        // amqp reconnect event
        that.emitter.on("reconnect", function () {
            // limit to reconnect in multiple
            if (that.reconnecting) {
                return
            }

            // close the connection
            try {that.connection.close()} catch (error) {} // ignore the error

            // limit retry times
            log.info("amqplib", `try to reconnect, count=${that.reconnectCount}`)
            that.reconnectCount += 1
            if (that.reconnectCount > that.maxReconnectCount) {
                log.error("amqplib", "reconnect fail")
                return
            }

            // retry in timeout
            that.reconnecting = true
            setTimeout(function () {
                try {
                    that.__connect(url, options)
                } finally {
                    that.reconnecting = false
                }
            }, 3000)
        })

        // amqp register subscribers event
        that.emitter.on("subscribe", function () {
            if (that.registering) {
                return
            }
            that.registering = true
            for (const item of that.subscribes) {
                that.__createDelayQueue(item.exchange, item.routingKey, item.retryStrategy, item.handle)
            }
            that.registering = false
            log.info("amqplib", "register subscribers finished")
        })

        // amqp register publish event
        that.emitter.on("publish", function (exchange, routingKey, content, options) {
            options = options?options:{}
            options.messageCount = options.messageCount?options.messageCount:0
            options.type = options.type?options.type:"direct"
            try {
                let msgContent = JSON.stringify(content)
                if (!options.messageCount) {
                    that.channel.assertExchange(exchange, options.type, {durable: true})
                    log.info(content.messageId, msgContent)
                }
                that.channel.publish(exchange, routingKey, Buffer.from(msgContent), options)
            } catch (error) {
                setTimeout(function () {
                    options.messageCount += 1
                    if (options.messageCount > that.maxResentCount) {
                        return
                    }
                    that.emitter.emit("publish", exchange, routingKey, content, options)
                }, 500)
            }
        })
    }
    __connect(url, options) {
        log.info("amqplib", "creating connection")
        let that = this
        amqp.connect(url, options,function (error, connection){
            if (error) {
                log.error("amqplib", error)
                that.emitter.emit("reconnect")
                return
            }
            that.connection = connection
            that.reconnectCount = 0
            log.info("amqplib", "created connection")
            // create channel
            that.connection.createChannel(function (error, channel) {
                if (error) {
                    log.error("amqplib", "create channel", error)
                    that.emitter.emit("reconnect")
                    return
                }
                that.channel = channel
                if (that.receving) {
                    that.emitter.emit("subscribe")
                }
                log.info("amqplib", "created channel")
                that.channel.on("error", function (error) {
                    log.error("amqplib", "channel error", error)
                    that.emitter.emit("reconnect")
                })
                that.channel.on("close", function () {
                    log.info("amqplib", "channel closed")
                })
            })
            connection.on("close", function () {
                log.info( "amqplib","connection closed")
            })
            connection.on("error", function (error) {
                log.error( "amqplib",`connection error: ${error}`)
                that.emitter.emit("reconnect")
            })
        })
    }
    createDelayQueue(exchange, routingKey, retryStrategy, handle) {
        if (!exchange || !routingKey || !retryStrategy || !handle || typeof(retryStrategy) !== "object" || typeof(handle) !== "function") {
            throw "param error"
        }
        this.receving = true
        this.subscribes.push({exchange, routingKey, retryStrategy, handle})
    }
    __createDelayQueue(exchange, routingKey, retryStrategy, handle) {
        let that = this
        that.channel.assertExchange(exchange, "direct", {durable: true})
        // create business queue
        let exOptions = {deadLetterExchange: exchange, messageTtl: that.messageTtl, durable: true, deadLetterRoutingKey: `${routingKey}.dead`}
        that.channel.assertQueue(routingKey, exOptions, function (error, q) {
            if (error) {
                return log.fatal(error)
            }
            that.channel.bindQueue(q.queue, exchange, routingKey)
            that.channel.consume(q.queue, function (msg) {
                handle(msg, function(error) {
                    let msgContent = JSON.parse(msg.content.toString())
                    if (error) {
                        log.warn(msgContent.messageId, error)
                        msgContent.retryCount += 1
                        if (retryStrategy[msgContent.retryCount]) {
                            log.info(msgContent.messageId, `resent to next delay queue, count=${msgContent.retryCount}`)
                            let rtk = `${routingKey}-delayed-${retryStrategy[msgContent.retryCount]}`
                            that.emitter.emit("publish", exchange, rtk, msgContent)
                        } else {
                            log.warn(msgContent.messageId, "retry fail")
                            that.channel.reject(msg, false)
                            return
                        }
                        return that.channel.ack(msg)
                    }
                    log.warn(msgContent.messageId, "processed successfully")
                    that.channel.ack(msg)
                })
            })
        })
        // create business dead queue
        that.channel.assertQueue(`${routingKey}.dead`, {durable: true}, function (error, q) {
            if (error) {
                return log.fatal(error)
            }
            that.channel.bindQueue(q.queue, exchange, `${routingKey}.dead`)
        })
        // create multiple delay queue
        for (let i = 0; i < retryStrategy.length; i++) {
            let ttl = retryStrategy[i]
            let exOptions = {deadLetterExchange: exchange, messageTtl: ttl, durable: true, deadLetterRoutingKey: routingKey}
            let rtk = `${routingKey}-delayed-${ttl}`
            if (i === 0) {
                rtk = `${routingKey}-delayed`
            }
            that.channel.assertQueue(rtk, exOptions, function (error, q) {
                if (error) {
                    return log.fatal(error)
                }
                that.channel.bindQueue(q.queue, exchange, rtk)
            })
        }
    }
    sendDelayMessage(exchange, routingKey, message, options) {
        let msg = {messageId: v4(), retryCount: 0, content: message}
        routingKey = `${routingKey}-delayed`
        this.emitter.emit("publish", exchange, routingKey, msg, options)
    }
}

module.exports = DelayQueue