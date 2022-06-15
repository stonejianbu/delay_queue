const DelayQueue = require("./index")

let dl = new DelayQueue()
dl.createDelayQueue("order", "updateOrder", [1000, 3000, 5000, 10000, 20000], function (msg, callback) {
    let message = JSON.parse(msg.content.toString())
    if (message.retryCount <= 3) {
        return callback("something error...")
    }
    callback()
})

dl.listen("amqp://test:test@81.68.200.136/test")
dl.sendDelayMessage("order", "updateOrder", `test message ${Math.random()}`)
