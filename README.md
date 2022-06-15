# delay_queue
Delay queue implementation that base on rabbitmq dead queue and messageTtl.

```javascript
let dl = new DelayQueue()
dl.createDelayQueue("order", "updateOrder", [1000, 3000, 5000, 10000, 20000], function (msg, callback) {
    let message = JSON.parse(msg.content.toString())
    if (message.retryCount <= 3) {
        return callback("something error...")
    }
    callback()
})

dl.listen("amqp://<username>:<password>@<host>:<ip>/<vhost>")
dl.sendDelayMessage("order", "updateOrder", `test message ${Math.random()}`)
```
