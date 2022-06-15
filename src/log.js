const moment = require("moment");

// happen file and line
function get_log_address() {
    let log_address = ""
    try {
        let stack_list = (new Error()).stack.split('\n').slice(3)
        let stack = stack_list[1]
        log_address = stack.split("at")[1]
    } catch (e) {
        log_address = "unknown"
    }
    return log_address
}

// log to std.out
function log(requestId, level, ...msg) {
    if (requestId && msg.length === 0) {
        msg = [requestId]
        requestId = ""
    }
    let msg_obj = {
        id: requestId,
        time: moment().format('YYYY-MM-DD HH:mm:ss.SSS'),
        level: level,
        msg: msg.join(',')
    }
    if (level === "ERROR" || level === "FATAL") {
        msg_obj.line = get_log_address()
    }
    console.log(JSON.stringify(msg_obj))
}

module.exports = {
    debug: function (requestId, ...msg) {
        log(requestId, "DEBUG", ...msg)
    },
    info: function (requestId, ...msg) {
        log(requestId, "INFO", ...msg)
    },
    warn: function (requestId, ...msg) {
        log(requestId, "WARN", ...msg)
    },
    error: function (requestId, ...msg) {
        log(requestId, "ERROR", ...msg)
    },
    fatal: function (requestId, ...msg) {
        log(requestId, "FATAL", ...msg)
        process.exit(0)
    },
}