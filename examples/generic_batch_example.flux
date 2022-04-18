// Flux translation of the generic_batch_example.tick

// Import Flux packages and define task options
import "influxdata/influxdb/monitor"
import "influxdata/influxdb/schema"
import "math"

// Always inclue an offset to avoid read and write conflicts. Period and every are defined by the every parameter.
option task = {
name: "generic",
every: 10s,
offset: 2s,
}

// Parameters
infoVal = <info_level>
warnVal = <warn_level>
critVal = <crit_level>
infoSig = 1.0
warnSig = 2.0
critSig = 3.0


// Data is grouped by tags or host by default so no need to groupBy('host') as with line 28 in generic_batch_example.tick
data = from(bucket: "<bucket>")
    |> range(start: -task.every)
    |> filter(fn: (r) => r._measurement == "<measurement>")
    |> filter(fn: (r) => r.host == "hostValue1" or r.host == "hostValue2")
    |> filter(fn: (r) => r._field == "stat")
        
// Calculate the mean and standard deviation instead of .sigma 
// Calculate mean from sample and extract the value with findRecord() 
mean_val = (data
    |> mean(column: "_value")
    |> findRecord(fn: (key) => true, idx: 0))._value

// Calculate standard deviation from sample and extract the value with findRecord() 
stddev_val = (data
    |> stddev()
    |> findRecord(fn: (key) => true, idx: 0))._value

// Two options to perform the check: 
// 1. Use the map() function. 
// 2. Use the monitor.check() function. 

// Option 1–Use the map() function. No need to pivot data. No metadata will be written to the default _monitoring bucket.
// Advantages: Relatively straitforward compared to option 2. 
// Disadvantages: No metadata is written to your instance like with Option 2. However you can always elect to incorporate the to() function to write data as desired. 
// Create a custom message to alert on data
alert = (level, type, eventValue)  => {
slack.message(
       url: "https://hooks.slack.com/services/####/####/####",
       text: "An alert \"${string(v: type)}\" event has occurred! The number of field values= \"${string(v: eventValue)}\".",
       color: "warning",
       )
       return level 
       }
data 
    // Map across values and return the number of stddev to the level as well as a custom slack message defined in the alert() function on line 48. 
    |> map(
        fn: (r) => ({r with
level: if r._value < mean_val + math.abs(x: stddev_val) and r._value > mean_val - math.abs(x: stddev_val) or r._value > infoVal then 
              alert(level: 1, type: info, eventValue: r._value)
            else if r._value < mean_val + math.abs(x: stddev_val) * float(v: 2) and r.airTemperature > mean_val - math.abs(x: stddev_val) * float(v: 2) or r._value > okVal then
              alert(level: 2, type: ok, eventValue: r._value)
            else if r._value < mean_val + math.abs(x: stddev_val) * float(v: 3) and r.airTemperature > mean_val - math.abs(x: stddev_val) * float(v: 3) or r._value > warnVal then
              alert(level: 3, type: warn, eventValue: r._value)
            else
               alert(level: 4, type: crit, eventValue: r._value)
)

    // Use the to() function to write the level created by the map() function if you desire. This is not shown.


// Option 2–Use the monitor.check() function: 
// Pivot data with the schema.fieldsAsCols() function to transform data into the expected shape for the monitor.check() function. The monitor.check() function writes metadata about your check to the default _monitoring bucket. 
// Advantages: Write metatada about your check and notification (or alert) to the _monitoring bucket. Read the following for more information about the type of metatdata https://awesome.influxdata.com/docs/part-3/checks-and-notifications/
// Disadvantage: Notifications to your alert endpoint will be delayed by one task every duration or execution. 
data
    |> schema.fieldsAsCols()
    // Use the map() function to create a level column that describes how many standard deviations each stat value is from the mean. 
    |> map(
        fn: (r) => ({r with
level: if r.stat < mean_val + math.abs(x: stddev_val) and r.airTemperature > mean_val - math.abs(x: stddev_val) then
                1
            else if r.stat < mean_val + math.abs(x: stddev_val) * float(v: 2) and r.airTemperature > mean_val - math.abs(x: stddev_val) * float(v: 2) then
                2
            else if r.stat < mean_val + math.abs(x: stddev_val) * float(v: 3) and r.airTemperature > mean_val - math.abs(x: stddev_val) * float(v: 3) then
                3
            else
                4,
}),
)
    // Use the monitor.check() function to perform check and define the thresholds 
    |> monitor.check(
info: (r) => r["level"] == 1,
ok: (r) => r["level"] == 2,
warn: (r) => r["level"] == 3,
crit: (r) => r["level"] == 4,
messageFn: (r) => if r._level == "info" or r.stat > infoVal then
            "${r._check_name} - stat within a single deviation of mean @ ${r.stat}."
        else if r._level == "ok" or r.stat > okVal then
            "${r._check_name} - stat is within two deviations of mean @ ${r.stat}."
        else if r._level == "warn" or r.stat > warnVal then
            "${r._check_name} - stat is within three deviations of mean @ ${r.stat}."
        else
            "${r._check_name} - stat is an outlier @ ${r.stat}",
data: {_check_name: "generic_batch_example", _check_id: "<alphanumeric 16 characters, like: 081488f2dc59f000>", _type: "custom", tags: {}},
)

slack_endpoint = slack["endpoint"](url: "https:hooks.slack.com/services/xxx/xxx/xxx")
notification = {_notification_rule_id: "<alpha numeric 16 characters, like: 081488f2dc59f000>",
                _notification_rule_name: "generic batch example notification",
                _notification_endpoint_id: "<alpha numeric 16 characters, like: 081488f2dc59f000>",
                _notification_endpoint_name: "My slack",}
statuses = monitor["from"](start: -20s, fn: (r) = r["_check_name"] == "generic_batch_example")
crit = statuses 
       |> filter(fn: (r) => r["level"] >=3)
all_statuses = crit 
                // gather data from the last time the task ran to ensure that points have been written to the _monitoring bucket
               |> range(start: -20s, stop:-10s)

all_statuses 
|> monitor["notify"](data: notification, 
                     endpoint: slack_endpoint(mapFn: (r) = (
{channel: "", 
 text: "Notification Rule: ${ r._notification_rule_name } triggered by     check: ${ r._check_name }: ${ r._message }", 
 color: if r["_level"] == "crit" then "danger" else if r["_level"] == "warn" then "warning" else "good"})))


