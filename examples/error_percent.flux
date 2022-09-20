option task = {name: "error percent", every: 1h, offset: 5m}

errors =
    from(bucket: "pages")
        |> range(start: -task.every)
        |> filter(fn: (r) => r._measurement == "errors")
        |> filter(fn: (r) => r._field == "value")
        |> aggregateWindow(every: 1m, fn: sum, createEmpty: true)
        |> fill(column: "_value", value: 0.0)

views =
    from(bucket: "pages")
        |> range(start: -task.every)
        |> filter(fn: (r) => r._measurement == "views")
        |> filter(fn: (r) => r._field == "value")
        |> aggregateWindow(every: 1m, fn: sum, createEmpty: true)
        |> fill(column: "_value", value: 0.0)

join(
    tables: {errors: errors, views: views},
    on: ["_time", "_field", "_start", "_stop"],
    method: "inner",
)
    |> map(fn: (r) => ({_value: r._value_errors / (r._value_views + r._value_errors)}))
    |> set(key: "_measurement", value: "error_percent")
    |> to(bucket: "pages")
