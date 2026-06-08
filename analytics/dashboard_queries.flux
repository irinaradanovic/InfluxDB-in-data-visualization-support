// [GAUGE] - Current central furnace load status

from(bucket: "smart_home")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "smart_home_metrics")
  |> filter(fn: (r) => r["_field"] == "Furnace")
  |> last()


// [MOSAIC] - Correlation of external temperature and heating system operation
temp = from(bucket: "smart_home")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "smart_home_metrics")
  |> filter(fn: (r) => r["_field"] == "temperature")
  |> aggregateWindow(every: 30m, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({ 
      _time: r._time, 
      Tip_Podatka: "Spoljna Temperatura", 
      _value: if r._value <= 32.0 then "Ekstremna zima (ispod 0°C)"
              else if r._value > 32.0 and r._value <= 50.0 then "Hladno (0°C - 10°C)"
              else if r._value > 50.0 and r._value <= 70.0 then "Prohladno (10°C - 20°C)"
              else if r._value > 70.0 and r._value <= 85.0 then "Toplo (20°C - 30°C)"
              else "Visoka temperatura (preko 30°C)"
    }))

generation = from(bucket: "smart_home")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "smart_home_metrics")
  |> filter(fn: (r) => r["_field"] == "Furnace") 
  |> aggregateWindow(every: 30m, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({
      _time: r._time,
      Tip_Podatka: "Status Grejanja", 
      _value: if r._value <= 0.1 then "Ugaseno"
              else if r._value > 0.1 and r._value <= 0.5 then "Minimalan rad"
              else if r._value > 0.5 and r._value <= 1.2 then "Normalan rad"
              else if r._value > 1.2 and r._value <= 2.0 then "Pojacan rad"
              else "Maksimalan rad"
    }))

union(tables: [temp, generation])
  |> group(columns: ["Tip_Podatka"])