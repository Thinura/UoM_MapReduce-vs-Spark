# Queries to run
queries = [
    "SELECT Year, avg((CarrierDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((NASDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((WeatherDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((LateAircraftDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year",
    "SELECT Year, avg((SecurityDelay / ArrDelay) * 100) FROM DelayedFlights GROUP BY Year ORDER BY Year"
]

queries_names = [
    "Carrier Delay Query",
    "NAS Delay Query",
    "Weather Delay Query",
    "Late Aircraft Delay Query",
    "Security Delay Query"
]