import datetime as dt

from fmiopendata.wfs import download_stored_query

def get_timeframe(hours):
    #The end time is now
    end_time = dt.datetime.utcnow()
    start_time = end_time - dt.timedelta(hours)
    # Convert times to properly formatted strings
    start_time = start_time.isoformat(timespec="seconds") + "Z"
    end_time = end_time.isoformat(timespec="seconds") + "Z"

    return {"start time":start_time, "end time":end_time}

def get_latest_temperature(station):
    if not station:
        return {}

    timeframe=get_timeframe(1)
    
    obs = download_stored_query("fmi::observations::weather::multipointcoverage",
                                args=["bbox=18,55,35,75",
                                      "starttime=" + timeframe["start time"],
                                      "endtime=" + timeframe["end time"]])

    steps = obs.data.keys()
    if not steps:
        return {}

    correct_steps=[]
    #print(steps)
    for step in steps:
        for key in obs.data[step].keys():
            if station in key:
                correct_steps.append(step)
                correct_station=key

    if len(correct_steps)==0:
        return {}

    last_step=max(correct_steps)
    temperature=obs.data[last_step][correct_station]["Air temperature"]["value"]
    return {"Time":last_step,"Temperature":temperature, "Station":correct_station}
    
    


if __name__ == "__main__":

    datapoint=get_latest_temperature("Katinen")

    print(datapoint["Station"])
    print(datapoint["Time"])
    print(datapoint["Temperature"])


    
