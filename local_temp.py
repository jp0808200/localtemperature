import sys
import datetime as dt
from fmiopendata.wfs import download_stored_query
import json

import paho.mqtt.client as paho

def on_publish(client,userdata,result):
    Print("message published")

def init_mqtt(config):
    mqtt_client=paho.Client(config["MQTTConfig"]["NodeName"])
    mqtt_client.on_publish=on_publish
    if config["MQTTConfig"]["LoginRequired"]:
        mqtt_client.username_pw_set(config["MQTTConfig"]["UserName"], config["MQTTConfig"]["Password"])
    mqtt_client.connect(config["MQTTConfig"]["Server"], config["MQTTConfig"]["Port"])

    return mqtt_client

def load_config(file):
    try:
        cfg=open(file)
        configuration=json.load(cfg)
        cfg.close()
    except:
        print("failed to open configuration file ", file)
        return {}
            
    return configuration



def get_timeframe(hours):
    #The end time is now
    end_time = dt.datetime.utcnow()
    start_time = end_time - dt.timedelta(hours)
    # Convert times to properly formatted strings
    start_time = start_time.isoformat(timespec="seconds") + "Z"
    end_time = end_time.isoformat(timespec="seconds") + "Z"

    return {"start time":start_time, "end time":end_time}

def get_latest_temperature(config):
    if not config:
        return {}

    timeframe=get_timeframe(config["StationConfig"]["TimeFrame"])

    bbox="bbox="+str(config["StationConfig"]["Point1"])+","+ \
        str(config["StationConfig"]["Point2"])+","+ \
            str(config["StationConfig"]["Point3"])+","+ \
                str(config["StationConfig"]["Point4"])
    
    obs = download_stored_query("fmi::observations::weather::multipointcoverage",
                                args=[bbox,
                                      "starttime=" + timeframe["start time"],
                                      "endtime=" + timeframe["end time"]])

    steps = obs.data.keys()
    if not steps:
        return {}

    correct_steps=[]
    #print(steps)
    for step in steps:
        for key in obs.data[step].keys():
            if config["StationConfig"]["StationName"] in key:
                correct_steps.append(step)
                correct_station=key

    if len(correct_steps)==0:
        return {}

    last_step=max(correct_steps)
    temperature=obs.data[last_step][correct_station]["Air temperature"]["value"]
    return {"Time":last_step,"Temperature":temperature, "Station":correct_station}
    
def on_publish(client,userdata,result):
    print("data published \n")
    pass    


if __name__ == "__main__":

    if not len(sys.argv)>1:
        print("configuration file must be given as parameter!")
        exit(1)

    cfg_file=sys.argv[1]
    configuration = load_config(cfg_file)

    datapoint=get_latest_temperature(configuration)

    if datapoint:
        mqtt_client=init_mqtt(configuration)

        print(datapoint["Station"])
        print(datapoint["Time"])
        print(datapoint["Temperature"])
        ret=mqtt_client.publish("OutsideTemp", datapoint["Temperature"])
        exit(0)
    print("No data returned")
    exit(1)


    
