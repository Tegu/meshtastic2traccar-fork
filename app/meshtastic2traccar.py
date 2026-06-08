#!/usr/bin/env python3

import logging
import os
import signal

import requests

from datetime import datetime
from urllib.parse import urlparse, urlunparse
import json
import re


try:
    from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2, storeforward_pb2, paxcount_pb2, admin_pb2, remote_hardware_pb2, powermon_pb2
except ImportError:
    from meshtastic import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2, storeforward_pb2, paxcount_pb2, admin_pb2, remote_hardware_pb2, powermon_pb2

import paho.mqtt.client as mqtt

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import google.protobuf
import time
import base64






DEFAULT_TRACCAR_OSMAND = 'http://traccar:5055'

logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)



class Meshtastic2Traccar():
    def __init__(self, conf: dict):
        # Initialize the class.
        super().__init__()

        self.TraccarOsmand = conf.get("TraccarOsmand")

        self.mqtt_broker = conf.get("MqttServer") or "mqtt.meshtastic.org"
        self.mqtt_port = int(conf.get("MqttPort") or "1883")
        self.mqtt_username = conf.get("MqttUser") or "meshdev"
        self.mqtt_password = conf.get("MqttPassword") or "large4cats"
        self.subscribe_topic = conf.get("MqttTopic") or "msh/EU_868/#"

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="", clean_session=True, userdata=None)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.dup = {}
        
        self.chankeys = {
            "LongFast": "1PG7OiApB1nwvP+rz05pAQ==",
            "LongSlow": "1PG7OiApB1nwvP+rz05pAQ==",
            "MediumFast": "1PG7OiApB1nwvP+rz05pAQ==",
            "MediumSlow": "1PG7OiApB1nwvP+rz05pAQ==",
            "ShortFast": "1PG7OiApB1nwvP+rz05pAQ==",
            "ShortSlow": "1PG7OiApB1nwvP+rz05pAQ==",
            "LongMod": "1PG7OiApB1nwvP+rz05pAQ==",
            "ShortTurbo": "1PG7OiApB1nwvP+rz05pAQ=="
        }


    def start(self):
        self.connect_mqtt()
        self.client.loop_forever()

    def connect_mqtt(self):
        logging.info("connect_mqtt")
        print("connect_mqtt")
        if not self.client.is_connected():
            while True:
                self.client.username_pw_set(self.mqtt_username, self.mqtt_password)
                logging.info(f"Connecting to MQTT broker at {self.mqtt_broker}...")
                try:
                    self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
                    return
                except Exception as err:
                    logging.info(err)
                    time.sleep(10)
                    pass


    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.client.subscribe(self.subscribe_topic)
            logging.info(f"Connected to {self.mqtt_broker} on topic {self.subscribe_topic}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logging.info(f"Disconnected from MQTT broker with result code {str(reason_code)}")

    def on_message(self, client, userdata, msg):
        if not re.match("^msh/(.*/)*2/e", msg.topic):
            return()

        # logging.info("on_message")
        se = mqtt_pb2.ServiceEnvelope()

        try:
            se.ParseFromString(msg.payload)
            mp = se.packet
        except Exception as e:
            logging.debug(f"*** ServiceEnvelope: {str(e)}")
            return
        
        # qui ho from, to, id

        # ignora i pacchetti senza from, to e id
        if not (getattr(mp, "from") and getattr(mp, "to") and getattr(mp, "id")):
            logging.debug(f"*** NO from, to or id: {mp}")
            return
        

        i_topic = msg.topic
        i_short_topic = "/".join(msg.topic.split("/")[:-4])
        i_id = getattr(mp, "id")
        i_from = self.dec2hex(getattr(mp, "from"))
        i_to = self.dec2hex(getattr(mp, "to"))
        i_chan = se.channel_id #questo posso ricavarlo dal topic
        i_gateway = se.gateway_id #questo posso ricavarlo dal topic
        i_hopstart = getattr(mp, "hop_start")
        i_hoplimit = getattr(mp, "hop_limit")
        i_hops = i_hopstart - i_hoplimit


        # trova duplicati
        isdup = self.duplicated(getattr(mp, "id"))
        if isdup:
            return()


        if mp.HasField("encrypted") and not mp.HasField("decoded"):
            key = self.chankeys.get(se.channel_id)
            if key:
                self.decode_encrypted(mp, key)

        # qui ho portnum
        i_protocol = portnums_pb2.PortNum.Name(mp.decoded.portnum)
                
        msgjson = google.protobuf.json_format.MessageToDict(se)
        if not mp.HasField("decoded"):
            return()
        
        if not mp.decoded.portnum in (portnums_pb2.POSITION_APP, portnums_pb2.TELEMETRY_APP):
            return()
        
        pl = None
        try:
            pl = self.decode_payload(mp)
            msgjson["packet"]["decoded"]["payload"] = google.protobuf.json_format.MessageToDict(pl)
        except Exception as e:
            logging.debug(f"*** packet decode failed: {str(e)}")
            return()




        # print ("")
        # print ("Service Envelope:")
        # print (se)
        # print ("")
        # print ("Message Packet:")
        # print (mp)
       
        
        # pacchetto totalmente decodificato
        
        # logging.info(msgjson)
        logging.info(", ".join(map(str,[i_from, i_to, i_chan, i_gateway, i_id, i_short_topic, i_protocol])))
        # self.client.publish("test/json", json.dumps(msgjson))

        if mp.decoded.portnum == portnums_pb2.POSITION_APP:
            lat = pl.latitude_i * 1e-7
            lon = pl.longitude_i * 1e-7
            alt = pl.altitude

            name = self.dec2hex(getattr(mp, "from"))
            accuracy = int(23300 / 2 ** (max(pl.precision_bits, 10) - 10))
            speed = pl.ground_speed
            course = 0
            fixTime =  pl.time

            params = {
                "id": i_from,
                "timestamp": fixTime,
                "lat": lat,
                "lon": lon,
                "alt": alt,
                "accuracy": accuracy,
                "speed": speed,
                "bearing": course,
                "meshtastic_chan": i_chan,
                "meshtastic_topic": i_short_topic,
                "meshtastic_gateway": i_gateway,
                "meshtastic_hops": i_hops,
            }
        elif mp.decoded.portnum == portnums_pb2.TELEMETRY_APP:
            params = {
                "id": i_from,
                "timestamp": pl.time,
                "batt": pl.device_metrics.battery_level,
                "voltage": pl.device_metrics.voltage,
                "meshtastic_chan": i_chan,
                "meshtastic_topic": i_short_topic,
                "meshtastic_gateway": i_gateway,
                "meshtastic_hops": i_hops,
            }


        try:
            self.tx_to_traccar(params)
        except ValueError:
            logging.warning(f"id={i_from}")





























    def decode_encrypted(self, mp, key):
            
            try:
                # Convert key to bytes
                key_bytes = base64.b64decode(key.encode('ascii'))
        
                nonce_packet_id = getattr(mp, "id").to_bytes(8, "little")
                nonce_from_node = getattr(mp, "from").to_bytes(8, "little")

                # Put both parts into a single byte array.
                nonce = nonce_packet_id + nonce_from_node

                cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
                decryptor = cipher.decryptor()
                decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

                data = mesh_pb2.Data()
                data.ParseFromString(decrypted_bytes)
                mp.decoded.CopyFrom(data)

            except Exception as e:

                # logging.debug(f"failed to decrypt: \n{mp}")
                logging.debug(f"*** Decryption failed: {str(e)}")
                return


    def decode_payload(self, mp):
        pl = None
        match mp.decoded.portnum:

            case portnums_pb2.NODEINFO_APP:
                pl = mesh_pb2.User()

            case portnums_pb2.POSITION_APP:
                pl = mesh_pb2.Position()

            case portnums_pb2.NEIGHBORINFO_APP:
                pl = mesh_pb2.NeighborInfo()

            case portnums_pb2.TELEMETRY_APP:
                pl = telemetry_pb2.Telemetry()

            case portnums_pb2.TRACEROUTE_APP:
                pl = mesh_pb2.RouteDiscovery()

            case portnums_pb2.ROUTING_APP:
                pl = mesh_pb2.Routing()

            case portnums_pb2.STORE_FORWARD_APP:
                pl = storeforward_pb2.StoreAndForward()

            case portnums_pb2.ADMIN_APP:
                pl = admin_pb2.AdminMessage()

            case portnums_pb2.REMOTE_HARDWARE_APP:
                pl = remote_hardware_pb2.HardwareMessage()

            case portnums_pb2.SIMULATOR_APP:
                pl = mesh_pb2.Compressed()
            
            case portnums_pb2.WAYPOINT_APP:
                pl = mesh_pb2.Waypoint()

            case portnums_pb2.PAXCOUNTER_APP:
                pl = paxcount_pb2.Paxcount()

            case portnums_pb2.STORE_FORWARD_APP:
                pl = storeforward_pb2.StoreAndForward()
            
            case portnums_pb2.NEIGHBORINFO_APP: 
                pl = mesh_pb2.NeighborInfo()

            case portnums_pb2.MAP_REPORT_APP:
                pl = mqtt_pb2.MapReport()

            case portnums_pb2.POWERSTRESS_APP:
                pl = powermon_pb2.PowerStressMessage()

            # portnums_pb2.TEXT_MESSAGE_APP: text
            # portnums_pb2.RANGE_TEST_APP: text
            # portnums_pb2.DETECTION_SENSOR_APP: text

            case _:
                logging.info(f"*** Packet type not found: {mp.decoded.portnum}")

        try:
            pl.ParseFromString(mp.decoded.payload)
        except Exception as e:
            logging.info(f"*** {portnums_pb2.PortNum.Name(mp.decoded.portnum)}: {str(e)}")

        return(pl)




    def dec2hex(self, code):
        return('!' + hex(code)[2:].zfill(8))
    
    def hex2dec(self, code):
        return(int(code[1:], 16))
    
    def base642hex(self, code):
        return(hex(int.from_bytes(base64.b64decode(code), "big"))[2:].zfill(12))
    
    def hex2base64(self, code):
        return(base64.b64encode(int(code, 16).to_bytes(6, "big")).decode("utf-8"))

    def duplicated(self, key):
        dt = datetime.now()
        dup = self.dup

        # clean old data
        for k in list(dup.keys()):
            if (dt - dup[k]).total_seconds() > 60:
                del dup[k]

        if not dup.get(key):
            dup[key] = dt
            return(False)

        return(True)
        








    def tx_to_traccar(self, params: dict):
        # Send position report to Traccar server
        logging.debug(f"tx_to_traccar({params})")
        url = f"{self.TraccarOsmand}/"
        try:
            post = requests.post(url, params=params)
            logging.debug(f"POST {post.status_code} {post.reason} - {post.content.decode()}")
            if post.status_code == 400:
                logging.warning(
                    f"{post.status_code}: {post.reason}. Please create device with matching identifier on Traccar server.")
                raise ValueError(400)
            elif post.status_code > 299:
                logging.error(f"{post.status_code} {post.reason} - {post.content.decode()}")
        except OSError:
            logging.exception(f"Error sending to {url}")























if __name__ == '__main__':
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    logging.basicConfig(level=log_level)


    def sig_handler(sig_num, frame):
        logging.debug(f"Caught signal {sig_num}: {frame}")
        logging.info("Exiting program.")
        exit(0)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    config = {}
    config["TraccarOsmand"] = os.environ.get("TRACCAR_OSMAND", DEFAULT_TRACCAR_OSMAND)

    config["MqttServer"] = os.environ.get("MQTT_SERVER")
    config["MqttPort"] = os.environ.get("MQTT_PORT")
    config["MqttUser"] = os.environ.get("MQTT_USER")
    config["MqttPassword"] = os.environ.get("MQTT_PASSWORD")
    config["MqttTopic"] = os.environ.get("MQTT_TOPIC")


    M2T = Meshtastic2Traccar(config)

    M2T.start()

