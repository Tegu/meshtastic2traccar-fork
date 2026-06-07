# About

This little Docker container will connect to a Meshtastic MQTT server, decodes POSITION_APP packets and sends device locations to a [Traccar](https://www.traccar.org/) server.
## How to

### Docker

Clone this repo and then add this to your `docker-compose.yml` file:

```yaml
  meshtastic2traccar:
    build: https://github.com/tegu/meshtastic2traccar-fork.git
    container_name: meshtastic2traccar  # optional
    environment:
      - MQTT_SERVER=mqtt.example.com
      - MQTT_PORT=1883
      - MQTT_USER=user
      - MQTT_PASSWORD=pass
      - MQTT_TOPIC=msh/#
      - TRACCAR_OSMAND=http://traccar.example.com:5055  # optional, defaults to http://traccar:5055
      - LOG_LEVEL=DEBUG  # optional, defaults to INFO
    restart: unless-stopped
  ```
  
  * `TRACCAR_OSMAND` is your Traccar server's Osmand protocol URL


### Traccar

Create a device with the same identifier as the callsign you intend to track.

