# Glossary and abbreviations

This document defines domain-specific abbreviations and terminology used across Zugzwang.

## Railway and station terminology

- **DB** (*Deutsche Bahn*): German railway group operating passenger transport, freight transport, and railway infrastructure.

- **StaDa** (*Station Data*): Deutsche Bahn station master-data API providing metadata such as station names, coordinates, price categories, facilities, and federal states.

- **EVA / EVA number** (*historical DB system designation EVA*): Numeric identifier used in Deutsche Bahn timetable and passenger-information systems for stations and stops. The exact original expansion of `EVA` is not reliably documented.

- **IBNR** (*Internationale Bahnhofsnummer*): International station number used to identify railway stations. In German railway interfaces, it is often closely related to or identical to the EVA number.

- **DS100** (*Druckschrift 100*): Historical naming system for railway operating-point codes. It was later replaced by the term Ril100.

- **Ril100 / RiL 100** (*Richtlinie 100*): Alphanumeric code for a railway operating point (*Betriebsstelle*), such as a station or junction.

- **IRIS** (*Integriertes Reisenden-Informationssystem*): Deutsche Bahn passenger-information system providing timetable and operational information such as delays, cancellations, and platform changes.

- **IFOPT** (*Identification of Fixed Objects in Public Transport*): European standard for identifying fixed public-transport objects such as stop places, stations, platforms, and entrances.

- **Hbf** (*Hauptbahnhof*): Main or central railway station.

- **Bf** (*Bahnhof*): Railway station.

- **ICE** (*Intercity-Express*): High-speed long-distance passenger train category operated by DB Fernverkehr.

- **IC** (*Intercity*): Long-distance passenger train category.

- **EC** (*Eurocity*): International long-distance passenger train category.

- **RE** (*Regional-Express*): Regional train category generally stopping less frequently than RB services.

- **RB** (*Regionalbahn*): Regional train category generally serving more intermediate stops than RE services.

- **S-Bahn** (*established service name*): Urban or suburban rapid-rail system. The term has different historical expansions, so Zugzwang treats `S-Bahn` as the established name.

## Meteorological terminology

- **DWD** (*Deutscher Wetterdienst*): Germany's national meteorological service.

- **CDC** (*Climate Data Center*): DWD repository for historical and recent meteorological observations.

- **TU** (*Temperatur und relative Feuchte*): DWD hourly dataset for air temperature and relative humidity. `TU` is primarily used as a DWD dataset code.

- **FF** (*Windgeschwindigkeit und Windrichtung*): DWD hourly dataset for wind speed and wind direction. `FF` is primarily used as a DWD dataset code.

- **QN** (*Qualitätsniveau*): DWD quality-control level indicating which quality checks have been applied to an observation group.

- **QN_3** (*Qualitätsniveau, parameter group 3*): Quality-control field associated with the hourly wind dataset. The `_3` identifies the parameter group, not the quality level itself.

- **QN_9** (*Qualitätsniveau, parameter group 9*): Quality-control field associated with the temperature and humidity dataset. The `_9` identifies the parameter group, not the quality level itself.

- **MESS_DATUM** (*Messdatum*): DWD observation timestamp, represented as `YYYYMMDDHH` in the hourly datasets used by Zugzwang.

- **TT_TU** (*Lufttemperatur, TU dataset*): DWD field for 2-meter air temperature in degrees Celsius.

- **RF_TU** (*Relative Feuchte, TU dataset*): DWD field for relative humidity in percent.

- **F** (*Windgeschwindigkeit*): DWD field for hourly mean wind speed in meters per second.

- **D** (*Windrichtung*): DWD field for hourly mean wind direction in degrees.

- **STATIONS_ID** (*Stations-ID*): DWD numeric identifier for a meteorological observation station.

## Geospatial terminology

- **WGS 84** (*World Geodetic System 1984*): Global geodetic reference system used for latitude and longitude.

- **EPSG:4326** (*European Petroleum Survey Group code 4326*): Geographic coordinate reference system based on WGS 84.

- **Haversine distance**: Great-circle distance calculated between two latitude/longitude coordinates. Zugzwang uses it to find the nearest DWD weather station for each railway station.

## Temporal terminology

- **UTC** (*Coordinated Universal Time*): Global reference time standard used as the common time basis for analytical joins. The abbreviation `UTC` was chosen as a language-neutral compromise between English `CUT` and French `TUC`.

- **CET** (*Central European Time*): Central European standard time, UTC+1.

- **MEZ** (*Mitteleuropäische Zeit*): German name for CET, UTC+1.

- **CEST** (*Central European Summer Time*): Central European daylight-saving time, UTC+2.

- **MESZ** (*Mitteleuropäische Sommerzeit*): German name for CEST, UTC+2.
