# DWD Wetterdatenaufbereitung

Konvertierung von Wetterdaten des Deutschen Wetterdienstes (DWD) in SQL
Anweisungen zum Import in eine PostgreSQL Datenbank.

Die Rohdaten können hier bezogen werden:

ftp://ftp-cdc.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/

und sollten mit ihren jeweiligen Unterverzeichnissen (air_temperature,
precipitation, pressure, wind) in ein Verzeichnis dwd abgelegt werden.

Sowohl historische als auch aktuelle Daten können verarbeitet werden.

Diese Software entstand als Teil des mFUND Projektes TruckInvest 4.0 (gefördert
vom BMVi) und ist freie Software gemäß der MIT License.
