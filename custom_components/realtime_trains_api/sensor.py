"""Support for UK train data provided by data.rtt.io (NG API)."""
from __future__ import annotations

from datetime import datetime, timedelta
import logging
import aiohttp
import pytz

import voluptuous as vol
from typing import cast

from homeassistant.components.sensor import PLATFORM_SCHEMA, SensorEntity
from homeassistant.const import (
    UnitOfTime,
    STATE_UNKNOWN,
    CONF_SCAN_INTERVAL,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.util import Throttle
import homeassistant.util.dt as dt_util

_LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOFFSET = timedelta(minutes=0)

ATTR_ATCOCODE = "atcocode"
ATTR_LOCALITY = "locality"
ATTR_REQUEST_TIME = "request_time"
ATTR_JOURNEY_START = "journey_start"
ATTR_JOURNEY_END = "journey_end"
ATTR_NEXT_TRAINS = "next_trains"
ATTR_AGGREGATE = "aggregate_data"

CONF_API_TOKEN = "token"
CONF_QUERIES = "queries"
CONF_AUTOADJUSTSCANS = "auto_adjust_scans"

CONF_START = "origin"
CONF_END = "destination"
CONF_JOURNEYDATA = "journey_data_for_next_X_trains"
CONF_SENSORNAME = "sensor_name"
CONF_TIMEOFFSET = "time_offset"
CONF_STOPS_OF_INTEREST = "stops_of_interest"

TIMEZONE = pytz.timezone('Europe/London')
STRFFORMAT = "%d-%m-%Y %H:%M"

_QUERY_SCHEME = vol.Schema(
    {
        vol.Optional(CONF_SENSORNAME): cv.string,
        vol.Required(CONF_START): cv.string,
        vol.Required(CONF_END): cv.string,
        vol.Optional(CONF_JOURNEYDATA, default=0): cv.positive_int,
        vol.Optional(CONF_TIMEOFFSET, default=DEFAULT_TIMEOFFSET):
            vol.All(cv.time_period, cv.positive_timedelta),
        vol.Optional(CONF_STOPS_OF_INTEREST): [cv.string],
    }
)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Optional(CONF_AUTOADJUSTSCANS, default=False): cv.boolean,
        vol.Required(CONF_API_TOKEN): cv.string,
        vol.Required(CONF_QUERIES): [_QUERY_SCHEME],
    }
)


class RTTTokenManager:
    """Manages JWT token exchange for the RTT NG API."""

    TOKEN_URL = "https://data.rtt.io/api/get_access_token"

    def __init__(self, refresh_token: str, client: aiohttp.ClientSession):
        self._refresh_token = refresh_token
        self._client = client
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None

    async def get_access_token(self) -> str:
        """Get a valid access token, refreshing if needed."""
        now = datetime.now(tz=pytz.UTC)
        if (self._access_token and self._token_expiry
                and now < (self._token_expiry - timedelta(seconds=60))):
            return self._access_token

        _LOGGER.debug("Refreshing RTT access token")
        async with self._client.post(
            self.TOKEN_URL,
            headers={"Authorization": f"Bearer {self._refresh_token}"}
        ) as response:
            if response.status == 200:
                data = await response.json()
                self._access_token = data["token"]
                valid_until = data.get("validUntil")
                if valid_until:
                    self._token_expiry = datetime.fromisoformat(valid_until)
                else:
                    self._token_expiry = now + timedelta(minutes=15)
                _LOGGER.debug("RTT token refreshed, valid until %s", self._token_expiry)
                return self._access_token
            else:
                body = await response.text()
                _LOGGER.error("Failed to refresh RTT token: HTTP %s - %s", response.status, body)
                raise Exception(f"Token refresh failed: HTTP {response.status}")

    def invalidate(self):
        """Force token refresh on next request."""
        self._access_token = None


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up the realtime trains sensors."""
    sensors = {}
    interval = config[CONF_SCAN_INTERVAL]
    autoadjustscans = config[CONF_AUTOADJUSTSCANS]
    refresh_token = config[CONF_API_TOKEN]
    queries = config[CONF_QUERIES]

    client = async_get_clientsession(hass)
    token_manager = RTTTokenManager(refresh_token, client)

    for query in queries:
        sensor_name = query.get(CONF_SENSORNAME, None)
        journey_start = query.get(CONF_START)
        journey_end = query.get(CONF_END)
        journey_data_for_next_X_trains = query.get(CONF_JOURNEYDATA)
        timeoffset = query.get(CONF_TIMEOFFSET)
        stops_of_interest = query.get(CONF_STOPS_OF_INTEREST, [])
        sensor = RealtimeTrainLiveTrainTimeSensor(
                sensor_name,
                token_manager,
                journey_start,
                journey_end,
                journey_data_for_next_X_trains,
                timeoffset,
                autoadjustscans,
                stops_of_interest,
                interval,
                client
            )
        sensors[sensor.name] = sensor

    async_add_entities(sensors.values(), True)


class RealtimeTrainLiveTrainTimeSensor(SensorEntity):
    """
    Sensor that reads the RTT NG API at data.rtt.io.

    Provides comprehensive train data for UK trains via JSON API
    with JWT Bearer authentication.
    """

    API_BASE = "https://data.rtt.io"
    _attr_icon = "mdi:train"
    _attr_native_unit_of_measurement = UnitOfTime.MINUTES

    def __init__(self, sensor_name, token_manager, journey_start, journey_end,
                journey_data_for_next_X_trains, timeoffset, autoadjustscans,
                stops_of_interest, interval, client):
        """Construct a live train time sensor."""
        default_sensor_name = (
            f"Next train from {journey_start} to {journey_end} ({timeoffset})"
            if (timeoffset.total_seconds() > 0)
            else f"Next train from {journey_start} to {journey_end}")

        self._journey_start = journey_start
        self._journey_end = journey_end
        self._journey_data_for_next_X_trains = journey_data_for_next_X_trains
        self._next_trains = []
        self._aggregate_data = {}
        self._data = {}
        self._token_manager = token_manager
        self._timeoffset = timeoffset
        self._autoadjustscans = autoadjustscans
        self._stops_of_interest = stops_of_interest
        self._interval = interval
        self._client = client

        self._name = default_sensor_name if sensor_name is None else sensor_name
        self._state = None

        self.async_update = self._async_update

    async def _async_update(self):
        """Get the latest live departure data for the specified stop."""
        await self._getdepartures_api_request()
        self._next_trains = []
        departureCount = 0
        now = cast(datetime, dt_util.now()).astimezone(TIMEZONE)

        nextDepartureEstimatedTs: (datetime | None) = None

        services = ([] if not self._data or self._data.get("services") is None
                    else self._data["services"])

        for service in services:
            sched_meta = service.get("scheduleMetadata", {})
            if not sched_meta.get("inPassengerService", False):
                continue

            temporal = service.get("temporalData", {})
            dep_data = temporal.get("departure", {})

            is_cancelled = dep_data.get("isCancelled", False)

            sched_str = dep_data.get("scheduleAdvertised")
            if not sched_str:
                continue
            scheduledTs = datetime.fromisoformat(sched_str).astimezone(TIMEZONE)

            if _delta_secs(scheduledTs, now) < self._timeoffset.total_seconds():
                continue

            if is_cancelled:
                estimatedTs = scheduledTs
            else:
                est_str = (dep_data.get("realtimeForecast")
                           or dep_data.get("realtimeActual")
                           or sched_str)
                estimatedTs = datetime.fromisoformat(est_str).astimezone(TIMEZONE)

                if nextDepartureEstimatedTs is None:
                    nextDepartureEstimatedTs = estimatedTs
                else:
                    nextDepartureEstimatedTs = min(nextDepartureEstimatedTs, estimatedTs)

            departureCount += 1

            origin_list = service.get("origin", [])
            dest_list = service.get("destination", [])
            loc_meta = service.get("locationMetadata", {})
            platform_data = loc_meta.get("platform", {})

            planned_platform = platform_data.get("planned")
            forecast_platform = platform_data.get("forecast") or planned_platform

            train = {
                "origin_name": (origin_list[0]["location"]["description"]
                                if origin_list else "Unknown"),
                "destination_name": (dest_list[0]["location"]["description"]
                                     if dest_list else "Unknown"),
                "service_uid": sched_meta.get("identity", ""),
                "headcode": sched_meta.get("trainReportingIdentity", ""),
                "scheduled": scheduledTs.strftime(STRFFORMAT),
                "estimated": estimatedTs.strftime(STRFFORMAT),
                "delay": _delta_secs(estimatedTs, scheduledTs) // 60,
                "minutes": _delta_secs(estimatedTs, now) // 60,
                "platform": forecast_platform,
                "platform_changed": (planned_platform != forecast_platform
                                     if planned_platform and forecast_platform else False),
                "carriages": loc_meta.get("numberOfVehicles"),
                "operator_name": sched_meta.get("operator", {}).get("name", "Unknown"),
                "is_cancelled": is_cancelled,
                "cancellation_reason": dep_data.get("cancellationReasonCode", ""),
                "service_url": f"https://www.realtimetrains.co.uk/service/gb-nr:{sched_meta.get('identity', '')}/{sched_meta.get('departureDate', '')}/detailed",
                "calling_points": [],
            }

            if departureCount > self._journey_data_for_next_X_trains:
                break

            if not is_cancelled:
                await self._add_journey_data(train, scheduledTs, estimatedTs, sched_meta)
            self._next_trains.append(train)

        self._aggregate_data = await self._calculate_aggregates()

        if nextDepartureEstimatedTs is None:
            self._state = None
        else:
            self._state = _delta_secs(nextDepartureEstimatedTs, now) // 60

        if self._autoadjustscans:
            if nextDepartureEstimatedTs is None:
                self.async_update = Throttle(timedelta(minutes=30))(self._async_update)
            else:
                self.async_update = self._async_update

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def native_value(self):
        """Return the state of the sensor."""
        return self._state

    async def _getdepartures_api_request(self):
        """Fetch departures from the NG location endpoint."""
        try:
            token = await self._token_manager.get_access_token()
        except Exception:
            _LOGGER.error("Could not obtain RTT access token")
            return

        url = f"{self.API_BASE}/gb-nr/location"
        params = {"code": self._journey_start, "filterTo": self._journey_end}
        headers = {"Authorization": f"Bearer {token}"}

        try:
            async with self._client.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    self._data = await response.json()
                elif response.status == 401:
                    _LOGGER.warning("RTT API auth failed, invalidating token")
                    self._token_manager.invalidate()
                elif response.status == 429:
                    _LOGGER.warning("RTT API rate limited")
                else:
                    _LOGGER.warning("RTT API error: HTTP %s", response.status)
        except aiohttp.ClientError as err:
            _LOGGER.error("Error fetching RTT departures: %s", err)

    async def _add_journey_data(self, train, scheduled_departure, estimated_departure,
                                sched_meta):
        """Fetch service detail for arrival times at destination."""
        try:
            token = await self._token_manager.get_access_token()
        except Exception:
            return

        identity = sched_meta.get("identity", train["service_uid"])
        dep_date = sched_meta.get("departureDate",
                                  scheduled_departure.strftime("%Y-%m-%d"))

        url = f"{self.API_BASE}/gb-nr/service"
        params = {"identity": identity, "departureDate": dep_date}
        headers = {"Authorization": f"Bearer {token}"}

        try:
            async with self._client.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    stopsOfInterest = []
                    stopCount = -1
                    found = False

                    locations = data.get("service", {}).get("locations", [])
                    calling_points = []

                    for stop in locations:
                        location = stop.get("location", {})
                        short_codes = location.get("shortCodes", [])
                        temporal = stop.get("temporalData", {})
                        display_as = temporal.get("displayAs", "")
                        loc_meta_stop = stop.get("locationMetadata", {})
                        plat_stop = loc_meta_stop.get("platform", {})

                        # Build calling point entry for every public stop
                        if display_as in ("CALL", "CANCELLED_CALL"):
                            arr_data = temporal.get("arrival", {})
                            dep_data_stop = temporal.get("departure", {})
                            # Use departure time for origin, arrival for others
                            time_data = arr_data if arr_data.get("scheduleAdvertised") else dep_data_stop
                            sched_time = time_data.get("scheduleAdvertised", "")
                            actual_time = time_data.get("realtimeActual", "")
                            forecast_time = time_data.get("realtimeForecast", "")

                            cp = {
                                "code": short_codes[0] if short_codes else "",
                                "name": location.get("description", ""),
                                "scheduled": sched_time[11:16] if sched_time else "",
                                "expected": (actual_time or forecast_time or sched_time)[11:16] if (actual_time or forecast_time or sched_time) else "",
                                "platform": plat_stop.get("forecast") or plat_stop.get("planned") or "",
                                "passed": bool(actual_time),
                                "cancelled": "CANCELLED" in display_as,
                            }
                            calling_points.append(cp)

                        # Existing destination matching logic
                        if (self._journey_end in short_codes
                                and display_as != "ORIGIN"):
                            arr_data = temporal.get("arrival", {})
                            sched_arr_str = arr_data.get("scheduleAdvertised")
                            est_arr_str = (arr_data.get("realtimeForecast")
                                          or arr_data.get("realtimeActual")
                                          or sched_arr_str)
                            if not sched_arr_str:
                                stopCount += 1
                                continue

                            scheduled_arrival = datetime.fromisoformat(
                                sched_arr_str).astimezone(TIMEZONE)
                            estimated_arrival = datetime.fromisoformat(
                                est_arr_str).astimezone(TIMEZONE)

                            status = "OK"
                            if "CANCELLED" in display_as:
                                status = "Cancelled"
                            elif estimated_arrival > scheduled_arrival:
                                status = "Delayed"

                            train.update({
                                "stops_of_interest": stopsOfInterest,
                                "scheduled_arrival": scheduled_arrival.strftime(STRFFORMAT),
                                "estimate_arrival": estimated_arrival.strftime(STRFFORMAT),
                                "arrival_delay": _delta_secs(estimated_arrival, scheduled_arrival) // 60,
                                "journey_time_mins": _delta_secs(estimated_arrival, estimated_departure) // 60,
                                "stops": stopCount,
                                "status": status
                            })
                            found = True

                        elif (any(c in self._stops_of_interest for c in short_codes)
                              and temporal.get("scheduledCallType", "").startswith("ADVERTISED")):
                            arr_data = temporal.get("arrival", {})
                            sched_stop_str = arr_data.get("scheduleAdvertised")
                            est_stop_str = (arr_data.get("realtimeForecast")
                                           or arr_data.get("realtimeActual")
                                           or sched_stop_str)
                            if sched_stop_str:
                                scheduled_stop = datetime.fromisoformat(
                                    sched_stop_str).astimezone(TIMEZONE)
                                estimated_stop = datetime.fromisoformat(
                                    est_stop_str).astimezone(TIMEZONE)
                                stopsOfInterest.append({
                                    "stop": short_codes[0] if short_codes else "",
                                    "name": location.get("description", ""),
                                    "scheduled_stop": scheduled_stop.strftime(STRFFORMAT),
                                    "estimate_stop": estimated_stop.strftime(STRFFORMAT),
                                    "stop_delay": _delta_secs(estimated_stop, scheduled_stop) // 60,
                                    "journey_time_mins": _delta_secs(estimated_stop, estimated_departure) // 60,
                                    "stops": stopCount
                                })

                        stopCount += 1

                    train["calling_points"] = calling_points

                    if not found:
                        _LOGGER.warning("Could not find %s in stops for service %s",
                                        self._journey_end, identity)
                else:
                    _LOGGER.warning("RTT service detail error: HTTP %s", response.status)
        except aiohttp.ClientError as err:
            _LOGGER.error("Error fetching RTT service detail: %s", err)

    async def _calculate_aggregates(self):
        """Calculate aggregate delay and duration data."""
        departure_delays = []
        arrival_delays = []
        stop_delays = []
        durations = []
        agg_delays = {}
        for train in self._next_trains:
            if 'journey_time_mins' in train:
                durations.append(train['journey_time_mins'])
            if 'delay' in train and train['delay'] > 0:
                departure_delays.append(train['delay'])
            if 'arrival_delay' in train and train['arrival_delay'] > 0:
                arrival_delays.append(train['arrival_delay'])
            if 'stop_delay' in train and train['stop_delay'] > 0:
                stop_delays.append(train['stop_delay'])

        if departure_delays:
            agg_delays['departure'] = {
                'count': len(departure_delays),
                'min': min(departure_delays),
                'max': max(departure_delays),
                'average': round(sum(departure_delays) / len(departure_delays))
            }
        if arrival_delays:
            agg_delays['arrival'] = {
                'count': len(arrival_delays),
                'min': min(arrival_delays),
                'max': max(arrival_delays),
                'average': round(sum(arrival_delays) / len(arrival_delays))
            }
        if stop_delays:
            agg_delays['stop'] = {
                'count': len(stop_delays),
                'min': min(stop_delays),
                'max': max(stop_delays),
                'average': round(sum(stop_delays) / len(stop_delays))
            }

        durations_data = None
        if durations:
            durations_data = {
                'count': len(durations),
                'min': min(durations),
                'max': max(durations),
                'average': round(sum(durations) / len(durations)),
            }

        return {
            'delays': agg_delays,
            'durations': durations_data,
        }

    @property
    def extra_state_attributes(self):
        """Return other details about the sensor state."""
        attrs = {}
        if self._data is not None:
            attrs[ATTR_JOURNEY_START] = self._journey_start
            attrs[ATTR_JOURNEY_END] = self._journey_end
            if self._next_trains:
                attrs[ATTR_NEXT_TRAINS] = self._next_trains
                attrs[ATTR_AGGREGATE] = self._aggregate_data
            return attrs


def _delta_secs(dt_a: datetime, dt_b: datetime) -> float:
    """Calculate time delta in seconds between two datetimes."""
    return (dt_a - dt_b).total_seconds()
