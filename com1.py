
from itertools import chain
import math
import statistics
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, get_type_hints
from typing_extensions import TypedDict, get_args

import grpc

try:
    from yagrc import importer
    importer.add_lazy_packages(["spacex.api.device"])
    imports_pending = True
except (ImportError, AttributeError):
    imports_pending = False

from spacex.api.device import device_pb2
from spacex.api.device import device_pb2_grpc
from spacex.api.device import dish_pb2

REQUEST_TIMEOUT = 10

HISTORY_FIELDS = ("pop_ping_drop_rate", "pop_ping_latency_ms", "downlink_throughput_bps",
                  "uplink_throughput_bps")

StatusDict = TypedDict(
    "StatusDict", {
        "id": str,
        "hardware_version": str,
        "software_version": str,
        "state": str,
        "uptime": int,
        "snr": Optional[float],
        "seconds_to_first_nonempty_slot": float,
        "pop_ping_drop_rate": float,
        "downlink_throughput_bps": float,
        "uplink_throughput_bps": float,
        "pop_ping_latency_ms": float,
        "alerts": int,
        "fraction_obstructed": float,
        "currently_obstructed": bool,
        "seconds_obstructed": Optional[float],
        "obstruction_duration": Optional[float],
        "obstruction_interval": Optional[float],
        "direction_azimuth": float,
        "direction_elevation": float,
        "is_snr_above_noise_floor": bool,
    })

ObstructionDict = TypedDict(
    "ObstructionDict", {
        "wedges_fraction_obstructed[]": Sequence[Optional[float]],
        "raw_wedges_fraction_obstructed[]": Sequence[Optional[float]],
        "valid_s": float,
    })

AlertDict = Dict[str, bool]

LocationDict = TypedDict("LocationDict", {
    "latitude": Optional[float],
    "longitude": Optional[float],
    "altitude": Optional[float],
})

HistGeneralDict = TypedDict("HistGeneralDict", {
    "samples": int,
    "end_counter": int,
})

HistBulkDict = TypedDict(
    "HistBulkDict", {
        "pop_ping_drop_rate": Sequence[float],
        "pop_ping_latency_ms": Sequence[Optional[float]],
        "downlink_throughput_bps": Sequence[float],
        "uplink_throughput_bps": Sequence[float],
        "snr": Sequence[Optional[float]],
        "scheduled": Sequence[Optional[bool]],
        "obstructed": Sequence[Optional[bool]],
    })

PingDropDict = TypedDict(
    "PingDropDict", {
        "total_ping_drop": float,
        "count_full_ping_drop": int,
        "count_obstructed": int,
        "total_obstructed_ping_drop": float,
        "count_full_obstructed_ping_drop": int,
        "count_unscheduled": int,
        "total_unscheduled_ping_drop": float,
        "count_full_unscheduled_ping_drop": int,
    })

PingDropRlDict = TypedDict(
    "PingDropRlDict", {
        "init_run_fragment": int,
        "final_run_fragment": int,
        "run_seconds[1,]": Sequence[int],
        "run_minutes[1,]": Sequence[int],
    })

PingLatencyDict = TypedDict(
    "PingLatencyDict", {
        "mean_all_ping_latency": float,
        "deciles_all_ping_latency[]": Sequence[float],
        "mean_full_ping_latency": float,
        "deciles_full_ping_latency[]": Sequence[float],
        "stdev_full_ping_latency": Optional[float],
    })

LoadedLatencyDict = TypedDict(
    "LoadedLatencyDict", {
        "load_bucket_samples[]": Sequence[int],
        "load_bucket_min_latency[]": Sequence[Optional[float]],
        "load_bucket_median_latency[]": Sequence[Optional[float]],
        "load_bucket_max_latency[]": Sequence[Optional[float]],
    })

UsageDict = TypedDict("UsageDict", {
    "download_usage": int,
    "upload_usage": int,
})

_FIELD_NAME_MAP = {
    "wedges_fraction_obstructed[]": "wedges_fraction_obstructed[12]",
    "raw_wedges_fraction_obstructed[]": "raw_wedges_fraction_obstructed[12]",
    "run_seconds[1,]": "run_seconds[1,61]",
    "run_minutes[1,]": "run_minutes[1,61]",
    "deciles_all_ping_latency[]": "deciles_all_ping_latency[11]",
    "deciles_full_ping_latency[]": "deciles_full_ping_latency[11]",
    "load_bucket_samples[]": "load_bucket_samples[15]",
    "load_bucket_min_latency[]": "load_bucket_min_latency[15]",
    "load_bucket_median_latency[]": "load_bucket_median_latency[15]",
    "load_bucket_max_latency[]": "load_bucket_max_latency[15]",
}


def _field_names(hint_type):
    return list(_FIELD_NAME_MAP.get(key, key) for key in get_type_hints(hint_type))


def _field_names_bulk(hint_type):
    return list(key + "[]" for key in get_type_hints(hint_type))


def _field_types(hint_type):
    def xlate(value):
        while not isinstance(value, type):
            args = get_args(value)
            value = args[0] if args[0] is not type(None) else args[1]
        return value

    return list(xlate(val) for val in get_type_hints(hint_type).values())


def resolve_imports(channel: grpc.Channel):
    importer.resolve_lazy_imports(channel)
    global imports_pending
    imports_pending = False


class GrpcError(Exception):
    def __init__(self, e, *args, **kwargs):
        if isinstance(e, grpc.Call):
            msg = e.details()
        elif isinstance(e, grpc.RpcError):
            msg = "Unknown communication or service error"
        elif isinstance(e, (AttributeError, IndexError, TypeError, ValueError)):
            msg = "Protocol error"
        else:
            msg = str(e)
        super().__init__(msg, *args, **kwargs)


class UnwrappedHistory:

    unwrapped: bool


class ChannelContext:
    def __init__(self, target: Optional[str] = None) -> None:
        self.channel = None
        self.target = "192.168.100.1:9200" if target is None else target

    def get_channel(self) -> Tuple[grpc.Channel, bool]:
        reused = True
        if self.channel is None:
            self.channel = grpc.insecure_channel(self.target)
            reused = False
        return self.channel, reused

    def close(self) -> None:
        if self.channel is not None:
            self.channel.close()
        self.channel = None


def call_with_channel(function, *args, context: Optional[ChannelContext] = None, **kwargs):
    if context is None:
        with grpc.insecure_channel("192.168.100.1:9200") as channel:
            return function(channel, *args, **kwargs)

    while True:
        channel, reused = context.get_channel()
        try:
            return function(channel, *args, **kwargs)
        except grpc.RpcError:
            context.close()
            if not reused:
                raise


def status_field_names(context: Optional[ChannelContext] = None):
    if imports_pending:
        try:
            call_with_channel(resolve_imports, context=context)
        except grpc.RpcError as e:
            raise GrpcError(e) from e
    alert_names = []
    try:
        for field in dish_pb2.DishAlerts.DESCRIPTOR.fields:
            alert_names.append("alert_" + field.name)
    except AttributeError:
        pass

    return _field_names(StatusDict), _field_names(ObstructionDict), alert_names


def status_field_types(context: Optional[ChannelContext] = None):
    if imports_pending:
        try:
            call_with_channel(resolve_imports, context=context)
        except grpc.RpcError as e:
            raise GrpcError(e) from e
    num_alerts = 0
    try:
        num_alerts = len(dish_pb2.DishAlerts.DESCRIPTOR.fields)
    except AttributeError:
        pass
    return (_field_types(StatusDict), _field_types(ObstructionDict), [bool] * num_alerts)


def get_status(context: Optional[ChannelContext] = None):
    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_status={}), timeout=REQUEST_TIMEOUT)
        return response.dish_get_status

    return call_with_channel(grpc_call, context=context)


def get_id(context: Optional[ChannelContext] = None) -> str:

    try:
        status = get_status(context)
        return status.device_info.id
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e


def status_data(
        context: Optional[ChannelContext] = None) -> Tuple[StatusDict, ObstructionDict, AlertDict]:

    try:
        status = get_status(context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e

    try:
        if status.HasField("outage"):
            if status.outage.cause == dish_pb2.DishOutage.Cause.NO_SCHEDULE:
                state = "SEARCHING"
            else:
                try:
                    state = dish_pb2.DishOutage.Cause.Name(status.outage.cause)
                except ValueError:
                    state = str(status.outage.cause)
        else:
            state = "CONNECTED"
    except (AttributeError, ValueError):
        state = "UNKNOWN"

    alerts = {}
    alert_bits = 0
    try:
        for field in status.alerts.DESCRIPTOR.fields:
            value = getattr(status.alerts, field.name, False)
            alerts["alert_" + field.name] = value
            if field.number < 65:
                alert_bits |= (1 if value else 0) << (field.number - 1)
    except AttributeError:
        pass

    obstruction_duration = None
    obstruction_interval = None
    obstruction_stats = getattr(status, "obstruction_stats", None)
    if obstruction_stats is not None:
        try:
            if (obstruction_stats.avg_prolonged_obstruction_duration_s > 0.0
                    and not math.isnan(obstruction_stats.avg_prolonged_obstruction_interval_s)):
                obstruction_duration = obstruction_stats.avg_prolonged_obstruction_duration_s
                obstruction_interval = obstruction_stats.avg_prolonged_obstruction_interval_s
        except AttributeError:
            pass

    device_info = getattr(status, "device_info", None)
    return {
        "id": getattr(device_info, "id", None),
        "hardware_version": getattr(device_info, "hardware_version", None),
        "software_version": getattr(device_info, "software_version", None),
        "state": state,
        "uptime": getattr(getattr(status, "device_state", None), "uptime_s", None),
        "snr": None,  # obsoleted in grpc service
        "seconds_to_first_nonempty_slot": getattr(status, "seconds_to_first_nonempty_slot", None),
        "pop_ping_drop_rate": getattr(status, "pop_ping_drop_rate", None),
        "downlink_throughput_bps": getattr(status, "downlink_throughput_bps", None),
        "uplink_throughput_bps": getattr(status, "uplink_throughput_bps", None),
        "pop_ping_latency_ms": getattr(status, "pop_ping_latency_ms", None),
        "alerts": alert_bits,
        "fraction_obstructed": getattr(obstruction_stats, "fraction_obstructed", None),
        "currently_obstructed": getattr(obstruction_stats, "currently_obstructed", None),
        "seconds_obstructed": None,  # obsoleted in grpc service
        "obstruction_duration": obstruction_duration,
        "obstruction_interval": obstruction_interval,
        "direction_azimuth": getattr(status, "boresight_azimuth_deg", None),
        "direction_elevation": getattr(status, "boresight_elevation_deg", None),
        "is_snr_above_noise_floor": getattr(status, "is_snr_above_noise_floor", None),
    }, {
        "wedges_fraction_obstructed[]": [None] * 12,  # obsoleted in grpc service
        "raw_wedges_fraction_obstructed[]": [None] * 12,  # obsoleted in grpc service
        "valid_s": getattr(obstruction_stats, "valid_s", None),
    }, alerts


def location_field_names():

    return _field_names(LocationDict)


def location_field_types():

    return _field_types(LocationDict)


def get_location(context: Optional[ChannelContext] = None):

    def grpc_call(channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_location={}), timeout=REQUEST_TIMEOUT)
        return response.get_location

    return call_with_channel(grpc_call, context=context)


def location_data(context: Optional[ChannelContext] = None) -> LocationDict:

    try:
        location = get_location(context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        if isinstance(e, grpc.Call) and e.code() is grpc.StatusCode.PERMISSION_DENIED:
            return {
                "latitude": None,
                "longitude": None,
                "altitude": None,
            }
        raise GrpcError(e) from e

    try:
        return {
            "latitude": location.lla.lat,
            "longitude": location.lla.lon,
            "altitude": getattr(location.lla, "alt", None),
        }
    except AttributeError as e:

        raise GrpcError(e) from e


def history_bulk_field_names():

    return _field_names(HistGeneralDict), _field_names_bulk(HistBulkDict)


def history_bulk_field_types():

    return _field_types(HistGeneralDict), _field_types(HistBulkDict)


def history_ping_field_names():

    return history_stats_field_names()[0:3]


def history_stats_field_names():

    return (_field_names(HistGeneralDict), _field_names(PingDropDict), _field_names(PingDropRlDict),
            _field_names(PingLatencyDict), _field_names(LoadedLatencyDict), _field_names(UsageDict))


def history_stats_field_types():

    return (_field_types(HistGeneralDict), _field_types(PingDropDict), _field_types(PingDropRlDict),
            _field_types(PingLatencyDict), _field_types(LoadedLatencyDict), _field_types(UsageDict))


def get_history(context: Optional[ChannelContext] = None):

    def grpc_call(channel: grpc.Channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(get_history={}), timeout=REQUEST_TIMEOUT)
        return response.dish_get_history

    return call_with_channel(grpc_call, context=context)


def _compute_sample_range(history,
                          parse_samples: int,
                          start: Optional[int] = None,
                          verbose: bool = False):
    try:
        current = int(history.current)
        samples = len(history.pop_ping_drop_rate)
    except (AttributeError, TypeError):

        return range(0), 0, None

    if verbose:
        print("current counter:       " + str(current))
        print("All samples:           " + str(samples))

    if not hasattr(history, "unwrapped"):
        samples = min(samples, current)

    if verbose:
        print("Valid samples:         " + str(samples))

    if parse_samples < 0 or samples < parse_samples:
        parse_samples = samples

    if start is not None and start > current:
        if verbose:
            print("Counter reset detected, ignoring requested start count")
        start = None

    if start is None or start < current - parse_samples:
        start = current - parse_samples

    if start == current:
        return range(0), 0, current

    if hasattr(history, "unwrapped"):
        return range(samples - (current-start), samples), current - start, current

    end_offset = current % samples
    start_offset = start % samples

    sample_range: Iterable[int]
    if start_offset < end_offset:
        sample_range = range(start_offset, end_offset)
    else:
        sample_range = chain(range(start_offset, samples), range(0, end_offset))

    return sample_range, current - start, current


def concatenate_history(history1,
                        history2,
                        samples1: int = -1,
                        start1: Optional[int] = None,
                        verbose: bool = False):

    try:
        size2 = len(history2.pop_ping_drop_rate)
        new_samples = history2.current - history1.current
    except (AttributeError, TypeError):
        return history1

    if new_samples < 0:
        if verbose:
            print("Dish reboot detected. Appending anyway.")
        new_samples = history2.current if history2.current < size2 else size2
    elif new_samples > size2:
        if verbose:
            print("WARNING: Appending discontiguous samples. Polling interval probably too short.")
        new_samples = size2

    unwrapped = UnwrappedHistory()
    for field in HISTORY_FIELDS:
        if hasattr(history1, field) and hasattr(history2, field):
            setattr(unwrapped, field, [])
    unwrapped.unwrapped = True

    sample_range, ignore1, ignore2 = _compute_sample_range(  # pylint: disable=unused-variable
        history1, samples1, start=start1)
    for i in sample_range:
        for field in HISTORY_FIELDS:
            if hasattr(unwrapped, field):
                try:
                    getattr(unwrapped, field).append(getattr(history1, field)[i])
                except (IndexError, TypeError):
                    pass

    sample_range, ignore1, ignore2 = _compute_sample_range(history2, new_samples)  # pylint: disable=unused-variable
    for i in sample_range:
        for field in HISTORY_FIELDS:
            if hasattr(unwrapped, field):
                try:
                    getattr(unwrapped, field).append(getattr(history2, field)[i])
                except (IndexError, TypeError):
                    pass

    unwrapped.current = history2.current
    return unwrapped


def history_bulk_data(parse_samples: int,
                      start: Optional[int] = None,
                      verbose: bool = False,
                      context: Optional[ChannelContext] = None,
                      history=None) -> Tuple[HistGeneralDict, HistBulkDict]:

    if history is None:
        try:
            history = get_history(context)
        except (AttributeError, ValueError, grpc.RpcError) as e:
            raise GrpcError(e) from e

    sample_range, parsed_samples, current = _compute_sample_range(history,
                                                                  parse_samples,
                                                                  start=start,
                                                                  verbose=verbose)

    pop_ping_drop_rate = []
    pop_ping_latency_ms = []
    downlink_throughput_bps = []
    uplink_throughput_bps = []

    for i in sample_range:
        pop_ping_drop_rate.append(history.pop_ping_drop_rate[i])

        latency = None
        try:
            if history.pop_ping_drop_rate[i] < 1:
                latency = history.pop_ping_latency_ms[i]
        except (AttributeError, IndexError, TypeError):
            pass
        pop_ping_latency_ms.append(latency)

        downlink = None
        try:
            downlink = history.downlink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        downlink_throughput_bps.append(downlink)

        uplink = None
        try:
            uplink = history.uplink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        uplink_throughput_bps.append(uplink)

    return {
        "samples": parsed_samples,
        "end_counter": current,
    }, {
        "pop_ping_drop_rate": pop_ping_drop_rate,
        "pop_ping_latency_ms": pop_ping_latency_ms,
        "downlink_throughput_bps": downlink_throughput_bps,
        "uplink_throughput_bps": uplink_throughput_bps,
        "snr": [None] * parsed_samples,
        "scheduled": [None] * parsed_samples,
        "obstructed": [None] * parsed_samples,
    }


def history_ping_stats(parse_samples: int,
                       verbose: bool = False,
                       context: Optional[ChannelContext] = None
                       ) -> Tuple[HistGeneralDict, PingDropDict, PingDropRlDict]:
    return history_stats(parse_samples, verbose=verbose, context=context)[0:3]


def history_stats(
    parse_samples: int,
    start: Optional[int] = None,
    verbose: bool = False,
    context: Optional[ChannelContext] = None,
    history=None
) -> Tuple[HistGeneralDict, PingDropDict, PingDropRlDict, PingLatencyDict, LoadedLatencyDict,
           UsageDict]:
    if history is None:
        try:
            history = get_history(context)
        except (AttributeError, ValueError, grpc.RpcError) as e:
            raise GrpcError(e) from e

    sample_range, parsed_samples, current = _compute_sample_range(history,
                                                                  parse_samples,
                                                                  start=start,
                                                                  verbose=verbose)

    tot = 0.0
    count_full_drop = 0
    count_unsched = 0
    total_unsched_drop = 0.0
    count_full_unsched = 0
    count_obstruct = 0
    total_obstruct_drop = 0.0
    count_full_obstruct = 0

    second_runs = [0] * 60
    minute_runs = [0] * 60
    run_length = 0
    init_run_length = None

    usage_down = 0.0
    usage_up = 0.0

    rtt_full: List[float] = []
    rtt_all: List[Tuple[float, float]] = []
    rtt_buckets: List[List[float]] = [[] for _ in range(15)]

    for i in sample_range:
        d = history.pop_ping_drop_rate[i]
        if d >= 1:
            # just in case...
            d = 1
            count_full_drop += 1
            run_length += 1
        elif run_length > 0:
            if init_run_length is None:
                init_run_length = run_length
            else:
                if run_length <= 60:
                    second_runs[run_length - 1] += run_length
                else:
                    minute_runs[min((run_length-1) // 60 - 1, 59)] += run_length
            run_length = 0
        elif init_run_length is None:
            init_run_length = 0
        tot += d

        down = 0.0
        try:
            down = history.downlink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        usage_down += down

        up = 0.0
        try:
            up = history.uplink_throughput_bps[i]
        except (AttributeError, IndexError, TypeError):
            pass
        usage_up += up

        rtt = 0.0
        try:
            rtt = history.pop_ping_latency_ms[i]
        except (AttributeError, IndexError, TypeError):
            pass

        if d == 0.0:
            rtt_full.append(rtt)
            if down + up > 500000:
                rtt_buckets[min(14, int(math.log2((down+up) / 500000)))].append(rtt)
            else:
                rtt_buckets[0].append(rtt)
        if d < 1.0:
            rtt_all.append((rtt, 1.0 - d))

    if init_run_length is None:
        init_run_length = run_length
        run_length = 0

    def weighted_mean_and_quantiles(data, n):
        if not data:
            return None, [None] * (n+1)
        total_weight = sum(x[1] for x in data)
        result = []
        items = iter(data)
        value, accum_weight = next(items)
        accum_value = value * accum_weight
        for boundary in (total_weight * x / n for x in range(n)):
            while accum_weight < boundary:
                try:
                    value, weight = next(items)
                    accum_value += value * weight
                    accum_weight += weight
                except StopIteration:
                    break
            result.append(value)
        result.append(data[-1][0])
        accum_value += sum(x[0] for x in items)
        return accum_value / total_weight, result

    bucket_samples: List[int] = []
    bucket_min: List[Optional[float]] = []
    bucket_median: List[Optional[float]] = []
    bucket_max: List[Optional[float]] = []
    for bucket in rtt_buckets:
        if bucket:
            bucket_samples.append(len(bucket))
            bucket_min.append(min(bucket))
            bucket_median.append(statistics.median(bucket))
            bucket_max.append(max(bucket))
        else:
            bucket_samples.append(0)
            bucket_min.append(None)
            bucket_median.append(None)
            bucket_max.append(None)

    rtt_all.sort(key=lambda x: x[0])
    wmean_all, wdeciles_all = weighted_mean_and_quantiles(rtt_all, 10)
    rtt_full.sort()
    mean_full, deciles_full = weighted_mean_and_quantiles(tuple((x, 1.0) for x in rtt_full), 10)

    return {
        "samples": parsed_samples,
        "end_counter": current,
    }, {
        "total_ping_drop": tot,
        "count_full_ping_drop": count_full_drop,
        "count_obstructed": count_obstruct,
        "total_obstructed_ping_drop": total_obstruct_drop,
        "count_full_obstructed_ping_drop": count_full_obstruct,
        "count_unscheduled": count_unsched,
        "total_unscheduled_ping_drop": total_unsched_drop,
        "count_full_unscheduled_ping_drop": count_full_unsched,
    }, {
        "init_run_fragment": init_run_length,
        "final_run_fragment": run_length,
        "run_seconds[1,]": second_runs,
        "run_minutes[1,]": minute_runs,
    }, {
        "mean_all_ping_latency": wmean_all,
        "deciles_all_ping_latency[]": wdeciles_all,
        "mean_full_ping_latency": mean_full,
        "deciles_full_ping_latency[]": deciles_full,
        "stdev_full_ping_latency": statistics.pstdev(rtt_full) if rtt_full else None,
    }, {
        "load_bucket_samples[]": bucket_samples,
        "load_bucket_min_latency[]": bucket_min,
        "load_bucket_median_latency[]": bucket_median,
        "load_bucket_max_latency[]": bucket_max,
    }, {
        "download_usage": int(round(usage_down / 8)),
        "upload_usage": int(round(usage_up / 8)),
    }


def get_obstruction_map(context: Optional[ChannelContext] = None):

    def grpc_call(channel: grpc.Channel):
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        response = stub.Handle(device_pb2.Request(dish_get_obstruction_map={}),
                               timeout=REQUEST_TIMEOUT)
        return response.dish_get_obstruction_map

    return call_with_channel(grpc_call, context=context)


def obstruction_map(context: Optional[ChannelContext] = None):

    try:
        map_data = get_obstruction_map(context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e

    try:
        cols = map_data.num_cols
        return tuple((map_data.snr[i:i + cols]) for i in range(0, cols * map_data.num_rows, cols))
    except (AttributeError, IndexError, TypeError) as e:
        raise GrpcError(e) from e


def reboot(context: Optional[ChannelContext] = None) -> None:

    def grpc_call(channel: grpc.Channel) -> None:
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(reboot={}), timeout=REQUEST_TIMEOUT)
        # response is empty message in this case, so just ignore it

    try:
        call_with_channel(grpc_call, context=context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e


def set_stow_state(unstow: bool = False, context: Optional[ChannelContext] = None) -> None:

    def grpc_call(channel: grpc.Channel) -> None:
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(dish_stow={"unstow": unstow}), timeout=REQUEST_TIMEOUT)

    try:
        call_with_channel(grpc_call, context=context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e


def set_sleep_config(start: int,
                     duration: int,
                     enable: bool = True,
                     context: Optional[ChannelContext] = None) -> None:

    if not enable:
        start = 0
        duration = 1

    def grpc_call(channel: grpc.Channel) -> None:
        if imports_pending:
            resolve_imports(channel)
        stub = device_pb2_grpc.DeviceStub(channel)
        stub.Handle(device_pb2.Request(
            dish_power_save={
                "power_save_start_minutes": start,
                "power_save_duration_minutes": duration,
                "enable_power_save": enable
            }),
                    timeout=REQUEST_TIMEOUT)
        # response is empty message in this case, so just ignore it

    try:
        call_with_channel(grpc_call, context=context)
    except (AttributeError, ValueError, grpc.RpcError) as e:
        raise GrpcError(e) from e
