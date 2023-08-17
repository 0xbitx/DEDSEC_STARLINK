
import argparse
from datetime import datetime
from datetime import timezone
import logging
import re
import time
from typing import List

import grpc

import com1

BRACKETS_RE = re.compile(r"([^[]*)(\[((\d+),|)(\d*)\]|)$")
LOOP_TIME_DEFAULT = 0
STATUS_MODES: List[str] = ["status", "obstruction_detail", "alert_detail", "location"]
HISTORY_STATS_MODES: List[str] = [
    "ping_drop", "ping_run_length", "ping_latency", "ping_loaded_latency", "usage"
]
UNGROUPED_MODES: List[str] = []


def create_arg_parser(output_description, bulk_history=True):
    parser = argparse.ArgumentParser(
        description="Collect status and/or history data from a Starlink user terminal and " +
        output_description,
        epilog="Additional arguments can be read from a file by including @FILENAME as an "
        "option, where FILENAME is a path to a file that contains arguments, one per line.",
        fromfile_prefix_chars="@",
        add_help=False)

    parser.bulk_history = bulk_history

    group = parser.add_argument_group(title="General options")
    group.add_argument("-g",
                       "--target",
                       help="host:port of dish to query, default is the standard IP address "
                       "and port (192.168.100.1:9200)")
    group.add_argument("-h", "--help", action="help", help="Be helpful")
    group.add_argument("-N",
                       "--numeric",
                       action="store_true",
                       help="Record boolean values as 1 and 0 instead of True and False")
    group.add_argument("-t",
                       "--loop-interval",
                       type=float,
                       default=float(LOOP_TIME_DEFAULT),
                       help="Loop interval in seconds or 0 for no loop, default: " +
                       str(LOOP_TIME_DEFAULT))
    group.add_argument("-v", "--verbose", action="store_true", help="Be verbose")

    group = parser.add_argument_group(title="History mode options")
    group.add_argument("-a",
                       "--all-samples",
                       action="store_const",
                       const=-1,
                       dest="samples",
                       help="Parse all valid samples")
    group.add_argument("-o",
                       "--poll-loops",
                       type=int,
                       help="Poll history for N loops and aggregate data before computing history "
                       "stats; this allows for a smaller loop interval with less loss of data "
                       "when the dish reboots",
                       metavar="N")
    if bulk_history:
        sample_help = ("Number of data samples to parse; normally applies to first loop "
                       "iteration only, default: all in bulk mode, loop interval if loop "
                       "interval set, else all available samples")
        no_counter_help = ("Don't track sample counter across loop iterations in non-bulk "
                           "modes; keep using samples option value instead")
    else:
        sample_help = ("Number of data samples to parse; normally applies to first loop "
                       "iteration only, default: loop interval, if set, else all available " +
                       "samples")
        no_counter_help = ("Don't track sample counter across loop iterations; keep using "
                           "samples option value instead")
    group.add_argument("-s", "--samples", type=int, help=sample_help)
    group.add_argument("-j", "--no-counter", action="store_true", help=no_counter_help)

    return parser


def run_arg_parser(parser, need_id=False, no_stdout_errors=False, modes=None):
    if modes is None:
        modes = STATUS_MODES + HISTORY_STATS_MODES + UNGROUPED_MODES
        if parser.bulk_history:
            modes.append("bulk_history")
    parser.add_argument("mode",
                        nargs="+",
                        choices=modes,
                        help="The data group to record, one or more of: " + ", ".join(modes),
                        metavar="mode")

    opts = parser.parse_args()

    if opts.loop_interval <= 0.0 or opts.poll_loops is None:
        opts.poll_loops = 1
    elif opts.poll_loops < 2:
        parser.error("Poll loops arg must be 2 or greater to be meaningful")

    status_set = set(STATUS_MODES)
    opts.status_mode = bool(status_set.intersection(opts.mode))
    status_set.remove("location")
    opts.pure_status_mode = bool(status_set.intersection(opts.mode))
    opts.history_stats_mode = bool(set(HISTORY_STATS_MODES).intersection(opts.mode))
    opts.bulk_mode = "bulk_history" in opts.mode

    if opts.samples is None:
        opts.samples = int(opts.loop_interval) if opts.loop_interval >= 1.0 else -1
        opts.bulk_samples = -1
    else:
        opts.skip_query = True
        opts.bulk_samples = opts.samples

    opts.no_stdout_errors = no_stdout_errors
    opts.need_id = need_id

    return opts


def conn_error(opts, msg, *args):
    if opts.loop_interval > 0.0 and not opts.no_stdout_errors:
        print(msg % args)
    else:
        logging.error(msg, *args)


class GlobalState:
    def __init__(self, target=None):
        self.counter = None
        self.timestamp = None
        self.counter_stats = None
        self.timestamp_stats = None
        self.dish_id = None
        self.context = com1.ChannelContext(target=target)
        self.poll_count = 0
        self.accum_history = None
        self.first_poll = True
        self.warn_once_location = True

    def shutdown(self):
        self.context.close()


def get_data(opts, gstate, add_item, add_sequence, add_bulk=None, flush_history=False):
    if flush_history and opts.poll_loops < 2:
        return 0, None, None

    rc = 0
    status_ts = None
    hist_ts = None

    if not flush_history:
        rc, status_ts = get_status_data(opts, gstate, add_item, add_sequence)

    if opts.history_stats_mode and (not rc or opts.poll_loops > 1):
        hist_rc, hist_ts = get_history_stats(opts, gstate, add_item, add_sequence, flush_history)
        if not rc:
            rc = hist_rc

    if not flush_history and opts.bulk_mode and add_bulk and not rc:
        rc = get_bulk_data(opts, gstate, add_bulk)

    return rc, status_ts, hist_ts


def add_data_normal(data, category, add_item, add_sequence):
    for key, val in data.items():
        name, start, seq = BRACKETS_RE.match(key).group(1, 4, 5)
        if seq is None:
            add_item(name, val, category)
        else:
            add_sequence(name, val, category, int(start) if start else 0)


def add_data_numeric(data, category, add_item, add_sequence):
    for key, val in data.items():
        name, start, seq = BRACKETS_RE.match(key).group(1, 4, 5)
        if seq is None:
            add_item(name, int(val) if isinstance(val, int) else val, category)
        else:
            add_sequence(name,
                         [int(subval) if isinstance(subval, int) else subval for subval in val],
                         category,
                         int(start) if start else 0)


def get_status_data(opts, gstate, add_item, add_sequence):
    if opts.status_mode:
        timestamp = int(time.time())
        add_data = add_data_numeric if opts.numeric else add_data_normal
        if opts.pure_status_mode or opts.need_id and gstate.dish_id is None:
            try:
                groups = com1.status_data(context=gstate.context)
                status_data, obstruct_detail, alert_detail = groups[0:3]
            except com1.GrpcError as e:
                if "status" in opts.mode:
                    if opts.need_id and gstate.dish_id is None:
                        conn_error(opts, "Dish unreachable and ID unknown, so not recording state")
                        return 1, None
                    if opts.verbose:
                        print("Dish unreachable")
                    add_item("state", "DISH_UNREACHABLE", "status")
                    return 0, timestamp
                conn_error(opts, "Failure getting status: %s", str(e))
                return 1, None
            if opts.need_id:
                gstate.dish_id = status_data["id"]
                del status_data["id"]
            if "status" in opts.mode:
                add_data(status_data, "status", add_item, add_sequence)
            if "obstruction_detail" in opts.mode:
                add_data(obstruct_detail, "status", add_item, add_sequence)
            if "alert_detail" in opts.mode:
                add_data(alert_detail, "status", add_item, add_sequence)
        if "location" in opts.mode:
            try:
                location = com1.location_data(context=gstate.context)
            except com1.GrpcError as e:
                conn_error(opts, "Failure getting location: %s", str(e))
                return 1, None
            if location["latitude"] is None and gstate.warn_once_location:
                logging.warning("Location data not enabled. See README for more details.")
                gstate.warn_once_location = False
            add_data(location, "status", add_item, add_sequence)
        return 0, timestamp
    elif opts.need_id and gstate.dish_id is None:
        try:
            gstate.dish_id = com1.get_id(context=gstate.context)
        except com1.GrpcError as e:
            conn_error(opts, "Failure getting dish ID: %s", str(e))
            return 1, None
        if opts.verbose:
            print("Using dish ID: " + gstate.dish_id)

    return 0, None


def get_history_stats(opts, gstate, add_item, add_sequence, flush_history):
    if flush_history or (opts.need_id and gstate.dish_id is None):
        history = None
    else:
        try:
            timestamp = int(time.time())
            history = com1.get_history(context=gstate.context)
            gstate.timestamp_stats = timestamp
        except (AttributeError, ValueError, grpc.RpcError) as e:
            conn_error(opts, "Failure getting history: %s", str(com1.GrpcError(e)))
            history = None

    parse_samples = opts.samples if gstate.counter_stats is None else -1
    start = gstate.counter_stats if gstate.counter_stats else None

    if gstate.accum_history:
        if history is not None:
            gstate.accum_history = com1.concatenate_history(gstate.accum_history,
                                                                     history,
                                                                     samples1=parse_samples,
                                                                     start1=start,
                                                                     verbose=opts.verbose)
            if not opts.no_counter:
                gstate.counter_stats = 0
    else:
        gstate.accum_history = history

    if gstate.first_poll and gstate.accum_history:
        if opts.poll_loops > 1 and gstate.counter_stats:
            new_samples = gstate.accum_history.current - gstate.counter_stats
            if new_samples < 0:
                new_samples = gstate.accum_history.current
            if new_samples > len(gstate.accum_history.pop_ping_drop_rate):
                new_samples = len(gstate.accum_history.pop_ping_drop_rate)
            gstate.poll_count = max(gstate.poll_count, int((new_samples-1) / opts.loop_interval))
        gstate.first_poll = False

    if gstate.poll_count < opts.poll_loops - 1 and not flush_history:
        gstate.poll_count += 1
        return 0, None

    gstate.poll_count = 0

    if gstate.accum_history is None:
        return (0, None) if flush_history else (1, None)

    groups = com1.history_stats(parse_samples,
                                         start=start,
                                         verbose=opts.verbose,
                                         history=gstate.accum_history)
    general, ping, runlen, latency, loaded, usage = groups[0:6]
    add_data = add_data_numeric if opts.numeric else add_data_normal
    add_data(general, "ping_stats", add_item, add_sequence)
    if "ping_drop" in opts.mode:
        add_data(ping, "ping_stats", add_item, add_sequence)
    if "ping_run_length" in opts.mode:
        add_data(runlen, "ping_stats", add_item, add_sequence)
    if "ping_latency" in opts.mode:
        add_data(latency, "ping_stats", add_item, add_sequence)
    if "ping_loaded_latency" in opts.mode:
        add_data(loaded, "ping_stats", add_item, add_sequence)
    if "usage" in opts.mode:
        add_data(usage, "usage", add_item, add_sequence)
    if not opts.no_counter:
        gstate.counter_stats = general["end_counter"]

    timestamp = gstate.timestamp_stats
    gstate.timestamp_stats = None
    gstate.accum_history = None

    return 0, timestamp


def get_bulk_data(opts, gstate, add_bulk):
    before = time.time()

    start = gstate.counter
    parse_samples = opts.bulk_samples if start is None else -1
    try:
        general, bulk = com1.history_bulk_data(parse_samples,
                                                        start=start,
                                                        verbose=opts.verbose,
                                                        context=gstate.context)
    except com1.GrpcError as e:
        conn_error(opts, "Failure getting history: %s", str(e))
        return 1

    after = time.time()
    parsed_samples = general["samples"]
    new_counter = general["end_counter"]
    timestamp = gstate.timestamp
    if gstate.counter is not None and new_counter != gstate.counter + parsed_samples:
        timestamp = None
    if timestamp is not None and not before - 2.0 <= timestamp + parsed_samples <= after + 2.0:
        if opts.verbose:
            print("Lost sample time sync at: " +
                  str(datetime.fromtimestamp(timestamp + parsed_samples, tz=timezone.utc)))
        timestamp = None
    if timestamp is None:
        timestamp = int(before)
        if opts.verbose:
            print("Establishing new time base: {0} -> {1}".format(
                new_counter, datetime.fromtimestamp(timestamp, tz=timezone.utc)))
        timestamp -= parsed_samples

    if opts.numeric:
        add_bulk(
            {
                k: [int(subv) if isinstance(subv, int) else subv for subv in v]
                for k, v in bulk.items()
            }, parsed_samples, timestamp, new_counter - parsed_samples)
    else:
        add_bulk(bulk, parsed_samples, timestamp, new_counter - parsed_samples)

    gstate.counter = new_counter
    gstate.timestamp = timestamp + parsed_samples
    return 0
