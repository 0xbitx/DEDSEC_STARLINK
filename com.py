#!/usr/bin/python3

from datetime import datetime
import logging
import os
import signal
import sys
import time

import com2
import com1

COUNTER_FIELD = "end_counter"
VERBOSE_FIELD_MAP = {
    "alerts": "Alerts bit field",
    "samples": "Parsed samples",
    "end_counter": "Sample counter",
    "total_ping_drop": "Total ping drop",
    "count_full_ping_drop": "Count of drop == 1",
    "count_obstructed": "Obstructed",
    "total_obstructed_ping_drop": "Obstructed ping drop",
    "count_full_obstructed_ping_drop": "Obstructed drop == 1",
    "count_unscheduled": "Unscheduled",
    "total_unscheduled_ping_drop": "Unscheduled ping drop",
    "count_full_unscheduled_ping_drop": "Unscheduled drop == 1",
    "init_run_fragment": "Initial drop run fragment",
    "final_run_fragment": "Final drop run fragment",
    "run_seconds": "Per-second drop runs",
    "run_minutes": "Per-minute drop runs",
    "mean_all_ping_latency": "Mean RTT, drop < 1",
    "deciles_all_ping_latency": "RTT deciles, drop < 1",
    "mean_full_ping_latency": "Mean RTT, drop == 0",
    "deciles_full_ping_latency": "RTT deciles, drop == 0",
    "stdev_full_ping_latency": "RTT standard deviation, drop == 0",
    "download_usage": "Bytes downloaded",
    "upload_usage": "Bytes uploaded",
}

class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    raise Terminated


def parse_args():
    parser = com2.create_arg_parser(
        output_description="print it in text format; by default, will print in CSV format")

    group = parser.add_argument_group(title="CSV output options")
    group.add_argument("-H",
                       "--print-header",
                       action="store_true",
                       help="Print CSV header instead of parsing data")
    group.add_argument("-O",
                       "--out-file",
                       default="-",
                       help="Output file path; if set, can also be used to resume from prior "
                       "history sample counter, default: write to standard output")
    group.add_argument("-k",
                       "--skip-query",
                       action="store_true",
                       help="Skip querying for prior sample write point in history modes")

    opts = com2.run_arg_parser(parser)

    if (opts.history_stats_mode or opts.status_mode) and opts.bulk_mode and not opts.verbose:
        parser.error("bulk_history cannot be combined with other modes for CSV output")

    if opts.history_stats_mode and opts.status_mode and not opts.verbose and opts.poll_loops > 1:
        parser.error("usage of --poll-loops with history stats modes cannot be mixed with status "
                     "modes for CSV output")

    opts.skip_query |= opts.no_counter | opts.verbose
    if opts.out_file == "-":
        opts.no_stdout_errors = True

    return opts


def open_out_file(opts, mode):
    if opts.out_file == "-":
        return os.fdopen(sys.stdout.fileno(), "w", buffering=1, closefd=False)
    return open(opts.out_file, mode, buffering=1)


def print_header(opts, print_file):
    header = ["datetimestamp_utc"]

    def header_add(names):
        for name in names:
            name, start, end = com2.BRACKETS_RE.match(name).group(1, 4, 5)
            if start:
                header.extend(name + "_" + str(x) for x in range(int(start), int(end)))
            elif end:
                header.extend(name + "_" + str(x) for x in range(int(end)))
            else:
                header.append(name)

    if opts.status_mode:
        if opts.pure_status_mode:
            context = starlink_grpc.ChannelContext(target=opts.target)
            try:
                name_groups = starlink_grpc.status_field_names(context=context)
            except starlink_grpc.GrpcError as e:
                com2.conn_error(opts, "Failure reflecting status field names: %s", str(e))
                return 1
            if "status" in opts.mode:
                header_add(name_groups[0])
            if "obstruction_detail" in opts.mode:
                header_add(name_groups[1])
            if "alert_detail" in opts.mode:
                header_add(name_groups[2])
        if "location" in opts.mode:
            header_add(starlink_grpc.location_field_names())

    if opts.bulk_mode:
        general, bulk = starlink_grpc.history_bulk_field_names()
        header_add(bulk)

    if opts.history_stats_mode:
        groups = starlink_grpc.history_stats_field_names()
        general, ping, runlen, latency, loaded, usage = groups[0:6]
        header_add(general)
        if "ping_drop" in opts.mode:
            header_add(ping)
        if "ping_run_length" in opts.mode:
            header_add(runlen)
        if "ping_latency" in opts.mode:
            header_add(latency)
        if "ping_loaded_latency" in opts.mode:
            header_add(loaded)
        if "usage" in opts.mode:
            header_add(usage)

    print(",".join(header), file=print_file)
    return 0


def get_prior_counter(opts, gstate):
    try:
        with open_out_file(opts, "r") as csv_file:
            header = csv_file.readline().split(",")
            column = header.index(COUNTER_FIELD)
            last_line = None
            for last_line in csv_file:
                pass
        if last_line is not None:
            gstate.counter_stats = int(last_line.split(",")[column])
    except (IndexError, OSError, ValueError):
        pass


def loop_body(opts, gstate, print_file, shutdown=False):
    csv_data = []

    def xform(val):
        return "" if val is None else str(val)

    def cb_data_add_item(name, val, category):
        if opts.verbose:
            csv_data.append("{0:22} {1}".format(
                VERBOSE_FIELD_MAP.get(name, name) + ":", xform(val)))
        else:
            if name == "state" and val == "DISH_UNREACHABLE":
                csv_data.extend(["", "", "", val])
            else:
                csv_data.append(xform(val))

    def cb_data_add_sequence(name, val, category, start):
        if opts.verbose:
            csv_data.append("{0:22} {1}".format(
                VERBOSE_FIELD_MAP.get(name, name) + ":",
                ", ".join(xform(subval) for subval in val)))
        else:
            csv_data.extend(xform(subval) for subval in val)

    def cb_add_bulk(bulk, count, timestamp, counter):
        if opts.verbose:
            print("Time range (UTC):      {0} -> {1}".format(
                datetime.utcfromtimestamp(timestamp).isoformat(),
                datetime.utcfromtimestamp(timestamp + count).isoformat()),
                  file=print_file)
            for key, val in bulk.items():
                print("{0:22} {1}".format(key + ":", ", ".join(xform(subval) for subval in val)),
                      file=print_file)
            if opts.loop_interval > 0.0:
                print(file=print_file)
        else:
            for i in range(count):
                timestamp += 1
                fields = [datetime.utcfromtimestamp(timestamp).isoformat()]
                fields.extend([xform(val[i]) for val in bulk.values()])
                print(",".join(fields), file=print_file)

    rc, status_ts, hist_ts = com2.get_data(opts,
                                                  gstate,
                                                  cb_data_add_item,
                                                  cb_data_add_sequence,
                                                  add_bulk=cb_add_bulk,
                                                  flush_history=shutdown)

    if opts.verbose:
        if csv_data:
            print("\n".join(csv_data), file=print_file)
            if opts.loop_interval > 0.0:
                print(file=print_file)
    else:
        if csv_data:
            timestamp = status_ts if status_ts is not None else hist_ts
            csv_data.insert(0, datetime.utcfromtimestamp(timestamp).isoformat())
            print(",".join(csv_data), file=print_file)

    return rc


def main():
    opts = parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s")

    if opts.print_header:
        try:
            with open_out_file(opts, "a") as print_file:
                rc = print_header(opts, print_file)
        except OSError as e:
            logging.error("Failed opening output file: %s", str(e))
            rc = 1
        sys.exit(rc)

    gstate = com2.GlobalState(target=opts.target)
    if opts.out_file != "-" and not opts.skip_query and opts.history_stats_mode:
        get_prior_counter(opts, gstate)

    try:
        print_file = open_out_file(opts, "a")
    except OSError as e:
        logging.error("Failed opening output file: %s", str(e))
        sys.exit(1)
    signal.signal(signal.SIGTERM, handle_sigterm)

    rc = 0
    try:
        next_loop = time.monotonic()
        while True:
            rc = loop_body(opts, gstate, print_file)
            if opts.loop_interval > 0.0:
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
            else:
                break
    except (KeyboardInterrupt, Terminated):
        pass
    finally:
        loop_body(opts, gstate, print_file, shutdown=True)
        print_file.close()
        gstate.shutdown()

    sys.exit(rc)

if __name__ == "__main__":
    main()
