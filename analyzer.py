#!/bin/python3

import os
import pandas as pd
import argparse
import json



def read_csv(file_path: str) -> pd.DataFrame:
    """
    csv file header:
    url,latency,status_code,error,timestamp

    latency is in microseconds.
    Empty fields are read as empty strings instead of NaN.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    try:
        # keep_default_na=False ensures empty fields stay as "" (not NaN)
        df = pd.read_csv(file_path, keep_default_na=False)
        return df
    except Exception as e:
        raise ValueError(f"Error reading the CSV file: {e}")


def http1_url_to_service(url: str) -> str:
    """
    return path from URLs from this format: http://host:port/path?query
    """

    if "://" in url:
        url = url.split("://", 1)[1]
    if "/" in url:
        url = url.split("/", 1)[1]
    if "?" in url:
        url = url.split("?", 1)[0]
    return url

def overall_report(df: pd.DataFrame, output_path: str, warmup: int = 0, cooldown: int = 0):
    """
    Generate a csv file containing overall statistics (one-row CSV).

    The following columns are produced:
    - goodput: rate (requests/sec) of successful requests with latency <= SLO
    - slo_violations: rate (requests/sec) of successful requests with latency > SLO
    - dropped_requests: rate (requests/sec) of requests with dropped status code
    - errors: rate (requests/sec) of requests that resulted in an error
    - p50_latency: 50th percentile latency in milliseconds (successful requests)
    - p90_latency: 90th percentile latency in milliseconds (successful requests)
    - total_requests: number of requests in the filtered window
    - duration_seconds: duration of the filtered window in seconds

    Notes / assumptions:
    - The DataFrame must contain columns: 'latency', 'status_code', 'error', 'timestamp', 'url'.
    - 'latency' is expected in microseconds (as produced by the runner) and is
      converted to milliseconds for percentiles and SLO checks.
    - Uses module-level globals `version` and `slo`.
    """

    # Validate inputs and set defaults
    if df is None or len(df) == 0:
        raise ValueError("DataFrame is empty")

    # ensure slo and version are available
    global slo, version
    if 'timestamp' not in df.columns:
        raise ValueError("DataFrame must contain a 'timestamp' column with RFC3339 timestamps")

    # parse timestamps
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])

    # sort by time
    df = df.sort_values('timestamp')

    start = df['timestamp'].iloc[0]
    end = df['timestamp'].iloc[-1]
    full_duration_seconds = (end - start).total_seconds()
    if full_duration_seconds <= 0:
        raise ValueError('Invalid timestamp range in DataFrame')

    # Apply warmup and cooldown trimming (in seconds)
    filtered_start = start + pd.Timedelta(seconds=warmup)
    filtered_end = end - pd.Timedelta(seconds=cooldown)

    # If trimming removes the entire window, return a mostly-empty report
    if filtered_start >= filtered_end:
        raise ValueError("Warmup and cooldown periods remove the entire data range")
    else:
        duration_seconds = (filtered_end - filtered_start).total_seconds()
        mask = (df['timestamp'] >= filtered_start) & (df['timestamp'] <= filtered_end)
        window_df = df.loc[mask].copy()

    # convert latency to milliseconds (input is microseconds)
    if 'latency' not in window_df.columns:
        raise ValueError("DataFrame must contain a 'latency' column")
    window_df.loc[:, 'latency_ms'] = window_df['latency'].astype(float) / 1000.0

    # error flag: treat non-empty error string OR status_code == 0 as error
    window_df.loc[:, 'is_error'] = False
    if 'error' in window_df.columns:
        window_df.loc[:, 'is_error'] = ~window_df['status_code'].isin([status_dict['success'][version], status_dict['dropped'][version]])

    # determine dropped and success codes based on version
    dropped_code = status_dict['dropped'][version]
    success_code = status_dict['success'][version]

    # success rows: those that are not errors and whose status_code equals success_code
    success_mask = (~window_df['is_error']) & (window_df.get('status_code') == success_code)

    success_df = window_df[success_mask]

    # Compute counts
    total_requests = len(window_df)
    success_count = len(success_df)
    dropped_count = 0
    if dropped_code is not None and 'status_code' in window_df.columns:
        dropped_count = int((window_df['status_code'] == dropped_code).sum())
    error_count = int(window_df['is_error'].sum())
    if error_count > 0:
        # print only the (unique) error column values
        error_values = window_df.loc[window_df['is_error'], 'error'].astype(str).dropna().drop_duplicates()
        for v in error_values:
            print(v)

    # SLO checks among successes
    slo_ms = slo
    slo_violations_count = int((success_df['latency_ms'] > float(slo_ms)).sum())
    goodput_count = int((success_df['latency_ms'] <= float(slo_ms)).sum())

    # check if total count matches sum of categories
    if total_requests != (success_count + dropped_count + error_count):
        raise ValueError("Total request count does not match sum of success, dropped, and error counts")
    # check if success count matches sum of goodput and slo violations
    if success_count != (goodput_count + slo_violations_count):
        raise ValueError("Success count does not match sum of goodput and SLO violations")

    # rates per second
    goodput = goodput_count / duration_seconds
    slo_violations = slo_violations_count / duration_seconds
    dropped_requests = dropped_count / duration_seconds
    errors = error_count / duration_seconds

    # latency percentiles (use successful requests; if none, use all non-error rows)
    latency_source = success_df['latency_ms']
    if latency_source.empty:
        raise ValueError("No successful requests to compute latency percentiles")
    else:
        p50 = float(latency_source.quantile(0.5))
        p95 = float(latency_source.quantile(0.95))

    # prepare output data
    out_row = {
        'goodput': goodput,
        'num_goodput': goodput_count,
        'slo_ms': slo_ms,
        'slo_violations': slo_violations,
        'num_slo_violations': slo_violations_count,
        'dropped_requests': dropped_requests,
        'num_dropped_requests': dropped_count,
        'errors': errors,
        'num_errors': error_count,
        'p50_latency': p50,
        'p95_latency': p95,
        'total_requests': total_requests,
        'duration_seconds': duration_seconds,
        'start_time': start.isoformat(),
        'end_time': end.isoformat(),
    }

    # ensure directory exists
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # write JSON
    with open(output_path, 'w') as f:
        json.dump(out_row, f, indent=2)

    return out_row

def realtime_report(df: pd.DataFrame, output_path: str, freq: int, warmup: int = 0, cooldown: int = 0):
    """
    Generate a CSV file containing time-series statistics at the specified frequency.

    The following columns are produced:
    - timestamp: start of the interval (RFC3339 format)
    - relative_time: seconds since start of the entire dataset
    - goodput: rate (requests/sec) of successful requests with latency <= SLO
    - slo_violations: rate (requests/sec) of successful requests with latency > SLO
    - dropped_requests: rate (requests/sec) of requests with dropped status code
    - errors: rate (requests/sec) of requests that resulted in an error
    - p50_latency: 50th percentile latency in milliseconds (successful requests)
    - p90_latency: 90th percentile latency in milliseconds (successful requests)
    - total_requests: number of requests in the interval

    Notes / assumptions:
    - The DataFrame must contain columns: 'latency', 'status_code', 'error', 'timestamp', 'url'.
    - 'latency' is expected in microseconds (as produced by the runner) and is
      converted to milliseconds for percentiles and SLO checks.
    - Uses module-level globals `version` and `slo`.
    - freq is in milliseconds.
    """

    # Validate inputs
    if df is None or len(df) == 0:
        raise ValueError("DataFrame is empty")

    global slo, version
    if 'timestamp' not in df.columns:
        raise ValueError("DataFrame must contain a 'timestamp' column with RFC3339 timestamps")

    # parse timestamps and sort
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df = df.sort_values('timestamp')

    start = df['timestamp'].iloc[0]
    end = df['timestamp'].iloc[-1]
    full_duration_seconds = (end - start).total_seconds()
    if full_duration_seconds <= 0:
        raise ValueError('Invalid timestamp range in DataFrame')

    # Apply warmup/cooldown trimming
    filtered_start = start + pd.Timedelta(seconds=warmup)
    filtered_end = end - pd.Timedelta(seconds=cooldown)
    if filtered_start >= filtered_end:
        raise ValueError("Warmup and cooldown periods remove the entire data range")

    mask = (df['timestamp'] >= filtered_start) & (df['timestamp'] <= filtered_end)
    window_df = df.loc[mask].copy()

    # Convert latency to milliseconds
    if 'latency' not in window_df.columns:
        raise ValueError("DataFrame must contain a 'latency' column")
    window_df.loc[:, 'latency_ms'] = window_df['latency'].astype(float) / 1000.0

    # error flag
    window_df.loc[:, 'is_error'] = False
    if 'status_code' in window_df.columns:
        window_df.loc[:, 'is_error'] = ~window_df['status_code'].isin([status_dict['success'][version], status_dict['dropped'][version]])

    # success and dropped codes
    dropped_code = status_dict['dropped'][version]
    success_code = status_dict['success'][version]

    # prepare bins
    interval_ms = int(freq)
    interval_td = pd.Timedelta(milliseconds=interval_ms)
    intervals = []
    t = filtered_start
    while t < filtered_end:
        intervals.append(t)
        t = t + interval_td
    # ensure last bin includes filtered_end as the end boundary
    if len(intervals) == 0:
        raise ValueError('No intervals generated; check freq and trimmed range')
    print(f"Generated {len(intervals)} intervals from {filtered_start} to {filtered_end} with freq {freq} ms")

    # Prepare CSV output
    import csv as _csv
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    fieldnames = [
        'timestamp',
        'relative_time',
        'goodput',
        'slo_violations',
        'dropped_requests',
        'errors',
        'p50_latency',
        'p95_latency',
        'total_requests',
    ]

    interval_seconds = interval_ms / 1000.0

    with open(output_path, 'w', newline='') as f:
        writer = _csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, start_ts in enumerate(intervals):
            if i == len(intervals) - 1:
                continue
            end_ts = start_ts + interval_td
            # include rows where timestamp >= start_ts and < end_ts, except last interval include <=
            if i == len(intervals) - 1:
                mask = (window_df['timestamp'] >= start_ts) & (window_df['timestamp'] <= filtered_end)
            else:
                mask = (window_df['timestamp'] >= start_ts) & (window_df['timestamp'] < end_ts)

            chunk = window_df[mask]
            total_rate = len(chunk) / interval_seconds
            total_requests = len(chunk)

            # compute dropped and errors
            dropped_count = int((chunk.get('status_code') == dropped_code).sum())
            error_count = int(chunk['is_error'].sum())

            # success mask
            success_mask = (~chunk['is_error']) & (chunk.get('status_code') == success_code)
            success_df = chunk[success_mask]

            # SLO in ms (slo is given in milliseconds)
            slo_ms = float(slo)
            slo_violations_count = int((success_df['latency_ms'] > slo_ms).sum())
            goodput_count = int((success_df['latency_ms'] <= slo_ms).sum())

            # check counts
            success_count = len(success_df)
            if total_requests != (success_count + dropped_count + error_count) and total_requests > 0:
                raise ValueError(f"i: {i}, Total request count: {total_requests} does not match sum of success: {success_count}, dropped: {dropped_count}, and error: {error_count} counts")
            if success_count != (goodput_count + slo_violations_count):
                raise ValueError(f"Success count: {success_count} does not match sum of goodput: {goodput_count} and SLO violations: {slo_violations_count}")

            # rates per second (use interval_seconds; if zero, set rates to 0)
            if interval_seconds <= 0:
                raise ValueError("Interval duration is zero or negative")
            else:
                goodput = goodput_count / interval_seconds
                slo_violations = slo_violations_count / interval_seconds
                dropped_requests = dropped_count / interval_seconds
                errors = error_count / interval_seconds

            # latency percentiles
            if len(success_df) > 0:
                p50 = float(success_df['latency_ms'].quantile(0.5))
                p95 = float(success_df['latency_ms'].quantile(0.95))
            else:
                p50 = 0.0
                p95 = 0.0
                print(f"[Warning] i: {i}, No successful requests in interval to compute latency percentiles")

            relative_time = (start_ts - filtered_start).total_seconds()

            row = {
                'timestamp': start_ts.isoformat(),
                'relative_time': relative_time,
                'goodput': goodput,
                'slo_violations': slo_violations,
                'dropped_requests': dropped_requests,
                'errors': errors,
                'p50_latency': p50,
                'p95_latency': p95,
                'total_requests': total_rate,
            }
            writer.writerow(row)

    return True

status_dict = {
    "success": {
        1: 200,  # HTTP/1.x
        2: 0     # HTTP/2
    },
    "dropped": {
        1: 503,  # HTTP/1.x
        2: 8     # HTTP/2
    }
}

global version
global slo

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read and display a CSV file.")
    parser.add_argument("--rwg_output", type=str, help="Path to the CSV file.")
    parser.add_argument("--overall_output", type=str, help="Path to the overall output file.")
    parser.add_argument("--realtime_output", type=str, help="Path to the overtime output file.")
    parser.add_argument("--freq", type=int, help="Frequency in milliseconds.")
    parser.add_argument("--warmup", type=int, default=0, help="Warmup duration in seconds.")
    parser.add_argument("--cooldown", type=int, default=0, help="Cooldown duration in seconds.")
    parser.add_argument("--version", type=int, help="HTTP version (1 or 2).")
    parser.add_argument("--slo", type=int, default=1000, help="SLO in milliseconds.")

    args = parser.parse_args()
    slo = args.slo

    # either realtime_output and freq must be provided, or overall_output must be provided
    if not (args.realtime_output and args.freq) and not args.overall_output:
        raise ValueError("Either realtime_output and freq must be provided, or overall_output must be provided.")
    # only one of them
    if args.realtime_output and args.freq and args.overall_output:
        raise ValueError("Only one of realtime_output/freq or overall_output should be provided.")

    if args.version not in [1, 2]:
        raise ValueError("version must be 1 or 2")
    version = args.version

    try:
        df = read_csv(args.rwg_output)
    except Exception as e:
        raise Exception(f"Failed to read CSV file: {e}")

    # If overall_output path is provided, compute overall report and write it
    if args.overall_output:
        try:
            report = overall_report(df, args.overall_output, warmup=args.warmup, cooldown=args.cooldown)
            print(f"Wrote overall report to {args.overall_output}")
        except Exception as e:
            print(f"Failed to create overall report: {e}")
    
    if args.realtime_output and args.freq:
        try:
            success = realtime_report(df, args.realtime_output, freq=args.freq, warmup=args.warmup, cooldown=args.cooldown)
            if success:
                print(f"Wrote realtime report to {args.realtime_output}")
        except Exception as e:
            print(f"Failed to create realtime report: {e}")

    