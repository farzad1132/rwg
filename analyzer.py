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
        window_df = df[(df['timestamp'] >= filtered_start) & (df['timestamp'] <= filtered_end)]

    # convert latency to milliseconds (input is microseconds)
    if 'latency' not in window_df.columns:
        raise ValueError("DataFrame must contain a 'latency' column")
    window_df['latency_ms'] = window_df['latency'].astype(float) / 1000.0

    # error flag: treat non-empty error string OR status_code == 0 as error
    window_df['is_error'] = False
    if 'error' in window_df.columns:
        window_df['is_error'] = window_df['error'].astype(str).str.len() > 0
    """ if 'status_code' in window_df.columns:
        window_df.loc[window_df['status_code'] == 0, 'is_error'] = True """

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
    if error_count > 0 or success_code == 0:
        # print list of unique errors
        error_list = window_df[window_df['is_error']][['error']].drop_duplicates()
        print(f"Unique errors ({len(error_list)}):")
        print(error_list.to_string(index=False))
        raise ValueError(f"Found {error_count} errors in the data; see above for details.")

    # SLO checks among successes
    slo_ms = slo / 1000.0
    slo_violations_count = int((success_df['latency_ms'] > float(slo_ms)).sum())
    goodput_count = int((success_df['latency_ms'] <= float(slo_ms)).sum())

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

    