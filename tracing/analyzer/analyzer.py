#!/usr/bin/env python3
from argparse import ArgumentParser
import json
import chain_schema
import profile_generator
import profile_schema
import sys
import trace_schema

# Converts a collection of telemetry spans to Firefox profiler input format.
def main(args):
    # Parse the input JSON into trace schema objects.
    trace_input = trace_schema.TraceInput.parse(args.input_json)
    
    # Generate a chain schema objects from the trace schema.
    chain_history = chain_schema.generate(trace_input)
    
    # Generate a profile schema objects from the chain schema and write to JSON file .
    profile: profile_schema.Profile = profile_generator.generate_from_chain_schema(chain_history)
    with open(args.output_json, "w") as profile_file:
        profile_file.write(json.dumps(profile.json()))

    print(f"Done writing to {args.output_json}")


if __name__ == '__main__':
    parser = ArgumentParser(description='Generate Firefox profiler from telemetry spans.')
    parser.add_argument('--input-json',
                        type=str,
                        help='Path to the input JSON file containing the raw telemetry spans.')
    parser.add_argument('--output-json',
                        type=str,
                        help='Path to the output JSON file containing the input to the Firefox profiler.')
    args = parser.parse_args()
    main(args)
