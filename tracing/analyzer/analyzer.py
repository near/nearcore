#!/usr/bin/env python3
import json
import profile_schema
import sys
import trace_schema
import profile_generator


def main():
    assert len(sys.argv) == 3
    trace_input = trace_schema.TraceInput.parse(sys.argv[1])
    print(f"Parsed {len(trace_input.resource_spans)} spans")

    profile: profile_schema.Profile = profile_generator.generate(trace_input)

    with open(sys.argv[2], "w") as profile_file:
        profile_file.write(json.dumps(profile.json()))


if __name__ == '__main__':
    main()
