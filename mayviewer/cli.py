"""MAY-viewer command-line entrypoint.

Phase 1 implements ``synth``. ``prep`` and ``serve`` land in Phases 2-3.
"""

import argparse
import logging
import sys


def _human(n: int) -> int:
    """Parse 10000000 / '10m' / '60M' / '1k' into an int."""
    s = str(n).strip().lower()
    mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
    if s and s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    return int(s)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(prog="mayviewer")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("synth", help="Generate a synthetic scaled world by tiling a real one.")
    s.add_argument("source", help="Path to a real MAY world_state.h5")
    s.add_argument("--people", required=True, help="Target population, e.g. 10m, 60M, 1000000")
    s.add_argument("--out", required=True, help="Output .h5 path")

    d = sub.add_parser("describe", help="Print the discovered schema of a MAY world (no hardcoded terms).")
    d.add_argument("world", help="Path to any MAY world_state.h5")

    pr = sub.add_parser("prep", help="Build the cached viewer artifacts (memory-flat; scales to 60M).")
    pr.add_argument("world", help="Path to any MAY world_state.h5")
    pr.add_argument("--force", action="store_true", help="Rebuild even if the cache is up to date.")

    args = p.parse_args(argv)

    if args.cmd == "describe":
        from .schema import describe
        sc = describe(args.world)
        print(f"people={sc.num_people:,}  venues={sc.num_venues:,}  geo_units={sc.num_geo_units:,}")
        print(f"geo levels      : {sc.geo_levels}")
        print(f"venue types     : {sc.venue_types}")
        print(f"subset roles    : {sc.subset_names}")
        print(f"person props    : {sc.person_properties}")
        print(f"person relations: {sc.person_relations}")
        print(f"venue props     : {sc.venue_properties}")
        print(f"geo props       : {sc.geo_properties}")
        print(f"activities      : {sc.activity_names}")
        return 0

    if args.cmd == "prep":
        from .prep.pipeline import prep
        m = prep(args.world, force=args.force)
        a = m["artifacts"]
        print(f"\n✓ cache for {m['source']['name']}")
        for lv, info in sorted(a["aggregates"].items()):
            print(f"  aggregates L{lv} {info['level_name']:6s}: {info['units']:,} units")
        dd = a["drilldown"]
        print(f"  people  {len(dd['people']['row_groups']):,} units  "
              f"venues {len(dd['venues']['row_groups']):,}  "
              f"members {len(dd['members']['row_groups']):,}")
        print(f"  hexbin  {a['hexbin']['tiles']:,} tiles zooms={a['hexbin']['zooms']}")
        print(f"  peak unit rows: {m['peak_unit_rows']}  ({m['build_seconds']}s)")
        return 0

    if args.cmd == "synth":
        from .synth import generate
        stats = generate(args.source, _human(args.people), args.out)
        print(f"\n✓ {stats['num_people']:,} people  "
              f"{stats['num_venues']:,} venues  "
              f"{stats['num_geo_units']:,} geo units  "
              f"({stats['size_bytes'] / 1e6:.1f} MB, {stats['replicas']} replicas)")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
