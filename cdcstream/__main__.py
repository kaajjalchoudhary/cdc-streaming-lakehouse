"""Command-line entry point: python -m cdcstream <command>."""

from __future__ import annotations

import argparse
import logging
import sys

from cdcstream.config import Lake
from cdcstream.generate import GenerateConfig, generate

log = logging.getLogger("cdcstream")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdcstream", description="CDC streaming into Delta Lake")
    sub = parser.add_subparsers(dest="command", required=True)
    produce = sub.add_parser("produce", help="generate the change stream and publish it")
    produce.add_argument("--kafka", metavar="BOOTSTRAP", help="send to Kafka instead of landing files")
    produce.add_argument("--events", type=int, default=GenerateConfig().events)
    consume = sub.add_parser("consume", help="run the streaming job until the source is drained")
    consume.add_argument("--kafka", metavar="BOOTSTRAP", help="read from Kafka instead of landing files")
    consume.add_argument("--continuous", action="store_true", help="keep running (10 s micro-batches)")
    sub.add_parser("report", help="print a summary of the lake")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    lake = Lake.from_env()

    if args.command == "produce":
        from cdcstream import sources

        stream = generate(GenerateConfig(events=args.events))
        if args.kafka:
            sent = sources.produce_to_kafka(stream.records, args.kafka)
            log.info("sent %d records to Kafka at %s", sent, args.kafka)
        else:
            files = sources.write_files(stream.records, lake.landing)
            log.info("wrote %d records to %d files in %s", len(stream.records), files, lake.landing)
        log.info(
            "stream contains %d duplicates, %d replayed events, %d tombstones, %d poison pills",
            stream.duplicates, stream.replayed, stream.tombstones, stream.poison_pills,
        )
        return 0

    from cdcstream import report, sources
    from cdcstream.spark import get_spark
    from cdcstream.stream import run

    spark = get_spark()
    try:
        if args.command == "consume":
            source = (
                sources.kafka_stream(spark, args.kafka) if args.kafka else sources.file_stream(spark, lake.landing)
            )
            batches = run(spark, lake, source, available_now=not args.continuous)
            log.info("processed %d micro-batches", len(batches))
        print(report.build_report(spark, lake))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
