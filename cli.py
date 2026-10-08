"""Command-line entry point for the GDELT conflict forecasting pipeline.

Usage:
    python cli.py download [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--files-per-day N]
    python cli.py features [--raw-dir PATH]
    python cli.py train    [--leads 1,2,3,4]
    python cli.py predict
    python cli.py all      [--raw-dir PATH]

The web app in webapp/ calls the exact same functions; this file is just
argument parsing.
"""
from __future__ import annotations

import argparse
import sys

from src import config


def cmd_download(args) -> None:
    from src.download import download_range

    download_range(
        start=args.start,
        end=args.end,
        files_per_day=args.files_per_day,
    )


def cmd_features(args) -> None:
    from src.features import build_features
    from src.spark_session import get_spark

    build_features(get_spark(), raw_dir=args.raw_dir)


def cmd_train(args) -> None:
    from src.spark_session import get_spark
    from src.train import run_training

    leads = [int(x) for x in args.leads.split(",")] if args.leads else None
    run_training(get_spark(), leads=leads)


def cmd_predict(args) -> None:
    from src.predict import score_latest
    from src.spark_session import get_spark

    scores = score_latest(get_spark())
    top = scores.head(15)[["country", "country_name", "max_risk"]]
    print("\nTop risk countries (max probability across lead times):")
    for _, row in top.iterrows():
        print(f"  {row['country']:>3}  {row['country_name']:<28} {row['max_risk']:.3f}")


def cmd_all(args) -> None:
    if not args.skip_download:
        cmd_download(args)
    cmd_features(args)
    cmd_train(args)
    cmd_predict(args)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="GDELT conflict spike forecasting")
    sub = parser.add_subparsers(dest="command", required=True)

    p_dl = sub.add_parser("download", help="download the GDELT 2.0 event slice")
    p_dl.add_argument("--start", default=None, help=f"default {config.DEFAULT_START_DATE}")
    p_dl.add_argument("--end", default=None, help=f"default {config.DEFAULT_END_DATE}")
    p_dl.add_argument("--files-per-day", type=int, default=None,
                      help=f"of 96 daily files, default {config.FILES_PER_DAY}")
    p_dl.set_defaults(func=cmd_download)

    p_feat = sub.add_parser("features", help="build country-week features with Spark")
    p_feat.add_argument("--raw-dir", default=None,
                        help="raw events dir (use data/sample for the bundled sample)")
    p_feat.set_defaults(func=cmd_features)

    p_train = sub.add_parser("train", help="train GBT + baselines, evaluate, save metrics")
    p_train.add_argument("--leads", default=None, help="comma-separated lead weeks, e.g. 1,2")
    p_train.set_defaults(func=cmd_train)

    p_pred = sub.add_parser("predict", help="score the latest week into risk_scores.csv")
    p_pred.set_defaults(func=cmd_predict)

    p_all = sub.add_parser("all", help="download + features + train + predict")
    p_all.add_argument("--start", default=None)
    p_all.add_argument("--end", default=None)
    p_all.add_argument("--files-per-day", type=int, default=None)
    p_all.add_argument("--raw-dir", default=None)
    p_all.add_argument("--leads", default=None)
    p_all.add_argument("--skip-download", action="store_true")
    p_all.set_defaults(func=cmd_all)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
