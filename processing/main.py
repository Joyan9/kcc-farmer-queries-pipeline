import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KCC Data Processing Pipeline")
    parser.add_argument("--job", choices=["initial", "incremental"], required=True)
    args = parser.parse_args()

    if args.job == "initial":
        from initial_load import main as initial_main
        initial_main()
    elif args.job == "incremental":
        from incremental_load import main as incremental_main
        incremental_main()
