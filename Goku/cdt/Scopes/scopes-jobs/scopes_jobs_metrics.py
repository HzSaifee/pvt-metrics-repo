from goku.util.context import GokuContext
import sys

def main(argv):
    with GokuContext(argv) as ctx:
        for df in ctx.read():
            ctx.write_report(df, filename = "job_performance.csv", index = False)

def entry_point():
    raise SystemExit(main(sys.argv))

if __name__ == '__main__':
    entry_point()