import json
import argparse
from google.cloud import bigquery


def load_schema(schema_path):
    if schema_path:
        with open(schema_path, 'r') as schemaFile:
            return json.loads(schemaFile.read())


def du_field(field_du, from_du):
    du_query = "SELECT {} FROM {}".format(
        field_du, '[{}]'.format(from_du) if job_config.use_legacy_sql else '`{}`'.format(from_du)
    )

    query_job = client.query(
        query=du_query, job_config=job_config,
    )

    du_size = query_job.total_bytes_processed

    return du_size


def __travel_fields__(root, traveled_path, traveled_depth, stop_depth, field_path_separator):
    if 0 < stop_depth <= traveled_depth:
        return

    for f in root:
        field_name = f.get('name', None)
        if not field_name:
            continue

        field_type = f.get('type', None)
        is_field_record = field_type.lower() == "record"
        field_path = '{}{}{}'.format(traveled_path, field_path_separator, field_name)

        field_size = du_field(
            field_path + '.*' if is_field_record else field_path,
            table_name
        )

        yield field_path, \
            traveled_depth if is_field_record else 'L', field_size

        if is_field_record:
            for p in __travel_fields__(f.get("fields", None), field_path, traveled_depth + 1, stop_depth, '.'):
                yield p


def travel_fields(root, stop_depth=-1):
    if not isinstance(root, list):
        return

    return __travel_fields__(
        root,
        "",
        0,
        stop_depth,
        ''
    )


def human_readable_size(size):
    resp = '{}B'.format(size)
    for fmt in [[10, 'KB'], [10, 'MB'], [10, 'GB'], [10, 'TB']]:
        size = size >> fmt[0]
        if not size:
            break

        resp = '{}{}'.format(size, fmt[1])

    return resp


def csv_size(size):
    return '{}'.format(size)


def raw_size(size):
    return '{}B'.format(size)


def raw_output_formatter(out, options):
    fmt = raw_size
    pad = BYTES_PADDING

    if 'h' in options:
        pad = HUMAN_PADDING
        fmt = human_readable_size

    for field, _, size in out:
        print('{}\t{}'.format(fmt(size).rjust(pad), field))


def csv_output_formatter(out, options):
    fmt = csv_size

    if 'h' in options:
        fmt = human_readable_size

    print('field,level,size')
    for field, level, size in out:
        print('{},{},{}'.format(field, level, fmt(size)))


OUTPUT_FORMATTERS = {
    'csv': csv_output_formatter,
    'raw': raw_output_formatter,
}

args_parser = argparse.ArgumentParser()
BYTES_PADDING = 15
HUMAN_PADDING = 5
table_name = ''

if __name__ == "__main__":
    args_parser.add_argument(
        '--table_name', '--table_name', help='BQ table name clause.'
    )

    args_parser.add_argument(
        '--schema', '--schema', required=True, help='Path to a BQ schema file.'
    )

    args_parser.add_argument(
        '-d', '--depth', help='Display an entry for all fields depth records deep.'
    )

    args_parser.add_argument(
        '--format', '--format', default='raw', help='Output format. Either CSV or raw. Default is raw.'
    )

    args_parser.add_argument(
        '--use_legacy_sql', action='store_true', default=False, help='Use legacy SQL. Default is false.'
    )

    args_parser.add_argument(
        '--human_readable', '--human_readable', action='store_true', default=False, help='"Human-readable" output.'
    )

    args = args_parser.parse_args()

    bq_schema = None

    try:
        bq_schema = load_schema(args.schema)
    except Exception as e:
        bq_schema = None
        print('Failed to load BQ schema. Error: {}'.format(e))

    if not bq_schema:
        print('Empty BQ schema. Exiting ...')
        exit(1)

    if "fields" not in bq_schema:
        print('Invalid BQ schema. Missing fields. Exiting ...')
        exit(1)

    legacy = bool(
        args.use_legacy_sql or False
    )

    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_legacy_sql=legacy,
        use_query_cache=False,
    )

    max_depth = args.depth or -1
    table_name = args.table_name
    out_format = args.format or 'raw'

    formatting_opts = []
    if args.human_readable:
        formatting_opts.append('h')

    OUTPUT_FORMATTERS.get(out_format)(
        travel_fields(bq_schema.get("fields"), max_depth),
        formatting_opts
    )
