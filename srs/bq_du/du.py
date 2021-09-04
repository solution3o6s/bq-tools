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


def __travel_fields__(root, traveled_path, traveled_depth, field_path_separator):
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
            for p in __travel_fields__(f.get("fields", None), field_path, traveled_depth + 1, '.'):
                yield p


def travel_fields(root, path):
    if not isinstance(root, list):
        return

    return __travel_fields__(
        root,
        path,
        0,
        ''
    )


def format_size(size, options=None):
    res = '{}B'.format(size)

    if options and 'h' in options:
        for fmt in [[10, 'KB'], [10, 'MB'], [10, 'GB'], [10, 'TB']]:
            size = size >> fmt[0]
            if not size:
                break

            res = '{}{}'.format(size, fmt[1])

    return res


def raw_output_formatter(out, options):
    for field, _, size in out:
        print('{}\t{}'.format(format_size(size, options).rjust(R_JUST_SIZE), field))


def csv_output_formatter(out, options):
    for field, level, size in out:
        print('{},{},{}'.format(field, level, format_size(size, options)))


OUTPUT_FORMATTERS = {
    'csv': csv_output_formatter,
    'raw': raw_output_formatter,
}

client = bigquery.Client()
args_parser = argparse.ArgumentParser()
R_JUST_SIZE = 13

if __name__ == "__main__":
    args_parser.add_argument(
        '--schema', '--schema', required=True, help='A path to BQ schema file'
    )

    args_parser.add_argument(
        '-h', help='"Human-readable" output.'
    )

    args_parser.add_argument(
        '--table_name', '--table_name', help='BQ table name clause.'
    )

    args_parser.add_argument(
        '-d', help='Display an entry for all fields depth records deep.'
    )

    args_parser.add_argument(
        '--use_legacy_sql', '--use_legacy_sql', help='Use legacy SQL. Default is false.'
    )

    args_parser.add_argument(
        '--format', '--format', default='raw', help='Output format. Either CSV or raw. Default is raw.'
    )

    args = args_parser.parse_args()

    bq_schema = None
    table_name = args.table_name

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
        args.legacy_sql or True
    )

    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_legacy_sql=legacy,
        use_query_cache=False,
    )

    out_format = args.format
    OUTPUT_FORMATTERS.get(out_format)(
        travel_fields(bq_schema.get("fields"), ""),
        args.h
    )
