import pandas as pd
from io import StringIO
from select_plus import SSP
from select_plus.serializers import (
    InputSerialization,
    OutputSerialization,
    CSVInputSerialization,
    CSVOutputSerialization,
)

bucket = "fdotest"
key = "hayley old-export.csv"


def query(ssp, condition=""):
    base_query = "SELECT * FROM s3object s"
    data_query = base_query + " " + condition
    column_query = base_query + " limit 1"

    column_headers = ssp.select(
        threads=1,
        sql_query=column_query,
        input_serialization=InputSerialization(
            csv=CSVInputSerialization(
                file_header_info="NONE",
                allow_quoted_record_delimiter=True,
            )
        ),
        output_serialization=OutputSerialization(csv=CSVOutputSerialization()),
    )

    result = ssp.select(
        threads=1,
        sql_query=data_query,
        input_serialization=InputSerialization(
            csv=CSVInputSerialization(
                file_header_info="USE",
                allow_quoted_record_delimiter=True,
            )
        ),
        output_serialization=OutputSerialization(csv=CSVOutputSerialization()),
    )

    record_string = ""
    for event in result.payload:
        # records.append(event)
        record_string += event

    # get rid of weird last column leftover from import
    columns = column_headers.payload[0].split(",")[:-1]
    payload_df = pd.read_csv(StringIO(record_string), names=columns)

    return payload_df


if __name__ == "__main__":
    ssp = SSP(bucket_name=bucket, prefix=key)
    q = query(ssp, " where received = 'Yes'")
    qa = query(ssp)
    print("a")
