import boto3
import pandas as pd
from io import StringIO

bucket = "fdotest"
key = "hayley old-export.csv"
query = "SELECT * FROM S3Object"

s3 = boto3.client(service_name="s3")

from select_plus import SSP
from select_plus.serializers import (
    InputSerialization,
    OutputSerialization,
    CSVInputSerialization,
    CSVOutputSerialization,
)


ssp = SSP(bucket_name=bucket, prefix=key)
if __name__ == "__main__":
    result = ssp.select(
        threads=1,
        # sql_query='SELECT * FROM s3object s where s."Heading 1" = "H1 D1"',
        sql_query="SELECT * FROM s3object s where received = 'Yes'",
        input_serialization=InputSerialization(
            csv=CSVInputSerialization(
                file_header_info="USE",
                allow_quoted_record_delimiter=True,
            )
        ),
        output_serialization=OutputSerialization(csv=CSVOutputSerialization()),
    )
    zz = pd.read_csv(StringIO(result.payload[0]))
    records = []
    for event in result.payload:
        records.append(event)

    # file_str = "".join(r.decode("utf-8") for r in records)
