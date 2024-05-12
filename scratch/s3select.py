import pandas as pd
from io import StringIO
from select_plus import SSP
from select_plus.serializers import (
    InputSerialization,
    OutputSerialization,
    CSVInputSerialization,
    CSVOutputSerialization,
)

import nltk

from nltk.sentiment.vader import SentimentIntensityAnalyzer

from nltk.corpus import stopwords

from nltk.tokenize import word_tokenize

from nltk.stem import WordNetLemmatizer

# nltk.download("all")
"""
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download("all")
"""

bucket = "fdotest"
key = "hayley old-export.csv"
# initialize NLTK sentiment analyzer
analyzer = SentimentIntensityAnalyzer()


# function to return a dataframe with the s3select contents
# a second query is issued so that we can get the column headers and apply them to the df
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


def preprocess_text(text):
    # Tokenize the text
    tokens = word_tokenize(text.lower())

    # Remove stop words
    filtered_tokens = [
        token for token in tokens if token not in stopwords.words("english")
    ]

    # Lemmatize the tokens
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]

    # Join the tokens back into a string
    processed_text = " ".join(lemmatized_tokens)

    return processed_text


def get_sentiment(text):
    scores = analyzer.polarity_scores(text)
    sentiment = 1 if scores["pos"] > 0 else 0
    return sentiment


if __name__ == "__main__":
    ssp = SSP(bucket_name=bucket, prefix=key)
    # q = query(ssp, " where received = 'Yes'")
    qa = query(ssp)
    qa = qa.query("iMessage.notna()", engine="python")

    qa["assessed_text"] = qa["iMessage"].apply(preprocess_text)
    qa["sentiment"] = qa["iMessage"].apply(get_sentiment)

    print("a")
