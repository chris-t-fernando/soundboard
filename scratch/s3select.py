import pandas as pd
from io import StringIO
from select_plus import SSP
from select_plus.serializers import (
    InputSerialization,
    OutputSerialization,
    CSVInputSerialization,
    CSVOutputSerialization,
)
import wordcloud

bucket = "fdotest"
key = "hayley old-export.csv"


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


def extract_words_from_df(words_df):
    # pull out rows that aren't words
    words_df = words_df[words_df["iMessage"].notnull()]

    # remove canned responses
    words_df = words_df[
        ~words_df["iMessage"].str.match("Sorry, I can't talk right now.")
    ]

    # remove rows that are just missed calls
    words_df = words_df.loc[~words_df.iMessage.str.contains("MessageBank")]

    # remove punctuation
    words_df["iMessage"] = words_df["iMessage"].str.replace("[^\w\s]+", "", regex=True)

    # replace carriage returns
    # words_df["iMessage"] = words_df["iMessage"].str.replace("\\n", "\n")
    words_df["iMessage"] = words_df["iMessage"].str.replace("\n", " ")
    words_df["iMessage"] = words_df["iMessage"].str.lower()

    # pull the words out of the df
    words_string = " ".join(words_df.iMessage).split(" ")

    # clean out individual words that i don't want
    min_word_length = 4
    words_string = [word for word in words_string if not word.isdigit()]
    words_string = [word for word in words_string if len(word) >= min_word_length]

    # deal with plurals
    words_string = [
        word for word in words_string if not word[-1:] == "s" and word[-2:] != "ss"
    ]

    # get rid of common words
    STOPWORDS = set(map(str.strip, open("scratch/stopwords.txt").readlines()))
    words_string = [word for word in words_string if word not in STOPWORDS]

    # get the count for each word
    word_count, words = wordcloud.process_tokens(words_string)

    # load it back in to a df so that I can do stats stuff with each word
    cloud_df = pd.DataFrame.from_dict(word_count, orient="index")
    cloud_df.columns = ["count"]
    cloud_df = cloud_df.sort_values(by="count", ascending=False)
    return cloud_df


if __name__ == "__main__":
    ssp = SSP(bucket_name=bucket, prefix=key)
    # q = query(ssp, " where received = 'Yes'")
    qa = query(ssp)
    qa = qa.query("iMessage.notna()", engine="python")
    cloud = extract_words_from_df(qa)

    # cloud_df = pd.DataFrame.from_dict(word_count, orient="index")
