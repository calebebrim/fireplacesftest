import pandas as pd

def redis_result_to_df(raw_result):
    num_results = raw_result[0]
    data = raw_result[1:]

    # Transform to list of dicts
    docs = []
    for i in range(0, len(data), 2):
        doc_id = data[i]
        fields = data[i + 1]

        # Convert field list to dict
        doc_data = {fields[j]: fields[j + 1] for j in range(0, len(fields), 2)}
        doc_data["_doc_id"] = doc_id  # Add doc_id as a column
        docs.append(doc_data)

    # Create DataFrame
    df = pd.DataFrame(docs)
    return df, docs
