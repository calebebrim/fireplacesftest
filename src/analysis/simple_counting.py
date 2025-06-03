import os
from datetime import datetime


from src.analysis.utils.dataframe import redis_result_to_df
from src.services.utils.logger_utils import getLogger, hline

import redis
from redis.commands.json.path import Path
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

import redis.exceptions

from redis.commands.search.aggregation import AggregateRequest, Reducer

from src.services.utils.redis_utils import get_redis_client

logger = getLogger(__file__)

REDIS_EVENT_KEY_PREFIX = f"fire_event_data_serving:fireevent"
REDIS_EVENT_INDEX_ID = "fireevent_idx"

r = get_redis_client()

# Remembering the index schema:
# schema = [
#     TagField("Incident_Number"),  # Grouping/filtering by incident number
#     NumericField("ID", sortable=True),  # Row ID as numeric, sortable
#     TextField("Alarm_DtTm", sortable=True),  # ISO date-time string, sortable
#     TagField("neighborhood_district"),  # District as tag field
#     TagField("Battalion"),  # Battalion as tag field
# ]
hline(header="general index information")
FT__LIST = "FT._LIST"
logger.debug(FT__LIST)
logger.debug(r.execute_command(FT__LIST))


FT_INDEX_INFO = f"FT.INFO {REDIS_EVENT_INDEX_ID}"
index_info = r.execute_command(FT_INDEX_INFO)

logger.debug(FT_INDEX_INFO)
logger.debug(index_info)
hline(header="general index information")


hline(header="QUERY: @Battalion:{B09}")
query = ["FT.SEARCH", REDIS_EVENT_INDEX_ID, "@Battalion:{B09}"]
logger.debug(query)
battalionB09 = r.execute_command(*query)
# logger.info(battalionB09)
hline()
df, docs = redis_result_to_df(battalionB09)
# logger.info(docs)
df = df.sort_values(by=["Incident_Number", "Exposure_Number"])
logger.debug(df[["Incident_Number", "Exposure_Number"]].head())
hline()
logger.info("Max id by incident_number")
incident_number_counter = df.groupby("Incident_Number").agg(
    max=("ID", 'max'),
    count=('ID', 'count')
).reset_index()
logger.info(incident_number_counter[incident_number_counter['count']>1].head())
hline()
logger.info(f"Result keys: {df.keys()}")
logger.info(f"Count: {df.size}")


hline(header="QUERY: @Battalion:{B09} 2025 excelsior dt events")

start = datetime.fromisoformat("2024-01-01T00:00:00").timestamp()
logger.info(f"start: {start}")

filter = f"@Incident_Date:[{start} +inf] @Battalion:{{B09}} @neighborhood_district:{{Excelsior}}"

query = [
    "FT.SEARCH",
    REDIS_EVENT_INDEX_ID,
    filter,
    "RETURN",
    "5",
    "Incident_Number",
    "neighborhood_district",
    "Alarm_DtTm",
    "Incident_Date",
    "Battalion",
]
logger.info(" ".join(query))
res = r.execute_command(*query)
df,_ = redis_result_to_df(res)
logger.info(df.head())
hline(header="QUERY: @Battalion:{B09}")
