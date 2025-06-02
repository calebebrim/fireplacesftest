import os
import pandas as pc


data = pc.read_csv("/home/caleb/workspace/tests/ntdsoftware/mount/worker/firedata/Fire_Incidents_20250530.csv")


print(data.head())

count_by_incident_number = (
    data.groupby("Incident Number") 
    .size()
    .reset_index(name="count")
    .sort_values(by="count", ascending=False)
)
print(count_by_incident_number.head(10))

count_by_incident_number = (
    data.groupby("ID")
    .size()
    .reset_index(name="count")
    .sort_values(by="count", ascending=False)
)

print(count_by_incident_number.head(10))
