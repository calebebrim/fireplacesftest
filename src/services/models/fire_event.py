import os 

from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Any
from src.services.utils.dateutils import try_strptime
from src.services.utils.logger_utils import getLogger

logger = getLogger(__file__)

DATE_FORMAT = os.environ.get("DATE_FORMAT", "%Y/%m/%d")
DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT", "%Y/%m/%d %H:%M:%S").split("|")
ADITIONAL_ALLOWED_EMPTY_FIELDS = os.environ.get(
    "ADITIONAL_ALLOWED_EMPTY_FIELDS", ""
).split(",")
EFFECTIVE_DATE_FORMAT = [] if not DATE_FORMAT else [DATE_FORMAT]
EFFECTIVE_DATE_FORMAT += DATETIME_FORMAT if DATETIME_FORMAT else []


@dataclass
class FireEvent:
    Incident_Number: str
    Exposure_Number: int | None
    ID: str
    Address: str
    Incident_Date: datetime | None
    Call_Number: str
    Alarm_DtTm: datetime | None
    Arrival_DtTm: datetime | None
    Close_DtTm: datetime | None
    City: str
    zipcode: str
    Battalion: str
    Station_Area: str
    Box: Optional[str]
    Suppression_Units: Optional[int]
    Suppression_Personnel: Optional[int]
    EMS_Units: Optional[int]
    EMS_Personnel: Optional[int]
    Other_Units: Optional[int]
    Other_Personnel: Optional[int]
    First_Unit_On_Scene: Optional[str]
    Estimated_Property_Loss: Optional[str]
    Estimated_Contents_Loss: Optional[str]
    Fire_Fatalities: Optional[int]
    Fire_Injuries: Optional[int]
    Civilian_Fatalities: Optional[int]
    Civilian_Injuries: Optional[int]
    Number_of_Alarms: Optional[int]
    Primary_Situation: Optional[str]
    Mutual_Aid: Optional[str]
    Action_Taken_Primary: Optional[str]
    Action_Taken_Secondary: Optional[str]
    Action_Taken_Other: Optional[str]
    Detector_Alerted_Occupants: Optional[str]
    Property_Use: Optional[str]
    Area_of_Fire_Origin: Optional[str]
    Ignition_Cause: Optional[str]
    Ignition_Factor_Primary: Optional[str]
    Ignition_Factor_Secondary: Optional[str]
    Heat_Source: Optional[str]
    Item_First_Ignited: Optional[str]
    Human_Factors_Associated_with_Ignition: Optional[str]
    Structure_Type: Optional[str]
    Structure_Status: Optional[str]
    Floor_of_Fire_Origin: Optional[str]
    Fire_Spread: Optional[str]
    No_Flame_Spread: Optional[str]
    Number_of_floors_with_minimum_damage: Optional[str]
    Number_of_floors_with_significant_damage: Optional[str]
    Number_of_floors_with_heavy_damage: Optional[str]
    Number_of_floors_with_extreme_damage: Optional[str]
    Detectors_Present: Optional[str]
    Detector_Type: Optional[str]
    Detector_Operation: Optional[str]
    Detector_Effectiveness: Optional[str]
    Detector_Failure_Reason: Optional[str]
    Automatic_Extinguishing_System_Present: Optional[str]
    Automatic_Extinguishing_System_Type: Optional[str]
    Automatic_Extinguishing_System_Perfomance: Optional[str]
    Automatic_Extinguishing_System_Failure_Reason: Optional[str]
    Number_of_Sprinkler_Heads_Operating: Optional[str]
    Supervisor_District: Optional[str]
    neighborhood_district: Optional[str]
    point: Optional[str]
    data_as_of: Optional[str]
    data_loaded_at: Optional[str]


from typing import Dict, Optional


def parse_fire_event(row: Dict[str, Any]) -> FireEvent:
    def to_int(value: Optional[str]) -> Optional[int]:
        if value and value.strip().isdigit():
            return int(value)
        return 0
    try:
        return FireEvent(
            Incident_Number=row["Incident Number"],
            Exposure_Number=to_int(row["Exposure Number"]),
            ID=row["ID"],
            Address=row["Address"],
            Incident_Date=try_strptime(row["Incident Date"], EFFECTIVE_DATE_FORMAT),
            Alarm_DtTm=try_strptime(row["Alarm DtTm"], EFFECTIVE_DATE_FORMAT),
            Arrival_DtTm=try_strptime(row["Arrival DtTm"], EFFECTIVE_DATE_FORMAT),
            Close_DtTm=try_strptime(row["Close DtTm"], EFFECTIVE_DATE_FORMAT),
            Call_Number=row["Call Number"],
            City=row["City"],
            zipcode=row["zipcode"],
            Battalion=row["Battalion"],
            Station_Area=row["Station Area"],
            Box=row["Box"] if row["Box"] else None,
            Suppression_Units=to_int(row["Suppression Units"]),
            Suppression_Personnel=to_int(row["Suppression Personnel"]),
            EMS_Units=to_int(row["EMS Units"]),
            EMS_Personnel=to_int(row["EMS Personnel"]),
            Other_Units=to_int(row["Other Units"]),
            Other_Personnel=to_int(row["Other Personnel"]),
            First_Unit_On_Scene=(
                row["First Unit On Scene"] if row["First Unit On Scene"] else None
            ),
            Estimated_Property_Loss=(
                row["Estimated Property Loss"]
                if row["Estimated Property Loss"]
                else None
            ),
            Estimated_Contents_Loss=(
                row["Estimated Contents Loss"]
                if row["Estimated Contents Loss"]
                else None
            ),
            Fire_Fatalities=to_int(row["Fire Fatalities"]),
            Fire_Injuries=to_int(row["Fire Injuries"]),
            Civilian_Fatalities=to_int(row["Civilian Fatalities"]),
            Civilian_Injuries=to_int(row["Civilian Injuries"]),
            Number_of_Alarms=to_int(row["Number of Alarms"]),
            Primary_Situation=(
                row["Primary Situation"] if row["Primary Situation"] else None
            ),
            Mutual_Aid=row["Mutual Aid"] if row["Mutual Aid"] else None,
            Action_Taken_Primary=(
                row["Action Taken Primary"] if row["Action Taken Primary"] else None
            ),
            Action_Taken_Secondary=(
                row["Action Taken Secondary"] if row["Action Taken Secondary"] else None
            ),
            Action_Taken_Other=(
                row["Action Taken Other"] if row["Action Taken Other"] else None
            ),
            Detector_Alerted_Occupants=(
                row["Detector Alerted Occupants"]
                if row["Detector Alerted Occupants"]
                else None
            ),
            Property_Use=row["Property Use"] if row["Property Use"] else None,
            Area_of_Fire_Origin=(
                row["Area of Fire Origin"] if row["Area of Fire Origin"] else None
            ),
            Ignition_Cause=row["Ignition Cause"] if row["Ignition Cause"] else None,
            Ignition_Factor_Primary=(
                row["Ignition Factor Primary"]
                if row["Ignition Factor Primary"]
                else None
            ),
            Ignition_Factor_Secondary=(
                row["Ignition Factor Secondary"]
                if row["Ignition Factor Secondary"]
                else None
            ),
            Heat_Source=row["Heat Source"] if row["Heat Source"] else None,
            Item_First_Ignited=(
                row["Item First Ignited"] if row["Item First Ignited"] else None
            ),
            Human_Factors_Associated_with_Ignition=(
                row["Human Factors Associated with Ignition"]
                if row["Human Factors Associated with Ignition"]
                else None
            ),
            Structure_Type=row["Structure Type"] if row["Structure Type"] else None,
            Structure_Status=(
                row["Structure Status"] if row["Structure Status"] else None
            ),
            Floor_of_Fire_Origin=(
                row["Floor of Fire Origin"] if row["Floor of Fire Origin"] else None
            ),
            Fire_Spread=row["Fire Spread"] if row["Fire Spread"] else None,
            No_Flame_Spread=row["No Flame Spread"] if row["No Flame Spread"] else None,
            Number_of_floors_with_minimum_damage=(
                row["Number of floors with minimum damage"]
                if row["Number of floors with minimum damage"]
                else None
            ),
            Number_of_floors_with_significant_damage=(
                row["Number of floors with significant damage"]
                if row["Number of floors with significant damage"]
                else None
            ),
            Number_of_floors_with_heavy_damage=(
                row["Number of floors with heavy damage"]
                if row["Number of floors with heavy damage"]
                else None
            ),
            Number_of_floors_with_extreme_damage=(
                row["Number of floors with extreme damage"]
                if row["Number of floors with extreme damage"]
                else None
            ),
            Detectors_Present=(
                row["Detectors Present"] if row["Detectors Present"] else None
            ),
            Detector_Type=row["Detector Type"] if row["Detector Type"] else None,
            Detector_Operation=(
                row["Detector Operation"] if row["Detector Operation"] else None
            ),
            Detector_Effectiveness=(
                row["Detector Effectiveness"] if row["Detector Effectiveness"] else None
            ),
            Detector_Failure_Reason=(
                row["Detector Failure Reason"]
                if row["Detector Failure Reason"]
                else None
            ),
            Automatic_Extinguishing_System_Present=(
                row["Automatic Extinguishing System Present"]
                if row["Automatic Extinguishing System Present"]
                else None
            ),
            Automatic_Extinguishing_System_Type=(
                row["Automatic Extinguishing Sytem Type"]
                if row["Automatic Extinguishing Sytem Type"]
                else None
            ),
            Automatic_Extinguishing_System_Perfomance=(
                row["Automatic Extinguishing Sytem Perfomance"]
                if row["Automatic Extinguishing Sytem Perfomance"]
                else None
            ),
            Automatic_Extinguishing_System_Failure_Reason=(
                row["Automatic Extinguishing Sytem Failure Reason"]
                if row["Automatic Extinguishing Sytem Failure Reason"]
                else None
            ),
            Number_of_Sprinkler_Heads_Operating=(
                row["Number of Sprinkler Heads Operating"]
                if row["Number of Sprinkler Heads Operating"]
                else None
            ),
            Supervisor_District=(
                row["Supervisor District"] if row["Supervisor District"] else None
            ),
            neighborhood_district=(
                row["neighborhood_district"] if row["neighborhood_district"] else None
            ),
            point=row["point"] if row["point"] else None,
            data_as_of=row["data_as_of"] if row["data_as_of"] else None,
            data_loaded_at=row["data_loaded_at"] if row["data_loaded_at"] else None,
        )
    except Exception as err:
        import json

        logger.debug(f"provided row: {row}")
        raise err


def data_quality_analysis(row: FireEvent) -> dict:
    """
    Perform data quality analysis on a single row of fire event data.

    :param row: A FireEvent object representing a single row of fire event data.
    :return: A dictionary containing data quality issues found in the row.
    """
    issues = {}
    for key, value in row.__dict__.items():
        if value is None or value == "":
            if key not in ADITIONAL_ALLOWED_EMPTY_FIELDS:
                issues[key] = "Missing value"

    if row.Incident_Date is None:
        issues["Incident_Date"] = "Missing Incident Date"

    if row.Supervisor_District is None:
        issues["Supervisor_District"] = "Missing District"

    if row.Battalion is None:
        issues["Battalion"] = "Missing Battalion"

    return issues


def fire_event_to_key(event: FireEvent, prefix: str = "fire_event:") -> str:
    """
    Generate a unique key for a FireEvent object based on its ID and Incident Date.

    :param event: A FireEvent object.
    :return: A string representing the unique key for the FireEvent.
    """
    if not event.Incident_Date:
        raise ValueError("Incident_Date not set")
    if not event.Incident_Number:
        raise ValueError("Incident_Number not set")

    return f"{prefix}{":" if prefix else ""}:{event.Incident_Number}"
