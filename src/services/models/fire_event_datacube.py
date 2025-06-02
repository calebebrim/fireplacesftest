from dataclasses import dataclass
from typing import Optional
from datetime import datetime


# Dimension: Location
@dataclass
class Location:
    id: str  # Referenced by FireEventFact.location_id
    Address: str
    City: str
    zipcode: str
    neighborhood_district: Optional[str]
    Supervisor_District: Optional[str]
    point: Optional[str]


# Dimension: DateTime
@dataclass
class DateTime:
    id: str  # Referenced by FireEventFact.datetime_id
    Incident_Date: Optional[datetime]
    Alarm_DtTm: Optional[datetime]
    Arrival_DtTm: Optional[datetime]
    Close_DtTm: Optional[datetime]
    data_as_of: Optional[str]
    data_loaded_at: Optional[str]


# Dimension: Incident
@dataclass
class Incident:
    Incident_Number: str # Referenced by FireEventFact.incident_id
    Exposure_Number: Optional[int]
    Call_Number: str
    Battalion: str
    Station_Area: str
    Box: Optional[str]
    First_Unit_On_Scene: Optional[str]
    Primary_Situation: Optional[str]
    Mutual_Aid: Optional[str]


# Dimension: Detector
@dataclass
class Detector:
    id: str  # Referenced by FireEventFact.detector_id
    Detectors_Present: Optional[str]
    Detector_Type: Optional[str]
    Detector_Operation: Optional[str]
    Detector_Effectiveness: Optional[str]
    Detector_Failure_Reason: Optional[str]


# Dimension: Suppression
@dataclass
class Suppression:
    id: str  # Referenced by FireEventFact.suppression_id
    Suppression_Units: Optional[int]
    Suppression_Personnel: Optional[int]
    EMS_Units: Optional[int]
    EMS_Personnel: Optional[int]
    Other_Units: Optional[int]
    Other_Personnel: Optional[int]


# Dimension: Fire Spread
@dataclass
class FireSpread:
    id: str  # Referenced by FireEventFact.fire_spread_id
    Fire_Spread: Optional[str]
    No_Flame_Spread: Optional[str]
    Number_of_floors_with_minimum_damage: Optional[str]
    Number_of_floors_with_significant_damage: Optional[str]
    Number_of_floors_with_heavy_damage: Optional[str]
    Number_of_floors_with_extreme_damage: Optional[str]


# Dimension: Fire Origin
@dataclass
class FireOrigin:
    id: str  # Referenced by FireEventFact.fire_origin_id
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


# Dimension: Extinguishing System
@dataclass
class ExtinguishingSystem:
    id: str  # Referenced by FireEventFact.extinguishing_system_id
    Automatic_Extinguishing_System_Present: Optional[str]
    Automatic_Extinguishing_System_Type: Optional[str]
    Automatic_Extinguishing_System_Perfomance: Optional[str]
    Automatic_Extinguishing_System_Failure_Reason: Optional[str]
    Number_of_Sprinkler_Heads_Operating: Optional[str]


# Fact Table: FireEventFact
@dataclass
class FireEventFact:
    id: str
    location_id: str  # Links to Location.id
    datetime_id: str  # Links to DateTime.id
    incident_id: str  # Links to Incident.id
    detector_id: str  # Links to Detector.id
    suppression_id: str  # Links to Suppression.id
    fire_spread_id: str  # Links to FireSpread.id
    fire_origin_id: str  # Links to FireOrigin.id
    extinguishing_system_id: str  # Links to ExtinguishingSystem.id
    Fire_Fatalities: Optional[int]
    Fire_Injuries: Optional[int]
    Civilian_Fatalities: Optional[int]
    Civilian_Injuries: Optional[int]
    Estimated_Property_Loss: Optional[str]
    Estimated_Contents_Loss: Optional[str]
    Number_of_Alarms: Optional[int]
    Action_Taken_Primary: Optional[str]
    Action_Taken_Secondary: Optional[str]
    Action_Taken_Other: Optional[str]
    Detector_Alerted_Occupants: Optional[str]
